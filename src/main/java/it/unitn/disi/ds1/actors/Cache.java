package it.unitn.disi.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.messages.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Collections;

/**
 * Cache server actor
 * <p>
 * It caches database entries.
 * It can be of two types:
 * - L1: communicates with the database and returns the results
 * - L2: communicates with clients and L1 cache servers
 * <p>
 * It handles the following requests:
 * - READ
 * - WRITE
 * - CRITREAD
 * - CRITWRITE
 * <p>
 * This actor can crash.
 */
public class Cache extends Actor {
    /**
     * Reference to the parent actor
     */
    private final ActorRef parent;

    /**
     * Reference of all children cache servers
     * relevant only for L1 caches
     */
    private final List<ActorRef> caches;

    /**
     * Cached entries of the database
     */
    private final Map<Integer, Integer> cachedDatabase;

    /**
     * Cache constructor, by default the cache is an L2
     * Initialize all variables
     *
     * @param id     Cache identifier
     * @param parent Reference to the parent actor
     */
    public Cache(int id, ActorRef parent) {
        super(id, Cache.class.getName());
        this.parent = parent;
        this.caches = new ArrayList<>();
        this.cachedDatabase = new HashMap<>();
    }

    /**
     * Static class builder
     *
     * @param id       identifier
     * @param database reference to the database
     * @return Cache instance
     */
    static public Props props(int id, ActorRef database) {
        return Props.create(Cache.class, () -> new Cache(id, database));
    }

    /**
     * Method which clear the cache
     */
    private void clearCache() {
        this.cachedDatabase.clear();
    }

    /**
     * Handler of JoinCachesMsg message.
     * Add all the joined caches as children
     *
     * @param msg message containing information about the joined cache servers
     */
    @Override
    protected void onJoinCachesMessage(JoinCachesMessage msg) {
        this.caches.addAll(msg.caches);
        this.LOGGER.info(getSelf().path().name() + ": joining a the distributed cache with " + this.caches.size() + " children peers with ID " + this.id);
    }

    /**
     * Handler of the ReadMessage message
     * If the Cache has the message then it returns it otherwise, it asks to the
     * above layer (L1 will ask directly to the database)
     *
     * The general hops idea is to keep track of who has sent the original message in a chain of requests
     * When we need to send a request we append the current node (namely the cash).
     * In this way when the database leverages on the hops constructed during the onReadMessage and use
     * them to propagate the response back.
     *
     * Therefore, when we are in the database we can reconstruct the hops to whom send the request and onResponse
     * will traverse the hops backwards
     *
     * @param msg read message
     */
    @Override
    protected void onReadMessage(ReadMessage msg) {
        // Case of a cache hit
        if (this.cachedDatabase.containsKey(msg.requestKey)) {
            this.LOGGER.info(getSelf().path().name() + ": cache hit of key:" + msg.requestKey + " with ID " + this.id);

            // Get the list of hops, indicated by the read message
            List<ActorRef> newHops = new ArrayList<>(msg.hops);
            newHops.remove(newHops.size() - 1); // remove last element of the hop
            // Generate a new response message which contains the cached data and the new hops
            ResponseMessage responseMessage = new ResponseMessage(Collections.singletonMap(msg.requestKey, this.cachedDatabase.get(msg.requestKey)), newHops);
            // Send the message to the sender of the read message
            getSender().tell(responseMessage, getSelf());
        } else {
            // Cache miss
            this.LOGGER.info(getSelf().path().name() + ": cache miss of key:" + msg.requestKey + " with id: " + this.id + ", asking to the parent: " + this.parent.path().name());

            // Pass request to the parent, adding getSelf() into the hops
            List<ActorRef> newHops = new ArrayList<>(msg.hops);
            // Add itself to the list of hops
            newHops.add(getSelf());
            // Generate a new read message and sed it to the parent
            ReadMessage newReadMessage = new ReadMessage(msg.requestKey, newHops);
            this.parent.tell(newReadMessage, getSelf());
        }
    }

    /**
     * Handler of the onResponseMessage
     *
     * The main idea behind the response message is that the database will prepare it with the list of
     * hops which need to be traversed. In this scenario, the onResponse will only have to store the value and
     * traverse the list of hops backwards, preparing a new response message.
     *
     * @param msg response message
     */
    protected void onResponseMessage(ResponseMessage msg) {
        // Store the result in the cached database
        this.cachedDatabase.putAll(msg.values);

        // Generate a new ArrayList from the message hops
        List<ActorRef> newHops = new ArrayList<>(msg.hops);
        // Save the next hop of the communication
        ActorRef sendTo = msg.hops.get(msg.hops.size()-1);
        // Remove the next hop from the new hops (basically it is the actor to which we are sending the response)
        newHops.remove(newHops.size() - 1);
        // Create the response message with the new hops
        ResponseMessage newResponseMessage = new ResponseMessage(msg.values, newHops);
        // Send the newly created response to the next hop we previously saved
        sendTo.tell(newResponseMessage, getSelf());
        this.LOGGER.info(getSelf().path().name() + " is answering " + msg.values + " to " + sendTo.path().name());
    }

    /**
     * Handler of the WriteMessage message
     * The request is forwarded to the database, that applies the write and sends the notification of
     * the update to all its L1 caches. In turn, all L1 caches propagate it to their connected L2 caches.
     *
     * @param msg write message
     */
    @Override
    protected void onWriteMessage(WriteMessage msg) {
        LOGGER.info(getSelf().path().name() + ": forwarding the message to the parent with ID " + this.id);
        this.parent.tell(msg, getSelf());
    }

    @Override
    protected void onCriticalReadMessage(CriticalReadMessage msg) {

    }

    @Override
    protected void onCriticalWriteMessage(CriticalWriteMessage msg) {

    }

    protected void onUpdatedCacheMessage(UpdateCacheMessage msg) {
        int updatedKey = (int) msg.values.keySet().toArray()[0];
        int updatedValue = (int) msg.values.values().toArray()[0];

        if (this.cachedDatabase.containsKey(updatedKey)) {
            this.LOGGER.info(getSelf().path().name() + ": updating the cached value for key " + updatedKey);
            this.cachedDatabase.remove(updatedKey);
            this.cachedDatabase.put(updatedKey, updatedValue);
        }

        if (!this.caches.isEmpty()) {
            this.LOGGER.info(getSelf().path().name() + ": forwarding the new value for " + updatedKey + " to lower L2 caches");
            multicast(msg, this.caches);
        }
    }

    /**
     * Handler of the Recovery message
     * In order to avoid issues, when one node recovers from crashes he forgot
     * all its data. Moreover, when the L1 node crashes, it communicates the L2
     * caches to flush
     *
     * @param msg write message
     */
    @Override
    protected void onRecoveryMessage(RecoveryMessage msg) {
        // Empty the local cache
        this.clearCache();
        // TODO maybe a better way to handle it since we may know whether one node is L1 or L2?
        this.multicast(new FlushMessage(), this.caches);
        this.LOGGER.info(getSelf().path().name() + ": recovering: flushing the cache and multicast flush with ID " + this.id);
    }

    /**
     * When a flush message is received
     * Just empties the local cache
     *
     * @param msg flush message
     */
    private void onFlushMessage(FlushMessage msg) {
        // Empty the local cache
        this.clearCache();
        this.LOGGER.info(getSelf().path().name() + ": flushing the cache with ID " + this.id);
    }

    /**
     * Create receive method, by default the cache behaves like a L1 cache
     *
     * @return message matcher
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinCachesMessage.class, this::onJoinCachesMessage)
                .match(ReadMessage.class, this::onReadMessage)
                .match(ResponseMessage.class, this::onResponseMessage)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(FlushMessage.class, this::onFlushMessage)
                .match(RecoveryMessage.class, this::onRecoveryMessage)
                .match(UpdateCacheMessage.class, this::onUpdatedCacheMessage)
                .match(TokenMessage.class, msg -> onToken(
                        msg,
                        this.cachedDatabase,
                        Stream.concat(
                                this.caches.stream(),
                                Collections.singletonList(this.parent).stream())
                                .collect(Collectors.toList())
                ))
                .build();
    }
}
