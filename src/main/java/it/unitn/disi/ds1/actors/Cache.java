package it.unitn.disi.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.Config;
import it.unitn.disi.ds1.Logger;
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
    private ActorRef parent;

    /**
     * Reference to the original parent
     */
    private final ActorRef originalParent;

    /**
     * Database reference
     */
    private final ActorRef database;

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
     * cache is waiting for a message
     */
    private boolean shouldReceiveResponse = false;

    /**
     * By default the cache is an L2
     */
    private boolean isL1;

    /**
     * Pending requests
     */
    private HashMap<UUID, Message> pendingQueries;

    /**
     * Cache constructor, by default the cache is an L2
     * Initialize all variables
     *
     * @param id     Cache identifier
     * @param parent Reference to the parent actor
     */
    public Cache(int id, ActorRef parent, ActorRef database) {
        super(id);
        this.parent = parent;
        this.originalParent = parent;
        this.caches = new ArrayList<>();
        this.cachedDatabase = new HashMap<>();
        this.isL1 = false;
        this.pendingQueries = new HashMap<>();
        this.database = database;
    }

    /**
     * Static class builder
     *
     * @param id       identifier
     * @param parent reference to the parent node
     * @param database reference to the database
     * @return Cache instance
     */
    static public Props props(int id, ActorRef parent, ActorRef database) {
        return Props.create(Cache.class, () -> new Cache(id, parent, database));
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
        this.isL1 = !this.caches.isEmpty();
        Logger.INSTANCE.info(getSelf().path().name() + ": joining a the distributed cache with " + this.caches.size() + " children peers with ID " + this.id);
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
            Logger.INSTANCE.info(getSelf().path().name() + ": cache hit of key:" + msg.requestKey + " with ID " + this.id);

            // Get the list of hops, indicated by the read message
            List<ActorRef> newHops = new ArrayList<>(msg.hops);
            newHops.remove(newHops.size() - 1); // remove last element of the hop
            // Generate a new response message which contains the cached data and the new hops
            ResponseMessage responseMessage = new ResponseMessage(Collections.singletonMap(msg.requestKey, this.cachedDatabase.get(msg.requestKey)), newHops, msg.queryUUID, Config.RequestType.READ);
            // Send the message to the sender of the read message
            getSender().tell(responseMessage, getSelf());
        } else {

            // TODO: L1 CRASH
            //if (this.isL1) {
            //   System.out.println(getSelf().path().name() + " crashed");
            //   getContext().become(crashed());
            //   return;
            //}

            // Cache miss
            Logger.INSTANCE.info(getSelf().path().name() + ": cache miss of key:" + msg.requestKey + " with id: " + this.id + ", asking to the parent: " + this.parent.path().name());

            // Generate a new request UUID
            UUID uuid;
            if (msg.queryUUID == null) {
                uuid = UUID.randomUUID();
            } else {
                uuid = msg.queryUUID;
            }
            // Pass request to the parent, adding getSelf() into the hops
            List<ActorRef> newHops = new ArrayList<>(msg.hops);
            // Add itself to the list of hops
            newHops.add(getSelf());
            // Generate a new read message and sed it to the parent
            ReadMessage newReadMessage = new ReadMessage(msg.requestKey, newHops, uuid);
            this.parent.tell(newReadMessage, getSelf());

            // This message is pending
            this.pendingQueries.put(uuid, newReadMessage);
            if (!this.isL1) {
                // Setting a scheduler for a possible timeout
                this.scheduleTimer(new TimeoutMessage(newReadMessage, this.parent), Config.L2_TIMEOUT);
            }
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
    @Override
    protected void onResponseMessage(ResponseMessage msg) {
        // Check if it's a pending query for the current cache
        boolean isPendingQuery = this.pendingQueries.containsKey(msg.queryUUID);
        // Remove the pending query since we got the response
        this.pendingQueries.remove(msg.queryUUID);
        if(!this.isL1){
            // If there was a timer I cancel it
            this.cancelTimer();
        }

        // Store the result in the cached database
        int updatedKey = (int) msg.values.keySet().toArray()[0];
        // TODO: maybe we should consider other cases like CRITREAD or CRITWRITE
        // If it is a read, we should pull, if it is a write we are listening only if the value is contained in the cache
        if(msg.requestType == Config.RequestType.READ || this.cachedDatabase.containsKey(updatedKey)){
            Logger.INSTANCE.info(getSelf().path().name() + ": updating the cached value for key " + updatedKey);
            this.cachedDatabase.remove(updatedKey);
            this.cachedDatabase.putAll(msg.values);
        }

        // For eventual snapshots
        capureTransitMessages(msg.values, getSender());

        // Generate a new ArrayList from the message hops
        List<ActorRef> newHops = new ArrayList<>(msg.hops);
        // Save the next hop of the communication
        ActorRef sendTo = msg.hops.get(msg.hops.size()-1);
        // Remove the next hop from the new hops (basically it is the actor to which we are sending the response)
        newHops.remove(newHops.size() - 1);
        // Create the response message with the new hops
        ResponseMessage newResponseMessage = new ResponseMessage(msg.values, newHops, msg.queryUUID, msg.requestType);

        if (msg.requestType == Config.RequestType.WRITE) {// WRITE -> perform the multicast to all the peers interested
            multicast(newResponseMessage, this.caches);
            Logger.INSTANCE.info(getSelf().path().name() + " is multicasting " + msg.values + " to children");
        }

        // If it is an L2 cache then it sends to the client
        // If the message is a READ, regardless of the cache type it sends only to  (pulled the request)cache that have pulled the value
        if (isPendingQuery && (!this.isL1 || msg.requestType == Config.RequestType.READ)) {
            // Send the newly created response to the next hop we previously saved
            sendTo.tell(newResponseMessage, getSelf());
            Logger.INSTANCE.info(getSelf().path().name() + " is answering " + msg.values + " to " + sendTo.path().name());
        }
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
        // TODO: L1 CRASH
        //if (this.isL1) {
        //   System.out.println(getSelf().path().name() + " crashed");
        //   getContext().become(crashed());
        //   return;
        //}

        Logger.INSTANCE.info(getSelf().path().name() + ": forwarding the message to the parent with ID " + this.id);

        // Generate a new request UUID
        UUID uuid = null;
        if (msg.queryUUID == null) {
            uuid = UUID.randomUUID();
        } else {
            uuid = msg.queryUUID;
        }
        // Pass request to the parent, adding getSelf() into the hops
        List<ActorRef> newHops = new ArrayList<>(msg.hops);
        // Add itself to the list of hops
        newHops.add(getSelf());

        // Recreating and sending the new write message
        WriteMessage newWriteMessage = new WriteMessage(msg.requestKey, msg.modifiedValue, newHops, uuid);
        this.parent.tell(newWriteMessage, getSelf());

        // This message is pending
        this.pendingQueries.put(uuid, newWriteMessage);
        if (!this.isL1) {
            // Setting a scheduler for a possible timeout
            this.scheduleTimer(new TimeoutMessage(newWriteMessage, this.parent), Config.L2_TIMEOUT);
        }

        // For eventual snapshots
        capureTransitMessages(Collections.singletonMap(msg.requestKey, msg.modifiedValue), getSender());
    }

    @Override
    protected void onCriticalReadMessage(CriticalReadMessage msg) {

    }

    @Override
    protected void onCriticalWriteMessage(CriticalWriteMessage msg) {

    }

    @Override
    protected void onTimeoutMessage(TimeoutMessage msg){
        // TODO: take care that when L1 respawn this cache need to return L2
        // or simply become unavailable
        int requestKey = 0;
        UUID queryUUID = null;
        List<ActorRef> hops = null;
        Config.RequestType requestType = null;
        if(msg.msg instanceof ReadMessage){
            requestKey = ((ReadMessage)(msg.msg)).requestKey;
            queryUUID = ((ReadMessage)(msg.msg)).queryUUID;
            hops = ((ReadMessage)(msg.msg)).hops;
            requestType = Config.RequestType.READ;
        }else if (msg.msg instanceof WriteMessage){
            requestKey = ((WriteMessage)(msg.msg)).requestKey;
            queryUUID = ((WriteMessage)(msg.msg)).queryUUID;
            hops = ((WriteMessage)(msg.msg)).hops;
            requestType = Config.RequestType.WRITE;
        }

        // Remove self from the hops, only the client will remain
        hops = new ArrayList<>(hops);
        hops.remove(hops.size()-1);

        if (!this.pendingQueries.containsKey(queryUUID))
            return;

        this.parent = this.database;
        this.isL1 = true;
        this.pendingQueries.remove(queryUUID);

        // The cache has become an L1 without assigned L2 caches
        // It won't be able to answer queries if we don't recreate the tree structure
        // However for the scope of the project we are not asked to recreate it,
        // so the cache can just become unavailable
        getContext().become(unavailable());
        Logger.INSTANCE.info("Cache timed-out: " + msg.whoCrashed.path().name() + " has probably crashed");

        // Send the error message
        Logger.INSTANCE.info("Sending error message");
        ResponseMessage responseMessage = new ResponseMessage(
                null,
                hops,
                queryUUID,       // Encapsulating the query UUID
                requestType

        );
        // Send to the client the empty response
        hops.get(hops.size()-1).tell(responseMessage, getSelf());
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
        Logger.INSTANCE.info(getSelf().path().name() + ": recovering: flushing the cache and multicast flush with ID " + this.id);
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
        Logger.INSTANCE.info(getSelf().path().name() + ": flushing the cache with ID " + this.id);
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
                .match(TimeoutMessage.class, this::onTimeoutMessage)
                .match(TokenMessage.class, msg -> onToken(
                        msg,
                        this.cachedDatabase,
                        Stream.concat(
                                    this.caches.stream(),
                                    Collections.singletonList(this.parent).stream()
                                ).collect(Collectors.toList())
                ))
                .build();
    }

    /**
     * Crashed behavior
     * @return builder
     */
    @Override
    public Receive crashed() {
        return receiveBuilder()
                .match(RecoveryMessage.class, this::onRecoveryMessage)
                .match(TokenMessage.class, msg -> onToken(
                        msg,
                        this.cachedDatabase,
                        Stream.concat(
                                this.caches.stream(),
                                Collections.singletonList(this.parent).stream()
                        ).collect(Collectors.toList())
                ))
                .matchAny(msg -> {})
                .build();
    }

    /**
     * Unavailable behavior
     * @return builder
     */
    public Receive unavailable() {
        return receiveBuilder()
                .match(RecoveryMessage.class, this::onRecoveryMessage)
                .match(ResponseMessage.class, this::onResponseMessage)
                .match(TokenMessage.class, msg -> onToken(
                        msg,
                        this.cachedDatabase,
                        Stream.concat(
                                    this.caches.stream(),
                                    Collections.singletonList(this.originalParent).stream()
                                ).collect(Collectors.toList())
                ))
                .matchAny(msg -> {})
                .build();
    }
}
