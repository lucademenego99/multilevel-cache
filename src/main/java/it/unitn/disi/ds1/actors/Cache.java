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
     * Type of the next simulated crash
     */
    private Config.CrashType nextCrash;

    /**
     * After how many milliseconds the node should recover after the crash
     */
    private int recoverIn;

    /**
     * Whether the cache is unvailable
     */
    private boolean unavailable = false;

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
        this.seqnoCache.clear();
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
        // Check if the node should crash before read L1 and L2
        if((this.isL1 && nextCrash == Config.CrashType.L1_BEFORE_READ) || (!this.isL1 && nextCrash == Config.CrashType.L2_BEFORE_READ)) {
            this.crash(this.recoverIn);
            return;
        }

        // Check if the node should crash before CRITICAL read L1 and L2
        if((this.isL1 && msg.isCritical && nextCrash == Config.CrashType.L1_BEFORE_CRIT_READ) || (!this.isL1 && nextCrash == Config.CrashType.L2_BEFORE_CRIT_READ)) {
            this.crash(this.recoverIn);
            return;
        }

        // Case of a cache hit
        // IF IS CRITICAL DO NOT RETURN THE CACHED RESULT
        if (!msg.isCritical && this.cachedDatabase.containsKey(msg.requestKey)) {
            // Compare the sequence number we got
            int currentSeqno = this.seqnoCache.get(msg.requestKey);
            // I do not answer with older value
            if(msg.seqno > currentSeqno){
                Logger.INSTANCE.info(getSelf().path().name() + ": got an older value with respect to one requested with key:" + msg.requestKey + " with ID " + this.id + " namely: " + msg.seqno + " > " + currentSeqno);
                return;
            }

            Logger.INSTANCE.info(getSelf().path().name() + ": cache hit of key:" + msg.requestKey + " with ID " + this.id);

            // Get the list of hops, indicated by the read message
            List<ActorRef> newHops = new ArrayList<>(msg.hops);
            newHops.remove(newHops.size() - 1); // remove last element of the hop
            // Generate a new response message which contains the cached data and the new hops
            ResponseMessage responseMessage = new ResponseMessage(Collections.singletonMap(msg.requestKey, this.cachedDatabase.get(msg.requestKey)), newHops, msg.queryUUID, Config.RequestType.READ, currentSeqno);
            // Network delay
            this.delay();
            // Send the message to the sender of the read message
            getSender().tell(responseMessage, getSelf());
        } else {

            // Cache miss
            Logger.INSTANCE.info(getSelf().path().name() + ": cache miss of key:" + msg.requestKey + " with id: " + this.id + ", asking to the parent: " + this.parent.path().name() + " [CRITICAL] = " + msg.isCritical);

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
            ReadMessage newReadMessage = new ReadMessage(msg.requestKey, newHops, uuid, msg.isCritical, msg.seqno);
            // Network delay
            this.delay();
            // Send the request to the parent
            this.parent.tell(newReadMessage, getSelf());

            // This message is pending, thus I add the message and the UUID in the setting
            this.pendingQueries.put(uuid, newReadMessage);
            if (!this.isL1) {
                // Setting a scheduler for a possible timeout
                this.scheduleTimer(new TimeoutMessage(newReadMessage, this.parent), Config.L2_TIMEOUT);
            }
        }

        // Check if the node should crash after CRITICAL read L1 and L2
        if((this.isL1 && msg.isCritical && nextCrash == Config.CrashType.L1_AFTER_CRIT_READ) || (!this.isL1 && nextCrash == Config.CrashType.L2_AFTER_CRIT_READ)) {
            this.crash(this.recoverIn);
            return;
        }

        // Check if the node should crash after read L1 and L2
        if((this.isL1 && nextCrash == Config.CrashType.L1_AFTER_READ) || (!this.isL1 && nextCrash == Config.CrashType.L2_AFTER_READ)) {
            this.crash(this.recoverIn);
            return;
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
        // Check if the node should crash before response L1 and L2
        if((this.isL1 && nextCrash == Config.CrashType.L1_BEFORE_RESPONSE) || (!this.isL1 && nextCrash == Config.CrashType.L2_BEFORE_RESPONSE)) {
            this.crash(this.recoverIn);
            return;
        }

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
        int value = (int) msg.values.values().toArray()[0];
        // TODO: maybe we should consider other cases like CRITWRITE
        // If it is a read, we should pull, if it is a write we are listening only if the value is contained in the cache
        if(msg.requestType == Config.RequestType.READ || msg.requestType == Config.RequestType.CRITREAD || this.cachedDatabase.containsKey(updatedKey)){
            // Update the value and the corresponding sequence number
            Integer currentSeqno = this.seqnoCache.get(updatedKey);
            currentSeqno = currentSeqno == null ? -1 : currentSeqno;
            // Always happens in FIFO consistency
            if(currentSeqno < msg.seqno){
                Logger.INSTANCE.info(getSelf().path().name() + ": updating the cached value for key " + updatedKey);
                // Update value
                this.cachedDatabase.remove(updatedKey);
                this.cachedDatabase.putAll(msg.values);

                // Update cache
                this.seqnoCache.remove(updatedKey);
                this.seqnoCache.put(updatedKey, msg.seqno);
            } else {
                Logger.INSTANCE.severe(getSelf().path().name() + ": not updating the cached value for key " + updatedKey + " value: " + value + " since I got a bigger sequence number " + "current " + currentSeqno + " > " + "received: " + msg.seqno + " current value: " + this.cachedDatabase.get(updatedKey) + " " + getSender().path().name());
            }
        }

        // For eventual snapshots
        capureTransitMessages(msg.values, Collections.singletonMap(updatedKey, msg.seqno), getSender());

        // Generate a new ArrayList from the message hops
        List<ActorRef> newHops = new ArrayList<>(msg.hops);
        // Save the next hop of the communication
        ActorRef sendTo = msg.hops.get(msg.hops.size()-1);
        // Remove the next hop from the new hops (basically it is the actor to which we are sending the response)
        newHops.remove(newHops.size() - 1);
        // Create the response message with the new hops
        ResponseMessage newResponseMessage = new ResponseMessage(msg.values, newHops, msg.queryUUID, msg.requestType, msg.seqno);

        // WRITE -> perform the multicast to all the peers interested
        if (this.isL1 && msg.requestType == Config.RequestType.WRITE) {
            this.multicast(newResponseMessage, this.caches);
            Logger.INSTANCE.info(getSelf().path().name() + " is multicasting " + msg.values + " to children");
        }

        // If it is an L2 cache then it sends to the client
        // If the message is a READ, regardless of the cache type it sends only to the cache that have pulled the value (pulled the request)
        if (isPendingQuery && (!this.isL1 || msg.requestType == Config.RequestType.READ || msg.requestType == Config.RequestType.CRITREAD)) {
            // Network delay
            this.delay();
            // Send the newly created response to the next hop we previously saved
            sendTo.tell(newResponseMessage, getSelf());
            Logger.INSTANCE.info(getSelf().path().name() + " is answering " + msg.values + " to " + sendTo.path().name());
        }

        // Check if the node should crash after response L1 and L2
        if((this.isL1 && nextCrash == Config.CrashType.L1_AFTER_RESPONSE) || (!this.isL1 && nextCrash == Config.CrashType.L2_AFTER_RESPONSE)) {
            this.crash(this.recoverIn);
            return;
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
        // Check if the node should crash after write L1 and L2
        if((this.isL1 && nextCrash == Config.CrashType.L1_BEFORE_WRITE) || (!this.isL1 && nextCrash == Config.CrashType.L2_BEFORE_WRITE)) {
            this.crash(this.recoverIn);
            return;
        }

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
        WriteMessage newWriteMessage = new WriteMessage(msg.requestKey, msg.modifiedValue, newHops, uuid, msg.isCritical);
        // Network delay
        this.delay();
        this.parent.tell(newWriteMessage, getSelf());

        // This message is pending
        this.pendingQueries.put(uuid, newWriteMessage);
        if (!this.isL1) {
            // Setting a scheduler for a possible timeout
            this.scheduleTimer(new TimeoutMessage(newWriteMessage, this.parent), Config.L2_TIMEOUT);
        }

        // For eventual snapshots
        // Write does not have sequence number, hence -10 it is the default
        capureTransitMessages(Collections.singletonMap(msg.requestKey, msg.modifiedValue), Collections.singletonMap(msg.requestKey, -10), getSender());

        // Check if the node should crash after write L1 and L2
        if((this.isL1 && nextCrash == Config.CrashType.L1_AFTER_WRITE) || (!this.isL1 && nextCrash == Config.CrashType.L2_AFTER_WRITE)) {
            this.crash(this.recoverIn);
            return;
        }
    }

    /**
     * Handles the timeout failure
     * In this case it simply becomes unavailable
     * @param msg timeout message
     */
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

        /**
         * Remember that the timeout is started from the L2 which is waiting for a response
         *
         * To avoid that onTimeout messages are put in the queue right after the response
         * In this case we have already addressed the queries, therefore, the cache has answered,
         * no need of setting it crashed.
         */
        if (!this.pendingQueries.containsKey(queryUUID))
            return;

        // To avoid consistency problem, this cache will not have the data updated as the L1 cache has
        // crashed, therefore we clear the cache
        // It is possible that, during the time in which the L1 cache was crashed,
        // another client had performed another write request. In this case the L2 wouldn't have the updated value
        this.clearCache();

        // Degenerate case of L2 -> L1 cache
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
                requestType,
                -1
        );
        // Network delay
        this.delay();
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

        getContext().become(this.createReceive());

        /**
         * If the cache is an L1, all the children could have inconsistent values, thus it is better to clear children
         * caches
         */
        if (this.isL1) {
            this.multicast(new FlushMessage(), this.caches);
            Logger.INSTANCE.info(getSelf().path().name() + " L1 recovery: flushing the cache and multicast flush with ID " + this.id);
        }else{
            Logger.INSTANCE.info(getSelf().path().name() + " L2 recovery: flushing the cache with ID " + this.id);
        }
    }

    /**
     * When a flush message is received
     * Just empties the local cache
     *
     * @param msg flush message
     *
     * Note that this message is sent only when a cache recovers from crashes
     */
    private void onFlushMessage(FlushMessage msg) {
        // Empty the local cache
        this.clearCache();
        Logger.INSTANCE.info(getSelf().path().name() + ": flushing the cache with ID " + this.id);

        // if I am unavailable, then it means that my father is back to life
        // So I can return an L2
        if(this.unavailable) {
            Logger.INSTANCE.info(getSelf().path().name() + ": degenerate L1 cache returns L2 with id: " + this.id);
            getContext().become(this.createReceive());
            this.isL1 = false;
            this.unavailable = false;
            this.parent = this.originalParent;
        }
    }

    /**
     * Method which crash the cache
     * @param recoverIn how much time to wait for recover
     */
    private void crash(int recoverIn){
        this.unavailable = false;
        this.nextCrash = Config.CrashType.NONE;
        this.recoverIn = 0;
        Logger.INSTANCE.severe(getSelf().path().name() + " crashed");
        getContext().become(crashed());

        // Schedule recovery timer
        this.scheduleTimer(new RecoveryMessage(), recoverIn);
    }

    /**
     * When a CrashMessage is received simulate a crash based on the provided options
     * @param msg crash message
     */
    private void onCrashMessage(CrashMessage msg) {
        this.nextCrash = msg.nextCrash;
        this.recoverIn = msg.recoverIn;
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
                .match(CrashMessage.class, this::onCrashMessage)
                .match(TokenMessage.class, msg -> onToken(
                        msg,
                        this.cachedDatabase,
                        this.seqnoCache,
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
        // Clear the cached database when the cache crashes
        this.clearCache();
        return receiveBuilder()
                .match(RecoveryMessage.class, this::onRecoveryMessage)
                .match(TokenMessage.class, msg -> onToken(
                        msg,
                        this.cachedDatabase,
                        this.seqnoCache,
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
        this.unavailable = true;
        return receiveBuilder()
                .match(ResponseMessage.class, this::onResponseMessage)
                .match(CrashMessage.class, this::onCrashMessage)
                .match(FlushMessage.class, this::onFlushMessage)
                .match(TokenMessage.class, msg -> onToken(
                        msg,
                        this.cachedDatabase,
                        this.seqnoCache,
                        Stream.concat(
                                    this.caches.stream(),
                                    Collections.singletonList(this.originalParent).stream()
                                ).collect(Collectors.toList())
                ))
                .matchAny(msg -> {})
                .build();
    }
}
