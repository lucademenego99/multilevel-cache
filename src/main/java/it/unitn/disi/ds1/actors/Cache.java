package it.unitn.disi.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.Config;
import it.unitn.disi.ds1.Logger;
import it.unitn.disi.ds1.messages.*;

import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
     * List of values which are currently going to update though CRITWRITES
     */
    private final Map<Integer, Integer> criticalKeyValue;
    /**
     * Map uuid to key of critical writes
     */
    private final Map<UUID, Integer> criticalSessionKey;
    /**
     * Acknowledgements for saying OK to the database
     */
    private final Map<UUID, Set<ActorRef>> receivedAcksForCritWrite;
    /**
     * Reference to the parent actor
     */
    private ActorRef parent;
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
        // Initialize the critical keys
        this.criticalSessionKey = new HashMap<>();
        this.criticalKeyValue = new HashMap<>();
        this.receivedAcksForCritWrite = new HashMap<>();
    }

    /**
     * Static class builder
     *
     * @param id       identifier
     * @param parent   reference to the parent node
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
        Logger.logCheck(Level.FINE, this.id, this.id, Config.RequestType.FLUSH, true,
                null, null, null, "Crash", null);
        this.cachedDatabase.clear();
        this.seqnoCache.clear();
        this.criticalKeyValue.clear();
        this.criticalSessionKey.clear();
        this.receivedAcksForCritWrite.clear();
        // Empty pending queries
        this.pendingQueries.clear();
    }

    /**
     * Clear for critical write
     *
     * @param requestId id of the request
     */
    private void clearCriticalWrite(UUID requestId) {
        Integer oldKey = this.criticalSessionKey.get(requestId);
        // Empty
        this.criticalSessionKey.remove(requestId);
        this.criticalKeyValue.remove(oldKey);
        this.receivedAcksForCritWrite.remove(requestId);
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
        Logger.DEBUG.info(getSelf().path().name() + ": joining a the distributed cache with " +
                this.caches.size() + " children peers with ID " + this.id);
    }

    /**
     * Handler of the ReadMessage message
     * If the Cache has the message then it returns it otherwise, it asks to the
     * above layer (L1 will ask directly to the database)
     * <p>
     * The general hops idea is to keep track of who has sent the original message in a chain of requests
     * When we need to send a request we append the current node (namely the cash).
     * In this way when the database leverages on the hops constructed during the onReadMessage and use
     * them to propagate the response back.
     * <p>
     * Therefore, when we are in the database we can reconstruct the hops to whom send the request and onResponse
     * will traverse the hops backwards
     *
     * @param msg read message
     */
    @Override
    protected void onReadMessage(ReadMessage msg) {
        // Check if the node should crash before read L1 and L2
        if ((this.isL1 && nextCrash == Config.CrashType.L1_BEFORE_READ) || (!this.isL1 && nextCrash == Config.CrashType.L2_BEFORE_READ)) {
            this.crash(this.recoverIn);
            return;
        }

        // Check if the node should crash before CRITICAL read L1 and L2
        if ((this.isL1 && msg.isCritical && nextCrash == Config.CrashType.L1_BEFORE_CRIT_READ) || (!this.isL1 && nextCrash == Config.CrashType.L2_BEFORE_CRIT_READ)) {
            this.crash(this.recoverIn);
            return;
        }

        // The value is in the middle of an update
        if (this.criticalKeyValue.containsKey(msg.requestKey)) {
            Logger.DEBUG.severe(getSelf().path().name() +
                    ": got a read request on a value which is in the middle of a critical write:" +
                    msg.requestKey + " with ID " + this.id
            );

            // Get the list of hops, indicated by the read message
            List<ActorRef> newHops = new ArrayList<>(msg.hops);
            newHops.remove(newHops.size() - 1); // remove last element of the hop

            // I answer with an error message
            ResponseMessage responseMessage = new ResponseMessage(null, newHops, msg.queryUUID,
                    Config.RequestType.READ, msg.isCritical, -1);
            // Network delay
            this.delay();

            Logger.logCheck(Level.FINE, this.id, this.getIdFromName(getSender().path().name()),
                    msg.isCritical ? Config.RequestType.CRITREAD : Config.RequestType.READ,
                    true, msg.requestKey, null, msg.seqno,
                    "Response read for key Error [CRIT: " + msg.isCritical + "]", msg.queryUUID
            );

            // Send the message to the sender of the read message
            getSender().tell(responseMessage, getSelf());
            return;
        }

        // Case of a cache hit
        // IF IS CRITICAL DO NOT RETURN THE CACHED RESULT
        if (!msg.isCritical && this.cachedDatabase.containsKey(msg.requestKey)) {
            // Compare the sequence number we got
            int currentSeqno = this.seqnoCache.get(msg.requestKey);
            // I do not answer with older value
            if (msg.seqno > currentSeqno) {
                Logger.DEBUG.info(getSelf().path().name() +
                        ": got an older value with respect to one requested with key:" + msg.requestKey + " with ID " +
                        this.id + " namely: " + msg.seqno + " > " + currentSeqno
                );
                return;
            }

            Logger.DEBUG.info(getSelf().path().name() + ": cache hit of key:" + msg.requestKey + " with ID " + this.id);

            // Get the list of hops, indicated by the read message
            List<ActorRef> newHops = new ArrayList<>(msg.hops);
            newHops.remove(newHops.size() - 1); // remove last element of the hop
            // Generate a new response message which contains the cached data and the new hops
            ResponseMessage responseMessage = new ResponseMessage(
                    Collections.singletonMap(msg.requestKey, this.cachedDatabase.get(msg.requestKey)),
                    newHops, msg.queryUUID, Config.RequestType.READ, false, currentSeqno
            );

            Logger.logCheck(Level.FINE, this.id, this.getIdFromName(getSender().path().name()), Config.RequestType.READ,
                    true, msg.requestKey, this.cachedDatabase.get(msg.requestKey), msg.seqno,
                    "Response read for key [CRIT: " + false + "]", msg.queryUUID
            );

            // Network delay
            this.delay();
            // Send the message to the sender of the read message
            getSender().tell(responseMessage, getSelf());
        } else {

            // Cache miss
            Logger.DEBUG.info(getSelf().path().name() + ": cache miss of key:" + msg.requestKey +
                    " with id: " + this.id + ", asking to the parent: " + this.parent.path().name() +
                    " [CRITICAL] = " + msg.isCritical
            );

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

            Logger.logCheck(Level.FINE, this.id, this.getIdFromName(this.parent.path().name()),
                    msg.isCritical ? Config.RequestType.CRITREAD : Config.RequestType.READ,
                    false, msg.requestKey, null, msg.seqno,
                    "Request read for key [CRIT: " + msg.isCritical + "]", uuid
            );

            // Network delay
            this.delay();
            // Send the request to the parent
            this.parent.tell(newReadMessage, getSelf());

            // This message is pending, thus I add the message and the UUID in the setting
            this.pendingQueries.put(uuid, newReadMessage);
            if (!this.isL1) {
                // Setting a scheduler for a possible timeout associated to that request uuid
                this.scheduleTimer(new TimeoutMessage(newReadMessage, this.parent), Config.L2_TIMEOUT, uuid);
            }
        }

        // Check if the node should crash after CRITICAL read L1 and L2
        if ((this.isL1 && msg.isCritical && nextCrash == Config.CrashType.L1_AFTER_CRIT_READ) || (!this.isL1 && nextCrash == Config.CrashType.L2_AFTER_CRIT_READ)) {
            this.crash(this.recoverIn);
            return;
        }

        // Check if the node should crash after read L1 and L2
        if ((this.isL1 && nextCrash == Config.CrashType.L1_AFTER_READ) || (!this.isL1 && nextCrash == Config.CrashType.L2_AFTER_READ)) {
            this.crash(this.recoverIn);
            return;
        }
    }

    /**
     * Handler of the onResponseMessage
     * <p>
     * The main idea behind the response message is that the database will prepare it with the list of
     * hops which need to be traversed. In this scenario, the onResponse will only have to store the value and
     * traverse the list of hops backwards, preparing a new response message.
     *
     * @param msg response message
     */
    @Override
    protected void onResponseMessage(ResponseMessage msg) {
        // Check if the node should crash before response L1 and L2
        if ((this.isL1 && nextCrash == Config.CrashType.L1_BEFORE_RESPONSE) || (!this.isL1 && nextCrash == Config.CrashType.L2_BEFORE_RESPONSE)) {
            this.crash(this.recoverIn);
            return;
        }

        // Check if it's a pending query for the current cache
        boolean isPendingQuery = this.pendingQueries.containsKey(msg.queryUUID);
        // Remove the pending query since we got the response
        this.pendingQueries.remove(msg.queryUUID);
        if (!this.isL1) {
            // If there was a timer associated with the pending request I cancel it
            this.cancelTimer(msg.queryUUID);
        }

        // Store the result in the cached database

        if (msg.values != null) {
            int updatedKey = (Integer) msg.values.keySet().toArray()[0];
            Integer value = (Integer) msg.values.values().toArray()[0];
            // TODO: maybe we should consider other cases like CRITWRITE
            // If it is a read, we should pull, if it is a write we are listening only if the value is contained in the cache
            if (msg.requestType == Config.RequestType.READ || msg.requestType == Config.RequestType.CRITREAD || this.cachedDatabase.containsKey(updatedKey)) {
                // Update the value and the corresponding sequence number
                Integer currentSeqno = this.seqnoCache.get(updatedKey);
                currentSeqno = currentSeqno == null ? -1 : currentSeqno;
                // Always happens in FIFO consistency
                if (currentSeqno < msg.seqno) {
                    Logger.DEBUG.info(getSelf().path().name() + ": updating the cached value for key " + updatedKey);
                    // Update value
                    this.cachedDatabase.remove(updatedKey);
                    this.cachedDatabase.putAll(msg.values);

                    // Update cache
                    this.seqnoCache.remove(updatedKey);
                    this.seqnoCache.put(updatedKey, msg.seqno);
                } else {
                    Logger.DEBUG.severe(getSelf().path().name() + ": not updating the cached value for key " +
                            updatedKey + " value: " + value + " since I got a bigger sequence number " + "current " +
                            currentSeqno + " > " + "received: " + msg.seqno + " current value: " +
                            this.cachedDatabase.get(updatedKey) + " " + getSender().path().name()
                    );
                }
            }

            // For eventual snapshots
            capureTransitMessages(msg.values, Collections.singletonMap(updatedKey, msg.seqno), getSender());
        }

        // Generate a new ArrayList from the message hops
        List<ActorRef> newHops = new ArrayList<>(msg.hops);
        // Save the next hop of the communication
        ActorRef sendTo = msg.hops.get(msg.hops.size() - 1);
        // Remove the next hop from the new hops (basically it is the actor to which we are sending the response)
        newHops.remove(newHops.size() - 1);
        // Create the response message with the new hops
        ResponseMessage newResponseMessage = new ResponseMessage(msg.values, newHops, msg.queryUUID, msg.requestType,
                msg.isCritical, msg.seqno);

        // WRITE -> perform the multicast to all the peers interested
        if (this.isL1 && msg.requestType == Config.RequestType.WRITE) {
            // TODO: here we don't have the requestKey
            this.multicastAndCheck(newResponseMessage, this.caches, msg.requestType,
                    msg.values == null ? null : (int) msg.values.keySet().toArray()[0],
                    msg.values == null ? null : (int) msg.values.values().toArray()[0],
                    msg.seqno, msg.isCritical, msg.queryUUID
            );
            Logger.DEBUG.info(getSelf().path().name() + " is multicasting " + msg.values + " to children");
        }

        // If it is an L2 cache then it sends to the client
        // If the message is a READ, regardless of the cache type it sends only to the cache that have pulled the value (pulled the request)
        // Also for returning the WRITE answer to the client
        // Notice: the other L2 caches won't answer to the client because isPendingQuery resolves to false
        if (isPendingQuery && (!this.isL1 || msg.requestType == Config.RequestType.READ || msg.requestType == Config.RequestType.CRITREAD)) {
            Logger.logCheck(Level.FINE, this.id, this.getIdFromName(sendTo.path().name()), msg.requestType,
                    true, msg.values == null ? null : (Integer) msg.values.keySet().toArray()[0],
                    msg.values == null ? null : (Integer) msg.values.values().toArray()[0], msg.seqno,
                    "Response for key [CRIT: " + msg.isCritical + "]", msg.queryUUID
            );

            // Network delay
            this.delay();
            // Send the newly created response to the next hop we previously saved
            sendTo.tell(newResponseMessage, getSelf());
            Logger.DEBUG.info(getSelf().path().name() + " is answering " + msg.values + " to " + sendTo.path().name());
        }

        // Check if the node should crash after response L1 and L2
        if ((this.isL1 && nextCrash == Config.CrashType.L1_AFTER_RESPONSE) || (!this.isL1 && nextCrash == Config.CrashType.L2_AFTER_RESPONSE)) {
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
        if ((this.isL1 && nextCrash == Config.CrashType.L1_BEFORE_WRITE) || (!this.isL1 && nextCrash == Config.CrashType.L2_BEFORE_WRITE)) {
            this.crash(this.recoverIn);
            return;
        }

        // Check if the node should crash after critical write L1 and L2
        if ((this.isL1 && nextCrash == Config.CrashType.L1_BEFORE_CRIT_WRITE) || (!this.isL1 && nextCrash == Config.CrashType.L2_BEFORE_CRIT_WRITE)) {
            this.crash(this.recoverIn);
            return;
        }

        // The value is in the middle of an update
        if (this.criticalKeyValue.containsKey(msg.requestKey)) {
            Logger.DEBUG.severe(getSelf().path().name() +
                    ": got a write request on a value which is in the middle of a critical write:" +
                    msg.requestKey + " with ID " + this.id
            );

            // Get the list of hops, indicated by the read message
            List<ActorRef> newHops = new ArrayList<>(msg.hops);
            newHops.remove(newHops.size() - 1); // remove last element of the hop

            // I answer with an error message
            ResponseMessage responseMessage = new ResponseMessage(null, newHops, msg.queryUUID, Config.RequestType.READ, msg.isCritical, -1);

            Logger.logCheck(Level.FINE, this.id, this.getIdFromName(getSender().path().name()),
                    msg.isCritical ? Config.RequestType.CRITREAD : Config.RequestType.READ, true,
                    msg.requestKey, null, -1,
                    "Response write for key Error [CRIT: " + msg.isCritical + "]",
                    msg.queryUUID
            );
            // Network delay
            this.delay();
            // Send the message to the sender of the read message
            getSender().tell(responseMessage, getSelf());
            return;
        }

        Logger.DEBUG.info(getSelf().path().name() + ": forwarding the message to the parent with ID " + this.id);

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
        // TODO: seqno?
        Logger.logCheck(Level.FINE, this.id, this.getIdFromName(this.parent.path().name()),
                msg.isCritical ? Config.RequestType.CRITWRITE : Config.RequestType.WRITE, false,
                msg.requestKey, msg.modifiedValue, -1,
                "Request write for key [CRIT: " + msg.isCritical + "]", uuid
        );
        // Network delay
        this.delay();
        this.parent.tell(newWriteMessage, getSelf());

        // This message is pending
        this.pendingQueries.put(uuid, newWriteMessage);
        if (!this.isL1) {
            // Setting a scheduler for a possible timeout associated with uuid
            this.scheduleTimer(new TimeoutMessage(newWriteMessage, this.parent), Config.L2_TIMEOUT, uuid);
        }

        // For eventual snapshots
        // Write does not have sequence number, hence -10 it is the default
        capureTransitMessages(Collections.singletonMap(msg.requestKey, msg.modifiedValue),
                Collections.singletonMap(msg.requestKey, -10), getSender());

        // Check if the node should crash after critical write L1 and L2
        if ((this.isL1 && nextCrash == Config.CrashType.L1_AFTER_CRIT_WRITE) || (!this.isL1 && nextCrash == Config.CrashType.L2_AFTER_CRIT_WRITE)) {
            this.crash(this.recoverIn);
            return;
        }

        // Check if the node should crash after write L1 and L2
        if ((this.isL1 && nextCrash == Config.CrashType.L1_AFTER_WRITE) || (!this.isL1 && nextCrash == Config.CrashType.L2_AFTER_WRITE)) {
            this.crash(this.recoverIn);
            return;
        }
    }

    protected void onCriticalUpdateMessage(CriticalUpdateMessage msg) {
        // Locking the value
        this.criticalSessionKey.put(msg.queryUUID, msg.updatedKey);
        this.criticalKeyValue.put(msg.updatedKey, msg.updatedValue);

        if (this.isL1) {
            // Send the critical update message to L2 caches - we expect an acknowledgement containing COMMIT/ABORT
            this.multicast(
                    new CriticalUpdateMessage(msg.updatedKey, msg.updatedValue, msg.queryUUID, msg.hops),
                    this.caches
            );

            Logger.DEBUG.info(getSelf().path().name() +
                    " sending the update messages to my children, hope they will answer OK for " + msg.updatedKey +
                    " value:" + msg.updatedValue
            );
            // If the L1 cache doesn't receive an acknowledgement within a given timeout, abort the write and return error
            this.scheduleTimer(
                    new CriticalUpdateTimeoutMessage(msg.queryUUID, msg.hops), Config.CRIT_WRITE_TIME_OUT, msg.queryUUID
            );
        } else {
            Logger.DEBUG.info(getSelf().path().name() + " sending the OK message to the parent " + msg.updatedKey +
                    " value:" + msg.updatedValue
            );

            // Send acknowledgement to the L1 cache
            this.parent.tell(new CriticalUpdateResponseMessage(Config.CUResponse.OK, msg.queryUUID, msg.hops), getSelf());
        }
    }

    protected void onCriticalUpdateTimeoutMessage(CriticalUpdateTimeoutMessage msg) {
        /**
         * Remember that the timeout is started from the database which is waiting for a response
         *
         * To avoid that onTimeout messages are put in the queue right after the response
         * In this case we have already addressed the queries, therefore, the all the caches have answered
         */
        if (!this.criticalSessionKey.containsKey(msg.queryUUID)) {
            return;
        }

        Logger.DEBUG.warning(getSelf().path().name() + " timed out for key " + this.criticalSessionKey.get(msg.queryUUID) + ", sending NO response to the database");
        // Network delay
        this.delay();
        // If the L2 cache didn't respond in time, send abort to the database
        this.parent.tell(new CriticalUpdateResponseMessage(Config.CUResponse.NO, msg.queryUUID, msg.hops), getSelf());
    }

    protected void onCriticalUpdateResponseMessage(CriticalUpdateResponseMessage msg) {
        Integer key = this.criticalSessionKey.get(msg.queryUUID);
        Integer value = this.criticalKeyValue.get(key);
        // Got an OK -> voted yes
        // I got OK from an L2 cache
        if (msg.response == Config.CUResponse.OK && this.isL1) {
            // Add the sender to the list of received acknowledgements
            if (!this.receivedAcksForCritWrite.containsKey(msg.queryUUID)) {
                this.receivedAcksForCritWrite.put(msg.queryUUID, new HashSet<>());
            }
            this.receivedAcksForCritWrite.get(msg.queryUUID).add(getSender());

            // If the L1 cache has received all acknowledgements, proceed with the protocol's flow
            if (this.receivedAcksForCritWrite.get(msg.queryUUID).containsAll(this.caches)) {
                // Network delay
                this.delay();
                // Send OK to the database, since all children L2 caches have sent an acknowledged
                this.parent.tell(new CriticalUpdateResponseMessage(Config.CUResponse.OK, msg.queryUUID, msg.hops), getSelf());

                Logger.DEBUG.info(getSelf().path().name() +
                        " L1 cache got a CriticalUpdateResponseMessage with all OK, sending it to the parent! for " +
                        key + " value: " + value
                );
            }
        } else if (msg.response == Config.CUResponse.NO && this.isL1) {
            // NEVER HERE, L2 will never decide NO

            // Network delay
            this.delay();
            // Got NO from an L2 cache - send NO to the database
            this.parent.tell(new CriticalUpdateResponseMessage(Config.CUResponse.NO, msg.queryUUID, msg.hops), getSelf());

            Logger.DEBUG.info(getSelf().path().name() +
                    " L1 cache got a CriticalUpdateResponseMessage with one NO, sending it to the parent! for " + key +
                    " value: " + value
            );
        } else {
            Logger.DEBUG.severe(getSelf().path().name() + " L2 cache got a CriticalUpdateResponseMessage");
        }
    }

    protected void onCriticalWriteResponseMessage(CriticalWriteResponseMessage msg) {
        int keyToUpdate = this.criticalSessionKey.get(msg.queryUUID);
        int newValue = this.criticalKeyValue.get(keyToUpdate);

        // Generate a new ArrayList from the message hops
        List<ActorRef> newHops = new ArrayList<>(msg.hops);
        // Save the next hop of the communication
        ActorRef sendTo = msg.hops.get(msg.hops.size() - 1);
        // Remove the next hop from the new hops (basically it is the actor to which we are sending the response)
        newHops.remove(newHops.size() - 1);

        if (msg.finalDecision == Config.ACResponse.COMMIT) {
            // Got COMMIT
            Logger.DEBUG.info(getSelf().path().name() + " got COMMIT decision from parent and key " + keyToUpdate);
            // If the key was already inside the cachedDatabase, update it

            // Update new value
            if (this.cachedDatabase.containsKey(keyToUpdate)) {
                this.cachedDatabase.remove(keyToUpdate);
                this.cachedDatabase.put(keyToUpdate, newValue);

                // Override the value in the sequence number cache
                this.seqnoCache.remove(keyToUpdate);
                this.seqnoCache.put(keyToUpdate, msg.seqno);
            }

            // Clear critical writes value
            this.clearCriticalWrite(msg.queryUUID);

            // Send commit to the caches with the new sequence number to be updated
            this.multicastAndCheck(
                    new CriticalWriteResponseMessage(Config.ACResponse.COMMIT, msg.queryUUID, newHops, msg.seqno),
                    this.caches, Config.RequestType.CRITWRITE, keyToUpdate, newValue, msg.seqno, true,
                    msg.queryUUID
            );
        } else {
            // Got ABORT
            Logger.DEBUG.warning(getSelf().path().name() + " got ABORT decision from parent and key " + keyToUpdate);
            this.clearCriticalWrite(msg.queryUUID);
            // TODO: in all other cases we used seqno -1 when there was an error - is it correct that here null is used?
            this.multicastAndCheck(
                    new CriticalWriteResponseMessage(Config.ACResponse.ABORT, msg.queryUUID, newHops, null),
                    this.caches, Config.RequestType.CRITWRITE, keyToUpdate, null, -1, true, msg.queryUUID
            );
        }

        // Check if it's a pending query for the current cache
        boolean isPendingQuery = this.pendingQueries.containsKey(msg.queryUUID);

        // Send final response to the client if the cache is L2
        if (!this.isL1 && isPendingQuery) {
            // Now that we got the response, remove the request from pendingQueries
            this.pendingQueries.remove(msg.queryUUID);

            // Create the response message with the new hops
            HashMap<Integer, Integer> responseMap = new HashMap<>();
            responseMap.put(keyToUpdate, newValue);
            ResponseMessage newResponseMessage = new ResponseMessage(
                    msg.finalDecision == Config.ACResponse.COMMIT ? responseMap : null, newHops, msg.queryUUID,
                    Config.RequestType.CRITWRITE, true, msg.seqno
            );

            Logger.logCheck(Level.FINE, this.id, this.getIdFromName(sendTo.path().name()), Config.RequestType.CRITWRITE,
                    true, keyToUpdate, msg.finalDecision == Config.ACResponse.COMMIT ? newValue : null,
                    msg.seqno, "Response for key [CRIT: " + true + "]", msg.queryUUID);

            // Network delay
            this.delay();
            // Send the newly created response to the next hop we previously saved
            sendTo.tell(newResponseMessage, getSelf());
        }
    }

    /**
     * Handles the timeout failure
     * In this case it simply becomes unavailable
     *
     * @param msg timeout message
     */
    @Override
    protected void onTimeoutMessage(TimeoutMessage msg) {
        // TODO: take care that when L1 respawn this cache need to return L2
        // or simply become unavailable
        int requestKey = 0;
        UUID queryUUID = null;
        List<ActorRef> hops = null;
        Config.RequestType requestType = null;
        boolean isCritical = false;
        if (msg.msg instanceof ReadMessage) {
            requestKey = ((ReadMessage) (msg.msg)).requestKey;
            queryUUID = ((ReadMessage) (msg.msg)).queryUUID;
            hops = ((ReadMessage) (msg.msg)).hops;
            requestType = Config.RequestType.READ;
            isCritical = ((ReadMessage) (msg.msg)).isCritical;
        } else if (msg.msg instanceof WriteMessage) {
            requestKey = ((WriteMessage) (msg.msg)).requestKey;
            queryUUID = ((WriteMessage) (msg.msg)).queryUUID;
            hops = ((WriteMessage) (msg.msg)).hops;
            requestType = Config.RequestType.WRITE;
            isCritical = ((WriteMessage) (msg.msg)).isCritical;
        }
        // TODO: CriticalRead and CriticalWrite?

        // Remove self from the hops, only the client will remain
        hops = new ArrayList<>(hops);
        hops.remove(hops.size() - 1);

        /**
         * Remember that the timeout is started from the L2 which is waiting for a response
         *
         * To avoid that onTimeout messages are put in the queue right after the response
         * In this case we have already addressed the queries, therefore, the cache has answered,
         * no need of setting it crashed.
         */
        if (!this.pendingQueries.containsKey(queryUUID)) return;

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
        Logger.DEBUG.info("Cache timed-out: " + msg.whoCrashed.path().name() + " has probably crashed");

        // Send the error message
        Logger.DEBUG.info("Sending error message");
        ResponseMessage responseMessage = new ResponseMessage(null, hops, queryUUID,       // Encapsulating the query UUID
                requestType, isCritical, -1);

        Logger.logCheck(Level.FINE, this.id, this.getIdFromName(hops.get(hops.size() - 1).path().name()), requestType,
                true, requestKey, null, -1,
                "Request write for key [CRIT: " + isCritical + "]", queryUUID
        );

        // Network delay
        this.delay();
        // Send to the client the empty response
        hops.get(hops.size() - 1).tell(responseMessage, getSelf());
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
            Logger.DEBUG.info(getSelf().path().name() + " L1 recovery: flushing the cache and multicast flush with ID " + this.id);
        } else {
            Logger.DEBUG.info(getSelf().path().name() + " L2 recovery: flushing the cache with ID " + this.id);
        }
    }

    /**
     * When a flush message is received
     * Just empties the local cache
     *
     * @param msg flush message
     *            <p>
     *            Note that this message is sent only when a cache recovers from crashes
     */
    private void onFlushMessage(FlushMessage msg) {
        // Empty the local cache
        this.clearCache();
        Logger.DEBUG.info(getSelf().path().name() + ": flushing the cache with ID " + this.id);

        // if I am unavailable, then it means that my father is back to life
        // So I can return an L2
        if (this.unavailable) {
            Logger.DEBUG.info(getSelf().path().name() + ": degenerate L1 cache returns L2 with id: " + this.id);
            getContext().become(this.createReceive());
            this.isL1 = false;
            this.unavailable = false;
            this.parent = this.originalParent;
        }
    }

    /**
     * Method which crash the cache
     *
     * @param recoverIn how much time to wait for recover
     */
    private void crash(int recoverIn) {
        this.unavailable = false;
        this.nextCrash = Config.CrashType.NONE;
        this.recoverIn = 0;
        Logger.DEBUG.severe(getSelf().path().name() + " crashed");
        getContext().become(crashed());

        // Schedule recovery timer
        this.scheduleDetatchedTimer(new RecoveryMessage(), recoverIn);
    }

    /**
     * When a CrashMessage is received simulate a crash based on the provided options
     *
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
                .match(CriticalUpdateMessage.class, this::onCriticalUpdateMessage)
                .match(CriticalUpdateResponseMessage.class, this::onCriticalUpdateResponseMessage)
                .match(CriticalUpdateTimeoutMessage.class, this::onCriticalUpdateTimeoutMessage)
                .match(CriticalWriteResponseMessage.class, this::onCriticalWriteResponseMessage)
                .match(CrashMessage.class, this::onCrashMessage).match(
                        TokenMessage.class, msg -> onToken(msg, this.cachedDatabase, this.seqnoCache,
                                Stream.concat(this.caches.stream(), Collections.singletonList(this.parent).stream())
                                        .collect(Collectors.toList())))
                .build();
    }

    /**
     * Crashed behavior
     *
     * @return builder
     */
    @Override
    public Receive crashed() {
        // Clear the cached database when the cache crashes
        this.clearCache();
        return receiveBuilder()
                .match(RecoveryMessage.class, this::onRecoveryMessage)
                .match(TokenMessage.class, msg -> onToken(
                        msg, this.cachedDatabase, this.seqnoCache,
                        Stream.concat(this.caches.stream(), Collections.singletonList(this.parent).stream())
                                .collect(Collectors.toList())))
                .matchAny(msg -> {
                })
                .build();
    }

    /**
     * Unavailable behavior
     *
     * @return builder
     */
    public Receive unavailable() {
        this.unavailable = true;
        return receiveBuilder().match(ResponseMessage.class, this::onResponseMessage)
                .match(CrashMessage.class, this::onCrashMessage)
                .match(FlushMessage.class, this::onFlushMessage)
                .match(TokenMessage.class, msg -> onToken(
                        msg, this.cachedDatabase, this.seqnoCache,
                        Stream.concat(this.caches.stream(), Collections.singletonList(this.originalParent).stream())
                                .collect(Collectors.toList())))
                .matchAny(msg -> {
                })
                .build();
    }
}
