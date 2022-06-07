package it.unitn.disi.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.Config;
import it.unitn.disi.ds1.Logger;
import it.unitn.disi.ds1.messages.*;

import java.util.*;

/**
 * Database actor
 *
 * It stores inside a variable a key-value pairs HashSet
 * It communicates with L1 cache servers, handling the following requests:
 * - READ
 * - WRITE
 * - CRITREAD
 * - CRITWRITE
 *
 * We can take for granted this actor doesn't crash
 */
public class Database extends Actor {
    /**
     * List of all L1 caches it communicates with
     */
    private final List<ActorRef> caches;

    /**
     * The database is stored inside this variable as
     * key-value integer pairs
     *
     * We can assume there's infinite space
     */
    private final Map<Integer, Integer> database;

    /**
     * Database Constructor
     * Initialize variables
     * @param id database identifier
     * @param database A Map containing the entries of our database
     */
    public Database(int id, Map<Integer, Integer> database) {
        super(id);
        this.database = database;
        this.caches = new ArrayList<>();
        // Initialize the sequence numbers at zero
        this.database.forEach((k, v) -> this.seqnoCache.put(k, 0));
    }

    @Override
    public void preStart(){
        // TODO schedule the things
    }

    /**
     * Database static builder
     * @param id database identifier
     * @param database database values
     * @return Database instance
     */
    static public Props props(int id, Map<Integer, Integer> database) {
        return Props.create(Database.class, () -> new Database(id, database));
    }

    /**
     * Handler of JoinCachesMessage message.
     * Add all the joined caches as target for queries
     * @param msg message containing information about the joined cache servers
     */
    @Override
    protected void onJoinCachesMessage(JoinCachesMessage msg) {
        this.caches.addAll(msg.caches);
        Logger.INSTANCE.info(getSelf().path().name() + ": joining a the distributed cache with " + this.caches.size() + " visible peers with ID " + this.id);
    }

    /**
     * Handler of the ReadMessage message.
     * Get the value for the specified key and send back the response to the sender
     * **NOTE**: by assumption of the assignment, all the value requested will concern values which are present in
     * the database
     *
     * @param msg message containing the queried key and the list of the communication hops
     */
    @Override
    protected void onReadMessage(ReadMessage msg) {
        // Generate a new ArrayList from the message hops
        List<ActorRef> newHops = new ArrayList<>(msg.hops);

        // Remove the next hop from the new hops array
        // The hops contains the nodes which have been traveled to reach the database
        newHops.remove(newHops.size() - 1);

        // Return the sequence number
        Integer seqno = this.seqnoCache.get(msg.requestKey);

        // Generate a response message containing the response and the new hops array
        ResponseMessage responseMessage = new ResponseMessage(
                Collections.singletonMap(msg.requestKey, this.database.get(msg.requestKey)),
                newHops,
                msg.queryUUID,                  // Encapsulating the query UUID
                msg.isCritical ? Config.RequestType.CRITREAD : Config.RequestType.READ,
                seqno
        );

        // Network delay
        this.delay();
        // Send the response back to the sender
        getSender().tell(responseMessage, getSelf());

        Logger.INSTANCE.info(getSelf().path().name() + " is answering " + msg.requestKey + " to: " + getSender().path().name() + " sequence number: " + seqno + " [CRITICAL] = " + msg.isCritical);
    }

    /**
     * Handler of the WriteMessage
     * The function overrides the element in the database
     * and sends the update to all the cache using multicast
     * @param msg
     */
    @Override
    protected void onWriteMessage(WriteMessage msg) {
        // Override the value in the database
        this.database.remove(msg.requestKey);
        this.database.put(msg.requestKey, msg.modifiedValue);

        // Generate a new ArrayList from the message hops
        // The hops contains the nodes which have been traveled to reach the database
        List<ActorRef> newHops = new ArrayList<>(msg.hops);

        // Update the sequence number
        Integer newSeqno = this.seqnoCache.get(msg.requestKey);
        newSeqno++;

        // Override the value in the sequence number cache
        this.seqnoCache.remove(msg.requestKey);
        this.seqnoCache.put(msg.requestKey, newSeqno);

        // Remove the next hop from the new hops array
        newHops.remove(newHops.size() - 1);

        Logger.INSTANCE.info(getSelf().path().name() + ": forwarding the new value for " + msg.requestKey + " to: " + getSender().path().name() + " sequence number " + newSeqno);

        // Multicast to the cache the update
        this.multicast(new ResponseMessage(Collections.singletonMap(msg.requestKey, msg.modifiedValue), newHops, msg.queryUUID, Config.RequestType.WRITE, newSeqno), this.caches);
    }

    @Override
    protected void onResponseMessage(ResponseMessage msg){};

    @Override
    protected void onTimeoutMessage(TimeoutMessage msg){};

    /**
     * Handler of the Recovery message
     * @param msg recovery message
     */
    @Override
    protected void onRecoveryMessage(RecoveryMessage msg){};

    /**
     * Handler of the messages
     *
     * It handles:
     * {@link JoinCachesMessage join message}
     * {@link TokenMessage token message} for distributed snapshot
     * {@link StartSnapshotMessage start snapshot message} for starting the snapshot, since the only node which
     * is connected with a spanning tree with all the other components
     * {@link ReadMessage join message}
     * {@link WriteMessage join message}
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinCachesMessage.class, this::onJoinCachesMessage)
                .match(TokenMessage.class, msg -> onToken(msg, this.database, this.seqnoCache, this.caches))
                .match(StartSnapshotMessage.class, msg -> onStartSnapshot(msg, this.database, this.seqnoCache, this.caches))
                .match(ReadMessage.class, this::onReadMessage)
                .match(WriteMessage.class, this::onWriteMessage)
                .build();
    }
}
