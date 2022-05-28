package it.unitn.disi.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
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
        super(id, Database.class.getName());
        this.database = database;
        this.caches = new ArrayList<>();
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
        LOGGER.info(getSelf().path().name() + ": joining a the distributed cache with " + this.caches.size() + " visible peers with ID " + this.id);
    }

    /**
     * Handler of the ReadMessage message.
     * Get the value for the specified key and send back the response to the sender
     * @param msg message containing the queried key and the list of the communication hops
     */
    @Override
    protected void onReadMessage(ReadMessage msg) {
        // Generate a new ArrayList from the message hops
        List<ActorRef> newHops = new ArrayList<>(msg.hops);

        // Remove the next hop from the new hops array
        newHops.remove(newHops.size() - 1);

        // Generate a response message containing the response and the new hops array
        ResponseMessage responseMessage = new ResponseMessage(
                Collections.singletonMap(msg.requestKey, this.database.get(msg.requestKey)),
                newHops,
                msg.queryUUID       // Encapsulating the query UUID
        );

        // Send the response back to the sender
        getSender().tell(responseMessage, getSelf());

        this.LOGGER.info(getSelf().path().name() + " is answering " + msg.requestKey + " to: " + getSender().path().name());
    }

    @Override
    protected void onWriteMessage(WriteMessage msg) {
        this.database.remove(msg.requestKey);
        this.database.put(msg.requestKey, msg.modifiedValue);

        this.LOGGER.info(getSelf().path().name() + ": forwarding the new value for " + msg.requestKey + " to: " + getSender().path().name());

        multicast(new UpdateCacheMessage(Collections.singletonMap(msg.requestKey, msg.modifiedValue)), this.caches);
    }

    @Override
    protected void onCriticalReadMessage(CriticalReadMessage msg) {

    }

    @Override
    protected void onCriticalWriteMessage(CriticalWriteMessage msg) {

    }

    @Override
    protected void onTimeoutMessage(TimeoutMessage msg){};

    /**
     * Handler of the Recovery message
     * @param msg recovery message
     */
    @Override
    protected void onRecoveryMessage(RecoveryMessage msg){};

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinCachesMessage.class, this::onJoinCachesMessage)
                .match(TokenMessage.class, msg -> onToken(msg, this.database, this.caches))
                .match(StartSnapshotMessage.class, msg -> onStartSnapshot(msg, this.database, this.caches))
                .match(ReadMessage.class, this::onReadMessage)
                .match(WriteMessage.class, this::onWriteMessage)
                .build();
    }
}
