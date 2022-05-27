package it.unitn.disi.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.messages.JoinCachesMsg;
import it.unitn.disi.ds1.messages.RecoveryMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
     * Handler of JoinCachesMsg message.
     * Add all the joined caches as target for queries
     * @param msg message containing information about the joined cache servers
     */
    private void onJoinCachesMsg(JoinCachesMsg msg) {
        this.caches.addAll(msg.caches);
        LOGGER.info(getSelf().path().name() + ": joining a the distributed cache with " + this.caches.size() + " visible peers with ID " + this.id);
    }

    /**
     * Handler of the Recovery message
     * @param msg recovery message
     */
    @Override
    protected void onRecoveryMessage(RecoveryMessage msg){};

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinCachesMsg.class, this::onJoinCachesMsg)
                .build();
    }
}
