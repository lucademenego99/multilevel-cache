package it.unitn.disi.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.messages.JoinCachesMsg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

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
public class Database extends AbstractActor {
    /**
     * Logger
     */
    private final static Logger LOGGER = Logger.getLogger(Database.class.getName());

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
    private final HashMap<Integer, Integer> database;

    /**
     * Database identifier
     */
    private final int id;

    /**
     * Database Constructor
     * Initialize variables
     * @param id database identifier
     * @param database A HashMap containing the entries of our database
     */
    public Database(int id, HashMap<Integer, Integer> database) {
        this.database = database;
        this.caches = new ArrayList<>();
        this.id = id;
    }

    static public Props props(int id, HashMap<Integer, Integer> database) {
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

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinCachesMsg.class, this::onJoinCachesMsg)
                .build();
    }
}
