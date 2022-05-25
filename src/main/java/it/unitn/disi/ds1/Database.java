package it.unitn.disi.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.messages.JoinCachesMsg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
     * List of all L1 caches it communicates with
     * TODO: maybe we don't even need this...
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
     * Database Constructor
     * Initialize variables
     * @param database A HashMap containing the entries of our database
     */
    public Database(HashMap<Integer, Integer> database) {
        this.database = database;
        this.caches = new ArrayList<>();
    }

    static public Props props(HashMap<Integer, Integer> database) {
        return Props.create(Database.class, () -> new Database(database));
    }

    /**
     * Handler of JoinCachesMsg message.
     * Add all the joined caches as target for queries
     * @param msg message containing information about the joined cache servers
     */
    private void onJoinCachesMsg(JoinCachesMsg msg) {
        caches.addAll(msg.caches);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinCachesMsg.class, this::onJoinCachesMsg)
                .build();
    }
}
