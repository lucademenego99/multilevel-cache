package it.unitn.disi.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.messages.JoinCachesMsg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Cache server actor
 *
 * It caches database entries.
 * It can be of two types:
 * - L1: communicates with the database and returns the results
 * - L2: communicates with clients and L1 cache servers
 *
 * It handles the following requests:
 * - READ
 * - WRITE
 * - CRITREAD
 * - CRITWRITE
 *
 * This actor can crash.
 */
public class Cache extends AbstractActor {

    /**
     * Reference to the master database actor
     */
    private final ActorRef database;

    /**
     * Reference of all children cache servers
     */
    private final List<ActorRef> caches;

    /**
     * Cached entries of the database
     */
    private final HashMap<Integer, Integer> cachedDatabase;

    /**
     * Cache constructor
     * Initialize all variables
     * @param database Reference to the master database actor
     */
    public Cache(ActorRef database) {
        this.database = database;
        this.caches = new ArrayList<>();
        this.cachedDatabase = new HashMap<>();
    }

    static public Props props(ActorRef database) {
        return Props.create(Cache.class, () -> new Cache(database));
    }

    /**
     * Handler of JoinCachesMsg message.
     * Add all the joined caches as children
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
