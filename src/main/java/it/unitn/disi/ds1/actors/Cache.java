package it.unitn.disi.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.Recover;
import it.unitn.disi.ds1.Config;
import it.unitn.disi.ds1.Main;
import it.unitn.disi.ds1.messages.*;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;

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
     * Logger
     */
    private final static Logger LOGGER = Logger.getLogger(Cache.class.getName());

    /**
     * Reference to the master database actor
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
    private final HashMap<Integer, Integer> cachedDatabase;

    /**
     * Cache id
     */
    private final int id;

    /**
     * Cache constructor
     * Initialize all variables
     * @param id Cache identifier
     * @param database Reference to the master database actor
     */
    public Cache(int id, ActorRef database) {
        this.database = database;
        this.caches = new ArrayList<>();
        this.cachedDatabase = new HashMap<>();
        this.id = id;
    }

    static public Props props(int id, ActorRef database) {
        return Props.create(Cache.class, () -> new Cache(id, database));
    }

    /**
     * Multicast method
     * Just multicast one serializable message to a set of nodes
     * @param msg message
     * @param multicastGroup group to whom send the message
     * @return
     */
    private void multicast(Serializable msg, List<ActorRef> multicastGroup) {
        for (ActorRef p: multicastGroup) {
            if (!p.equals(getSelf())) {
                p.tell(msg, getSelf());

                // simulate network delays using sleep
                try { Thread.sleep(Config.RANDOM.nextInt(Config.NETWORK_DELAY_MS)); }
                catch (InterruptedException e) { e.printStackTrace(); }
            }
        }
    }

    /**
     * Method which clear the cache
     */
    private void clearCache(){
        this.cachedDatabase.clear();
    }

    /**
     * Handler of JoinCachesMsg message.
     * Add all the joined caches as children
     * @param msg message containing information about the joined cache servers
     */
    private void onJoinCachesMsg(JoinCachesMsg msg) {
        this.caches.addAll(msg.caches);
        LOGGER.info(getSelf().path().name() + ": joining a the distributed cache with " + this.caches.size() + " children peers with ID " + this.id);
    }

    /**
     * Handler of the ReadMessage message
     * If the Cache has the message then it returns it otherwise, it asks to the
     * above layer (if L1 it will ask directly to the database)
     * @param msg read message
     */
    private void onReadMessage(ReadMessage msg){
        if(this.cachedDatabase.containsKey(msg.requestKey)){
            // TODO generate the answer to the sender
            LOGGER.info(getSelf().path().name() + ": cache hit with ID " + this.id);
        }else{
            // TODO ask to the above layers
            // TODO how can we discriminate which cache is which? I believe it is a good way to discriminate using
            // Classes, so Cache is Abstract and there are L1 and L2 caches which are specializations
            LOGGER.info(getSelf().path().name() + ": cache miss, asking to the parent with ID " + this.id);
        }
    }

    /**
     * Handler of the WriteMessage message
     * The request is forwarded to the database, that applies the write and sends the notification of
     * the update to all its L1 caches. In turn, all L1 caches propagate it to their connected L2 caches.
     * @param msg write message
     */
    private void onWriteMessage(WriteMessage msg){
        //TODO forward to the next layer or to the database
        LOGGER.info(getSelf().path().name() + ": forwarding the message to the parent with ID " + this.id);
    }


    /**
     * Handler of the Recovery message
     * In order to avoid issues, when one node recovers from crashes he forgot
     * all its data. Moreover, when the L1 node crashes, it communicates the L2
     * caches to flush
     *
     * @param msg write message
     */
    private void onRecoveryMessage(RecoveryMessage msg){
        // Empty the local cache
        this.clearCache();
        // TODO maybe a better way to handle it since we may know whether one node is L1 or L2?
        multicast(new FlushMessage(), this.caches);
        LOGGER.info(getSelf().path().name() + ": recovering: flushing the cache and multicast flush with ID " + this.id);
    }

    /**
     * When a flush message is received
     * Just empties the local cache
     * @param msg flush message
     */
    private void onFlushMessage(FlushMessage msg){
        // Empty the local cache
        this.clearCache();
        LOGGER.info(getSelf().path().name() + ": flushing the cache with ID " + this.id);
    }

    /**
     * Create receive method
     * @return message matcher
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinCachesMsg.class, this::onJoinCachesMsg)
                .match(ReadMessage.class, this::onReadMessage)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(RecoveryMessage.class, this::onRecoveryMessage)
                .match(FlushMessage.class, this::onFlushMessage)
                .build();
    }
}
