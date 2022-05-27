package it.unitn.disi.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.messages.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

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
public class Cache extends Actor {
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
    private final Map<Integer, Integer> cachedDatabase;

    /**
     * Cache constructor, by default the cache is an L2
     * Initialize all variables
     * @param id Cache identifier
     * @param database Reference to the master database actor
     * @param isL2 whether the cache is an L2 cache
     */
    public Cache(int id, ActorRef database, boolean isL2) {
        super(id, Cache.class.getName());
        this.database = database;
        this.caches = new ArrayList<>();
        this.cachedDatabase = new HashMap<>();
        if(isL2){
            this.getContext().become(L2Cache());
        }
    }

    /**
     * Static class builder
     * @param id identifier
     * @param database reference to the database
     * @param isL2 whether the cache is an L2 cache
     * @return Cache instance
     */
    static public Props props(int id, ActorRef database, boolean isL2) {
        return Props.create(Cache.class, () -> new Cache(id, database, isL2));
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
        this.LOGGER.info(getSelf().path().name() + ": joining a the distributed cache with " + this.caches.size() + " children peers with ID " + this.id);
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
            this.LOGGER.info(getSelf().path().name() + ": cache hit with ID " + this.id);
        }else{
            // TODO ask to the above layers
            // TODO how can we discriminate which cache is which? I believe it is a good way to discriminate using
            // Classes, so Cache is Abstract and there are L1 and L2 caches which are specializations
            this.LOGGER.info(getSelf().path().name() + ": cache miss, asking to the parent with ID " + this.id);
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
    @Override
    protected void onRecoveryMessage(RecoveryMessage msg){
        // Empty the local cache
        this.clearCache();
        // TODO maybe a better way to handle it since we may know whether one node is L1 or L2?
        this.multicast(new FlushMessage(), this.caches);
        this.LOGGER.info(getSelf().path().name() + ": recovering: flushing the cache and multicast flush with ID " + this.id);
    }

    /**
     * When a flush message is received
     * Just empties the local cache
     * @param msg flush message
     */
    private void onFlushMessage(FlushMessage msg){
        // Empty the local cache
        this.clearCache();
        this.LOGGER.info(getSelf().path().name() + ": flushing the cache with ID " + this.id);
    }

    /**
     * Create receive method, by default the cache behaves like a L1 cache
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


    /**
     * L2 cache behaviour
     * @return
     */
    public Receive L2Cache() {
        return receiveBuilder()
                .matchAny(msg -> {})
                .build();
    }
}
