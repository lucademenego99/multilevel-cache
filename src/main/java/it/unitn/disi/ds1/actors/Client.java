package it.unitn.disi.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.Config;
import it.unitn.disi.ds1.messages.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Client actor
 *
 * It performs READ, WRITE, CRITREAD, CRITWRITE requests to L2 cache servers
 *
 * If the L2 cache server crashes and/or the client doesn't receive a response within a given timeout,
 * it will ask the same thing to another L2 cache server
 *
 * We can safely assume a client won't perform concurrent requests
 */
public class Client extends Actor {
    /**
     * List of all L2 cache servers it can communicate with
     */
    private final List<ActorRef> caches;

    /**
     * Remember whether you are waiting for a response from the cache or not
     */
    private boolean shouldReceiveResponse;

    /**
     * Client constructor
     * Initialize the target cache servers with an empty array
     */
    public Client(int id) {
        super(id, Client.class.getName());
        this.caches = new ArrayList<>();
        this.shouldReceiveResponse = false;
    }

    /**
     * Client static builder
     * @param id identifier of the client
     * @return Client instance
     */
    static public Props props(int id) {
        return Props.create(Client.class, () -> new Client(id));
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
     * Sends a new read message request to the cache
     * @param msg Message containing the key to ask for
     */
    @Override
    protected void onReadMessage(ReadMessage msg) {
        if (this.shouldReceiveResponse)
            return;

        this.shouldReceiveResponse = true;
        ReadMessage newRequest = new ReadMessage(msg.requestKey, Collections.singletonList(getSelf()), null);
        int cacheToAskTo = (int)(Math.random() * (this.caches.size()));
        LOGGER.info("Client is sending read request for key " + msg.requestKey + " to " + this.caches.get(cacheToAskTo).path().name());
        this.caches.get(cacheToAskTo).tell(newRequest, getSelf());

        // Schedule the timer
        this.scheduleTimer(new TimeoutMessage(msg, this.caches.get(cacheToAskTo)), Config.CLIENT_TIMEOUT);
    }

    @Override
    protected void onWriteMessage(WriteMessage msg) {
        // Only one request at a time
        if (this.shouldReceiveResponse)
            return;

        WriteMessage newRequest = new WriteMessage(msg.requestKey, msg.modifiedValue);
        int cacheToAskTo = (int)(Math.random() * (this.caches.size()));
        LOGGER.info("Client is sending write request for key " + msg.requestKey + " and value " + msg.modifiedValue + " to " + this.caches.get(cacheToAskTo).path().name());
        this.caches.get(cacheToAskTo).tell(newRequest, getSelf());
    }

    @Override
    protected void onCriticalReadMessage(CriticalReadMessage msg) {

    }

    @Override
    protected void onCriticalWriteMessage(CriticalWriteMessage msg) {

    }

    @Override
    protected void onTimeoutMessage(TimeoutMessage msg){
        if (!this.shouldReceiveResponse)
            return;

        // Remove the crashed cache from the available caches (the cache becomes unavailable)
        this.caches.remove(msg.whoCrashed);

        // Ask to another cache the same thing asked before
        int cacheToAskTo = (int)(Math.random() * (this.caches.size()));
        LOGGER.info("Client is sending a read request to another cache for key " + ((ReadMessage)(msg.msg)).requestKey + " to " + this.caches.get(cacheToAskTo).path().name());
        this.caches.get(cacheToAskTo).tell(msg.msg, getSelf());

        // Schedule the timer
        this.scheduleTimer(new TimeoutMessage(msg.msg, this.caches.get(cacheToAskTo)), Config.CLIENT_TIMEOUT);
    }

    /**
     * Handler of the Recovery message
     * @param msg recovery message
     */
    @Override
    protected void onRecoveryMessage(RecoveryMessage msg){}

    /**
     * TODO: put onResponseMessage on Actor
     * Handler of the ResponseMessage
     * Print on the console the final result
     * @param msg message containing the final response
     */
    protected void onResponseMessage(ResponseMessage msg){
        this.shouldReceiveResponse = false;
        this.LOGGER.info(getSelf().path().name() + " got: " + msg.values + " from " + getSender().path().name());
        System.out.println("Requested " + msg.values.keySet().toArray()[0] + " got " + msg.values.values().toArray()[0]);

        // Cancel eventual timeout timer
        this.cancelTimer();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinCachesMessage.class, this::onJoinCachesMessage)
                .match(ReadMessage.class, this::onReadMessage)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(ResponseMessage.class, this::onResponseMessage)
                .match(TimeoutMessage.class, this::onTimeoutMessage)
                .build();
    }
}
