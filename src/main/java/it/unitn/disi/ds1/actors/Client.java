package it.unitn.disi.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.Config;
import it.unitn.disi.ds1.Logger;
import it.unitn.disi.ds1.messages.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

/**
 * Client actor
 * <p>
 * It performs READ, WRITE, CRITREAD, CRITWRITE requests to L2 cache servers
 * <p>
 * If the L2 cache server crashes and/or the client doesn't receive a response within a given timeout,
 * it will ask the same thing to another L2 cache server
 * <p>
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
     * UUID request
     */
    private UUID requestUUID;

    /**
     * Client constructor
     * Initialize the target cache servers with an empty array
     */
    public Client(int id) {
        super(id);
        this.caches = new ArrayList<>();
        this.shouldReceiveResponse = false;
    }

    /**
     * Client static builder
     *
     * @param id identifier of the client
     * @return Client instance
     */
    static public Props props(int id) {
        return Props.create(Client.class, () -> new Client(id));
    }

    /**
     * Handler of JoinCachesMessage message.
     * Add all the joined caches as target for queries
     *
     * @param msg message containing information about the joined cache servers
     */
    @Override
    protected void onJoinCachesMessage(JoinCachesMessage msg) {
        this.caches.addAll(msg.caches);
        Logger.DEBUG.info(getSelf().path().name() + ": joining a the distributed cache with " +
                this.caches.size() + " visible peers with ID " + this.id);
    }

    /**
     * Sends a new read message request to the cache
     *
     * @param msg Message containing the key to ask for
     */
    @Override
    protected void onReadMessage(ReadMessage msg) {
        // If it is waiting for a response, then it does not send anything
        // This comes from the assumptions that clients cannot send multiple request at a time, but they are blocked
        // until the response is received
        if (this.shouldReceiveResponse)
            return;

        // Generate the new request
        this.shouldReceiveResponse = true;

        // Find the sequence number associated to the request key
        Integer seqNo = this.seqnoCache.get(msg.requestKey);
        seqNo = seqNo == null ? -1 : seqNo;

        // New UUID
        this.requestUUID = UUID.randomUUID();

        // Put the seqNo inside the request
        ReadMessage newRequest = new ReadMessage(msg.requestKey, Collections.singletonList(getSelf()), requestUUID,
                msg.isCritical, seqNo.intValue());

        // Take a random client to send the request
        int cacheToAskTo = (int) (Math.random() * (this.caches.size()));

        Logger.DEBUG.info(getSelf().path().name() + " is sending read request for key " + msg.requestKey + " to " +
                this.caches.get(cacheToAskTo).path().name());
        Logger.logCheck(Level.FINE, this.id, this.getIdFromName(this.caches.get(cacheToAskTo).path().name()),
                msg.isCritical ? Config.RequestType.CRITREAD : Config.RequestType.READ, false, msg.requestKey,
                null, seqNo.intValue(), "Request read for key [CRIT: " + msg.isCritical + "]", requestUUID
        );

        // Network delay
        this.delay();
        // Forward the request
        this.caches.get(cacheToAskTo).tell(newRequest, getSelf());

        // Schedule the timer for a possible timeout
        this.scheduleTimer(new TimeoutMessage(msg, this.caches.get(cacheToAskTo)), Config.CLIENT_TIMEOUT, this.requestUUID);
    }

    /**
     * Sends a write message to a given cache
     *
     * @param msg on write message
     */
    @Override
    protected void onWriteMessage(WriteMessage msg) {
        // Only one request at a time
        if (this.shouldReceiveResponse)
            return;

        this.shouldReceiveResponse = true;

        // New UUID
        this.requestUUID = UUID.randomUUID();

        // Generate the new request message
        WriteMessage newRequest = new WriteMessage(msg.requestKey, msg.modifiedValue,
                Collections.singletonList(getSelf()), requestUUID, msg.isCritical);
        // Selects a new cache to ask to
        int cacheToAskTo = (int) (Math.random() * (this.caches.size()));

        Logger.DEBUG.info(getSelf().path().name() + " is sending write request for key " + msg.requestKey +
                " and value " + msg.modifiedValue + " to " + this.caches.get(cacheToAskTo).path().name());
        Logger.logCheck(Level.FINE, this.id, this.getIdFromName(this.caches.get(cacheToAskTo).path().name()),
                msg.isCritical ? Config.RequestType.CRITWRITE : Config.RequestType.WRITE, false,
                msg.requestKey, msg.modifiedValue, null,
                "Request write for key [CRIT: " + msg.isCritical + "]", requestUUID
        );

        // Network delay
        this.delay();
        // Forward the write request to the cache
        this.caches.get(cacheToAskTo).tell(newRequest, getSelf());

        // Schedule the timer for a possible timeout
        this.scheduleTimer(
                new TimeoutMessage(msg, this.caches.get(cacheToAskTo)), Config.CLIENT_TIMEOUT, this.requestUUID
        );
    }

    /**
     * When a timeout has been received:
     * it is expected that the client forward the request to a new client
     *
     * @param msg timeout message
     */
    @Override
    protected void onTimeoutMessage(TimeoutMessage msg) {
        // If the response has never arrived
        if (!this.shouldReceiveResponse)
            return;

        // Remove the crashed cache from the available caches (the cache becomes unavailable)
        // this.caches.remove(msg.whoCrashed);

        // New UUID
        this.requestUUID = UUID.randomUUID();

        // Ask to another cache the same thing asked before
        int cacheToAskTo = (int) (Math.random() * (this.caches.size()));

        // Tell to another cache
        int requestKey = -1, modifiedValue = -1;
        int newSeqno = -1;
        Message newMessage = null;
        String type = "-1";
        Config.RequestType reqType = Config.RequestType.READ;
        boolean critical = false;
        // Recreate the message which should be sent to a new cache
        if (msg.msg instanceof ReadMessage) {
            requestKey = ((ReadMessage) (msg.msg)).requestKey;
            newSeqno = ((ReadMessage) (msg.msg)).seqno;
            newMessage = new ReadMessage(requestKey, Collections.singletonList(getSelf()), requestUUID, ((ReadMessage) msg.msg).isCritical, newSeqno);
            critical = ((ReadMessage) msg.msg).isCritical;
            reqType = critical ? Config.RequestType.CRITREAD : Config.RequestType.READ;
            type = "read [CRITICAL] = " + critical + " [seqno] = " + newSeqno;
        } else if (msg.msg instanceof WriteMessage) {
            requestKey = ((WriteMessage) (msg.msg)).requestKey;
            modifiedValue = ((WriteMessage) (msg.msg)).modifiedValue;
            critical = ((WriteMessage) (msg.msg)).isCritical;
            reqType = critical ? Config.RequestType.CRITWRITE : Config.RequestType.WRITE;
            newMessage = new WriteMessage(requestKey, modifiedValue, Collections.singletonList(getSelf()), requestUUID, critical);
            type = "write";
        }

        Logger.DEBUG.info(getSelf().path().name() + " is sending a " + type +
                " request to another cache for key " + requestKey + " to " +
                this.caches.get(cacheToAskTo).path().name());
        Logger.logCheck(Level.FINE, this.id, this.getIdFromName(
                        this.caches.get(cacheToAskTo).path().name()), reqType,
                false, requestKey, modifiedValue, newSeqno,
                "Request to another cache for key [CRIT: " + critical + "]", requestUUID
        );

        // Network delay
        this.delay();
        // Forward the message to a new cache
        this.caches.get(cacheToAskTo).tell(newMessage, getSelf());

        // TODO: Put check if it's ReadMessage or WriteMessage
        this.requestUUID = UUID.randomUUID();
        // Schedule the timer
        this.scheduleTimer(new TimeoutMessage(msg.msg, this.caches.get(cacheToAskTo)), Config.CLIENT_TIMEOUT, this.requestUUID);
    }

    /**
     * Handler of the Recovery message
     *
     * @param msg recovery message
     */
    @Override
    protected void onRecoveryMessage(RecoveryMessage msg) { };

    /**
     * Handler of the ResponseMessage
     * Print on the console the final result
     *
     * @param msg message containing the final response
     */
    @Override
    protected void onResponseMessage(ResponseMessage msg) {
        // Cancel eventual timeout timer
        this.cancelTimer(this.requestUUID);
        this.shouldReceiveResponse = false;

        if (msg.values != null) {
            Logger.DEBUG.warning("Operation " + msg.requestType + " completed successful got " +
                    msg.values.keySet().toArray()[0] + " got " + msg.values.values().toArray()[0] +
                    " sequence number:" + msg.seqno);

            int requestKey = (Integer) msg.values.keySet().toArray()[0];
            // Override the value in the sequence number cache
            this.seqnoCache.remove(requestKey);
            this.seqnoCache.put(requestKey, msg.seqno);
        } else {
            // If the L1 cache crashed, the L2 cache became L1, so we remove it from the caches the client can communicate with
            if (msg.requestType == Config.RequestType.READ) {
                Logger.DEBUG.warning("Read operation failed");
                this.caches.remove(getSender());
            } else if (msg.requestType == Config.RequestType.WRITE) {
                Logger.DEBUG.warning("Write operation failed");
            } else if (msg.requestType == Config.RequestType.CRITREAD) {
                Logger.DEBUG.warning("CritRead operation failed");
            } else if (msg.requestType == Config.RequestType.CRITWRITE) {
                Logger.DEBUG.warning("CritWrite operation failed");
            }
        }
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
