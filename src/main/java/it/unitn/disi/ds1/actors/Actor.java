package it.unitn.disi.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.disi.ds1.Config;
import it.unitn.disi.ds1.messages.RecoveryMessage;
import it.unitn.disi.ds1.messages.TokenMessage;

import java.io.Serializable;
import java.util.Collections;
import java.util.logging.Logger;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Actor base class
 * Provides the actor base functionality
 */
public abstract class Actor extends AbstractActor {
    /**
     * Logger
     */
    protected final Logger LOGGER;

    /**
     * Peer id
     */
    public final int id;

    /**
     * Whether the node is in the snapshot yet, namely if the
     * state has been captured.
     * If this variable is true, it means that the snapshot is in progress
     */
    protected boolean stateCaptured = false;

    /**
     * Current cache/database
     */
    protected Map<Integer, Integer> currentCache = new HashMap<>();

    /**
     * Data in transit
     */
    protected Map<Integer, Integer> dataInTransit = new HashMap<>();

    /**
     * Set which considers the token which has been received
     */
    protected Set<ActorRef> tokensReceived = new HashSet<>();

    /**
     * Snapshot identifier
     */
    protected int snapshotId = 0;

    /**
     * Constructor of the Actor base class
     * @param id identifier of the peer
     * @param loggerName name of the logger
     */
    public Actor(int id, String loggerName){
        this.id = id;
        this.LOGGER = Logger.getLogger(loggerName);
    }

    /**
     * Multicast method
     * Just multicast one serializable message to a set of nodes
     * @param msg message
     * @param multicastGroup group to whom send the message
     * @return
     */
    protected void multicast(Serializable msg, List<ActorRef> multicastGroup) {
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
     * Send the token to all the peers
     * @param peers List of peers
     */
    protected void sendTokens(List<ActorRef> peers) {
        this.LOGGER.info(this.LOGGER.getName() + " with id " + this.id +" sending tokens");
        TokenMessage t = new TokenMessage(this.snapshotId);
        this.multicast(t, peers);
    }

    /**
     * Tells whether the snapshot has ended
     * @param peers
     */
    protected boolean snapshotEnded(List<ActorRef> peers){
        this.LOGGER.info(this.LOGGER.getName() + " with id " + this.id +" has ended the snapshot");
        return this.tokensReceived.containsAll(peers);
    }

    /**
     * Capture the current data within the system
     * @param data either the cache or the database
     */
    protected void captureState(Map data) {
        this.LOGGER.info(this.LOGGER.getName() + " with id" + this.id + " snapId: "+ this.snapshotId + " current data: " + this.currentCache + " data in transit" + this.dataInTransit);
        // State set to captured
        this.stateCaptured = true;
        // This means that the snapshot is not stored yet.
        this.currentCache = Collections.unmodifiableMap(data);
        // Add itself to the tokens received
        this.tokensReceived.add(getSelf());
    }

    /**
     * On Recovery method
     * @param msg recovery message
     */
    protected abstract void onRecoveryMessage(RecoveryMessage msg);

    /**
     * Crashed behavior
     * @return builder
     */
    public Receive crashed() {
        return receiveBuilder()
                .match(RecoveryMessage.class, this::onRecoveryMessage)
                .matchAny(msg -> {})
                .build();
    }
}
