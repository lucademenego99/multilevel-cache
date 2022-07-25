package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * CriticalUpdateTimeoutMessage message
 */
public class CriticalUpdateTimeoutMessage extends Message {
    /**
     * UUID of the query
     */
    public final UUID queryUUID;

    /**
     * Set of hops which has been traveled by the message to reach the database
     */
    public final List<ActorRef> hops;

    /**
     * CriticalUpdateTimeoutMessage
     *
     * @param queryUUID query uuid
     * @param hops      hops
     */
    public CriticalUpdateTimeoutMessage(UUID queryUUID, List<ActorRef> hops) {
        this.queryUUID = new UUID(queryUUID.getMostSignificantBits(), queryUUID.getLeastSignificantBits());
        this.hops = Collections.unmodifiableList(hops);
    }
}
