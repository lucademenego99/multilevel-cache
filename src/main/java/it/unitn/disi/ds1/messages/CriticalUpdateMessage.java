package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * CriticalUpdateMessage message
 */
public class CriticalUpdateMessage extends Message {

    public final int updatedKey, updatedValue;

    /**
     * UUID of the query
     */
    public final UUID queryUUID;

    /**
     * Set of hops which has been traveled by the message to reach the database
     */
    public final List<ActorRef> hops;

    /**
     * CriticalUpdateMessage constructor
     * @param updatedKey updated key
     * @param updatedValue updated value
     * @param uuid uuid
     * @param hops hops
     */
    public CriticalUpdateMessage(int updatedKey, int updatedValue, UUID uuid, List<ActorRef> hops) {
        this.updatedKey = updatedKey;
        this.updatedValue = updatedValue;
        this.queryUUID = new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        this.hops = Collections.unmodifiableList(hops);
    }
}
