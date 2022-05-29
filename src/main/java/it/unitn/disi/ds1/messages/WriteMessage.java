package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Write message
 *
 * The request is forwarded to the database, that applies the write and sends the notification of
 * the update to all its L1 caches. In turn, all L1 caches propagate it to their connected L2 caches. In this
 * way, the update is potentially applied at all caches, which is necessary for eventual consistency. Note that
 * only those caches that were already storing the written item will update their local values.
 */
public class WriteMessage extends Message {
    /**
     * Request key
     */
    public final int requestKey;

    /**
     * Modified value
     */
    public final int modifiedValue;

    public final List<ActorRef> hops;

    public final UUID queryUUID;

    /**
     * Constructor of the message
     * @param requestKey key of the requested item
     * @param modifiedValue value to be modified
     */
    public WriteMessage(int requestKey, int modifiedValue, List<ActorRef> hops, UUID uuid) {
        this.requestKey = requestKey;
        this.modifiedValue = modifiedValue;
        this.hops = Collections.unmodifiableList(hops);
        if (uuid != null)
            this.queryUUID = new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        else
            this.queryUUID = null;
    }
}

