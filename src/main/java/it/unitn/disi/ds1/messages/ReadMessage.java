package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * READ message
 *
 * When an L2 cache receives a Read, it responds immediately with the requested value if it is
 * found in its memory. Otherwise, it will contact the parent L1 cache. The L1 cache may respond with
 * the value, or contact the main database (typically referred to as read-through mode).
 *
 * Responses follow the path of the request backwards, until the client is reached. On the way back,
 * caches save the item for future requests.
 * Client timeouts should take into account the time for the request to reach the database.
 */
public class ReadMessage extends Message {
    /**
     * Request key
     */
    public final int requestKey;
    public final List<ActorRef> hops;

    public final UUID queryUUID;

    /**
    * Constructor of the message
     * @param requestKey key of the requested item
     * @param hops
     */
    public ReadMessage(int requestKey, List<ActorRef> hops, UUID uuid) {
        this.requestKey = requestKey;
        this.hops = Collections.unmodifiableList(hops);
        if (uuid != null)
            this.queryUUID = new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        else
            this.queryUUID = null;
    }
}
