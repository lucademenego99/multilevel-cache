package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;

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
    /**
     * Set of hops which has been traveled by the message to reach the database
     */
    public final List<ActorRef> hops;
    /**
     * UUID of the query
     */
    public final UUID queryUUID;
    /**
     * Is the ReadMessage a critical one?
     */
    public final boolean isCritical;

    /**
     * Sequence number for monotic read
     */
    public final int seqno;

    /**
    * Constructor of the message
     * @param requestKey key of the requested item
     * @param hops list of hops traveled by the message
     * @param uuid query uuid
     * @param seqno sequence number
     */
    public ReadMessage(int requestKey, List<ActorRef> hops, UUID uuid, boolean isCritical, int seqno) {
        this.isCritical = isCritical;
        this.requestKey = requestKey;
        this.hops = Collections.unmodifiableList(hops);
        this.seqno = seqno;
        // Copy of the UUID
        if (uuid != null)
            this.queryUUID = new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        else
            this.queryUUID = null;
    }
}
