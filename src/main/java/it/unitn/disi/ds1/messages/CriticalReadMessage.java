package it.unitn.disi.ds1.messages;

/**
 * READ message
 *
 * When an L2 cache receives a Read, it responds immediately with the requested value if it is
 * found in its memory. Otherwise, it will contact the parent L1 cache. The L1 cache may respond with
 * the value, or contact the main database (typically referred to as read-through mode).
 *
 * Responses follows the path of the request backwards, until the client is reached. On the way back,
 * caches save the item for future requests.
 * Client timeouts should take into account the time for the request to reach the database.
 */
public class CriticalReadMessage extends Message {
    /**
     * Request key
     */
    public final int requestKey;

    /**
    * Constructor of the message
    * @param requestKey key of the requested item
    */
    public CriticalReadMessage(int requestKey) {
        this.requestKey = requestKey;
    }
}
