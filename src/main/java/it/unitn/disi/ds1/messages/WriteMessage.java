package it.unitn.disi.ds1.messages;

import java.io.Serializable;

/**
 * Write message
 *
 * The request is forwarded to the database, that applies the write and sends the notification of
 * the update to all its L1 caches. In turn, all L1 caches propagate it to their connected L2 caches. In this
 * way, the update is potentially applied at all caches, which is necessary for eventual consistency. Note that
 * only those caches that were already storing the written item will update their local values.
 */
public class WriteMessage implements Serializable {
    /**
     * Request key
     */
    public final int requestKey;

    /**
     * Modified value
     */
    public final int modifiedValue;

    /**
     * Constructor of the message
     * @param requestKey key of the requested item
     * @param modifiedValue value to be modified
     */
    public WriteMessage(int requestKey, int modifiedValue) {
        this.requestKey = requestKey;
        this.modifiedValue = modifiedValue;
    }
}

