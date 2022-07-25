package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.Config;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * CriticalWriteResponseMessage message
 */
public class CriticalWriteResponseMessage extends Message {
    /**
     * Final decision enumerator
     */
    public final Config.ACResponse finalDecision;

    /**
     * UUID of the query
     */
    public final UUID queryUUID;

    /**
     * Set of hops which has been traveled by the message to reach the database
     */
    public final List<ActorRef> hops;

    /**
     * Sequence number of the value for the commit
     */
    public final Integer seqno;

    /**
     * Critical Response Message
     *
     * @param finalDecision final decision enum
     * @param uuid          uuid
     * @param hops          hops
     * @param seqno         sequence number
     */
    public CriticalWriteResponseMessage(Config.ACResponse finalDecision, UUID uuid, List<ActorRef> hops, Integer seqno) {
        this.finalDecision = finalDecision;
        this.queryUUID = new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        this.hops = Collections.unmodifiableList(hops);
        this.seqno = seqno;
    }
}
