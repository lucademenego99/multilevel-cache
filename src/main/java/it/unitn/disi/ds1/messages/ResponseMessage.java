package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.*;

public class ResponseMessage implements Serializable {
    /**
     * Map of value passed
     *
     * **NOTE**
     * Integers in Java are immutable, otherwise we would have used
     * other message passing methods
     */
    public final Map<Integer, Integer> values;

    /**
     * Hops the message needs to visit
     */
    public final List<ActorRef> hops;

    /**
     * Query UUID
     */
    public final UUID queryUUID;

    /**
     * Constructor of the response message
     * @param values values in the reply
     * @param hops hops which needs to be traversed to deliver the message
     */
    public ResponseMessage(Map<Integer, Integer> values, List<ActorRef> hops, UUID uuid) {
        this.values = Collections.unmodifiableMap(new HashMap<>(values));
        this.hops = Collections.unmodifiableList(new ArrayList<>(hops));
        this.queryUUID = new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }
}
