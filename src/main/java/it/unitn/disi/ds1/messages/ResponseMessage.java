package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;

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
     * Constructor of the response message
     * @param values values in the reply
     * @param hops hops which needs to be traversed to deliver the message
     */
    public ResponseMessage(Map<Integer, Integer> values, List<ActorRef> hops) {
        this.values = Collections.unmodifiableMap(new HashMap<>(values));
        this.hops = Collections.unmodifiableList(new ArrayList<>(hops));
    }
}
