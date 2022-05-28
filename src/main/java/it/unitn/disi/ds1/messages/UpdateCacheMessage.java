package it.unitn.disi.ds1.messages;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UpdateCacheMessage implements Serializable {
    /**
     * Map of value passed
     *
     * **NOTE**
     * Integers in Java are immutable, otherwise we would have used
     * other message passing methods
     */
    public final Map<Integer, Integer> values;

    public UpdateCacheMessage(Map<Integer, Integer> values) {
        this.values = Collections.unmodifiableMap(new HashMap<>(values));
    }
}
