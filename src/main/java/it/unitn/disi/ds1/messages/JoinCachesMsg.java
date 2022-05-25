package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Message to handle cache servers joining the distributed cache architecture
 * TODO: verify it works even if the class is not static
 */
public class JoinCachesMsg implements Serializable {
    /**
     * List of caches joining the architecture
     */
    public final List<ActorRef> caches;   // an array of group members

    /**
     * Constructor of the message
     * @param group The group of caches joining the architecture
     */
    public JoinCachesMsg(List<ActorRef> group) {
        this.caches = Collections.unmodifiableList(new ArrayList<>(group));
    }
}
