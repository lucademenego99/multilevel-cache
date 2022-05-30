package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Message to handle cache servers joining the distributed cache architecture
 */
public class JoinCachesMessage extends Message {
    /**
     * List of caches joining the architecture
     */
    public final List<ActorRef> caches;   // an array of group members

    /**
     * Constructor of the message
     * @param group The group of caches joining the architecture
     */
    public JoinCachesMessage(List<ActorRef> group) {
        this.caches = Collections.unmodifiableList(new ArrayList<>(group));
    }
}
