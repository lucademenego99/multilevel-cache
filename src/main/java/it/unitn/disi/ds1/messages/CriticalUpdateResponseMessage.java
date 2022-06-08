package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.Config;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class CriticalUpdateResponseMessage extends Message {
    /**
     * Either agree or no
     */
    public final Config.CUResponse response;

    /**
     * UUID of the query
     */
    public final UUID queryUUID;

    /**
     * Set of hops which has been traveled by the message to reach the database
     */
    public final List<ActorRef> hops;

    public CriticalUpdateResponseMessage(Config.CUResponse response, UUID uuid, List<ActorRef> hops) {
        this.response = response;

        this.queryUUID = new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        this.hops = Collections.unmodifiableList(hops);
    }
}
