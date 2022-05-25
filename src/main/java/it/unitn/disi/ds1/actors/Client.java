package it.unitn.disi.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.messages.JoinCachesMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Client actor
 *
 * It performs READ, WRITE, CRITREAD, CRITWRITE requests to L2 cache servers
 *
 * If the L2 cache server crashes and/or the client doesn't receive a response within a given timeout,
 * it will ask the same thing to another L2 cache server
 *
 * We can safely assume a client won't perform concurrent requests
 */
public class Client extends AbstractActor {
    /**
     * Logger
     */
    private final static Logger LOGGER = Logger.getLogger(Client.class.getName());

    /**
     * List of all L2 cache servers it can communicate with
     */
    private final List<ActorRef> caches;

    /**
     * Client identifier
     */
    private final int id;

    /**
     * Client constructor
     * Initialize the target cache servers with an empty array
     */
    public Client(int id) {
        this.id = id;
        this.caches = new ArrayList<>();
    }

    static public Props props(int id) {
        return Props.create(Client.class, () -> new Client(id));
    }

    /**
     * Handler of JoinCachesMsg message.
     * Add all the joined caches as target for queries
     * @param msg message containing information about the joined cache servers
     */
    private void onJoinCachesMsg(JoinCachesMsg msg) {
        this.caches.addAll(msg.caches);
        LOGGER.info(getSelf().path().name() + ": joining a the distributed cache with " + this.caches.size() + " visible peers with ID " + this.id);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinCachesMsg.class, this::onJoinCachesMsg)
                .build();
    }
}
