package it.unitn.disi.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.messages.JoinCachesMsg;

import java.util.ArrayList;
import java.util.List;

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
     * List of all L2 cache servers it can communicate with
     */
    private final List<ActorRef> caches;

    /**
     * Client constructor
     * Initialize the target cache servers with an empty array
     */
    public Client() {
        this.caches = new ArrayList<>();
    }

    static public Props props() {
        return Props.create(Client.class, Client::new);
    }

    /**
     * Handler of JoinCachesMsg message.
     * Add all the joined caches as target for queries
     * @param msg message containing information about the joined cache servers
     */
    private void onJoinCachesMsg(JoinCachesMsg msg) {
        caches.addAll(msg.caches);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinCachesMsg.class, this::onJoinCachesMsg)
                .build();
    }
}
