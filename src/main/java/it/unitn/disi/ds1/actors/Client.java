package it.unitn.disi.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.messages.JoinCachesMsg;
import it.unitn.disi.ds1.messages.RecoveryMessage;

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
public class Client extends Actor {
    /**
     * List of all L2 cache servers it can communicate with
     */
    private final List<ActorRef> caches;

    /**
     * Client constructor
     * Initialize the target cache servers with an empty array
     */
    public Client(int id) {
        super(id, Client.class.getName());
        this.caches = new ArrayList<>();
    }

    /**
     * Client static builder
     * @param id identifier of the client
     * @return Client instance
     */
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

    /**
     * Handler of the Recovery message
     * @param msg recovery message
     */
    @Override
    protected void onRecoveryMessage(RecoveryMessage msg){};

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinCachesMsg.class, this::onJoinCachesMsg)
                .build();
    }
}
