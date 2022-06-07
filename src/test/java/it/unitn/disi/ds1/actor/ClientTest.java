package it.unitn.disi.ds1.actor;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import it.unitn.disi.ds1.Logger;
import it.unitn.disi.ds1.Main;
import it.unitn.disi.ds1.actors.Client;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test of the client
 */
public class ClientTest {
    /**
     * Logger instance
     */
    private Logger LOG;

    /**
     * Client
     */
    private ActorRef client;

    /**
     * L1
     */
    private ActorRef L1;

    /**
     * L2
     */
    private ActorRef L2;

    /**
     * Actor reference system
     */
    private ActorSystem system;

    /**
     * Before all, create the setup for the test
     */
    @BeforeAll
    public void setup() {
        // Initialize the Logger
        this.LOG.initLogger();
        LOG.INSTANCE.info("startup - crearing test configuration");

        this.system = ActorSystem.create("test-client");
        int id = 1;
        this.client = (ActorRef) system.actorOf(Client.props(id++), "client");
    }

    /**
     * Test whether no exception are thrown in the main function
     */
    @Test
    public void verifySendMessage(){
        /*ActorRef pinger = testKit.spawn(Echo.create(), "ping");
        TestProbe probe = testKit.createTestProbe();
        pinger.tell(new Echo.Ping("hello", probe.ref()));
        probe.expectMessage(new Echo.Pong("hello"));*/
    }
}
