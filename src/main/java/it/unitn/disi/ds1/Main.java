package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.messages.JoinCachesMsg;

import java.util.*;

/***
 * Main class of the project
 */
public class Main {

    /**
     * Number of L1 caches
     */
    final static int N_L1 = 1;

    /**
     * Number of L2 caches associated to an L1 cache
     */
    final static int N_L2 = 1;

    /**
     * Number of clients assigned to an L2 cache
     */
    final static int N_CLIENTS = 1;

    /***
     * Main function of the distributed systems project
     * @param args command line arguments
     */
    public static void main(String[] args) {
        // Create the actor system
        final ActorSystem system = ActorSystem.create("distributed-cache");

        // Set up the main architecture of the distributed cache system
        // TODO: what should we return? Do we need to create an object containing all the actors?
        setupStructure(system);

        // Shutdown system
        system.terminate();
    }

    /**
     * Set up the main structure of the distributed cache, as a tree with:
     * - a database
     * - L1 caches communicating with the database
     * - L2 caches communicating with L1 caches and clients
     * - clients performing requests to L2 caches
     *
     * @param system The actor system in use
     */
    private static void setupStructure(ActorSystem system) {
        System.out.println("Creating tree structure...");

        // Create the database
        HashMap<Integer, Integer> db = initializeDatabase();
        ActorRef database = system.actorOf(Database.props(db), "database");

        // Create N_L1 cache servers
        List<ActorRef> l1Caches = new ArrayList<>();
        for (int i = 0; i < N_L1; i++) {
            l1Caches.add(system.actorOf(Cache.props(database), "l1-cache-" + i));
        }

        // Create N_L2 cache servers
        for (int i = 0; i < l1Caches.size(); i++) {
            List<ActorRef> l2Caches = new ArrayList<>();
            for (int j = 0; j < N_L2; j++) {
                // Create the L2 cache server
                l2Caches.add(system.actorOf(Cache.props(database), "l2-cache-" + i + "-" + j));

                // Create N_CLIENTS that will communicate with it
                List<ActorRef> clients = new ArrayList<>();
                for (int k = 0; k < N_CLIENTS; k++) {
                    clients.add(system.actorOf(Client.props(), "client-" + j + "-" + k));

                    // Send the L2 cache server to the generated client
                    JoinCachesMsg cachesMsg = new JoinCachesMsg(new ArrayList<>(Collections.singletonList(l2Caches.get(j))));
                    clients.get(k).tell(cachesMsg, ActorRef.noSender());
                }
            }

            // Send to the i-th l1 cache server its children
            JoinCachesMsg l2CachesMsg = new JoinCachesMsg(l2Caches);
            l1Caches.get(i).tell(l2CachesMsg, ActorRef.noSender());
        }

        // Send to the database the list of L1 cache servers
        // TODO: maybe we don't even need this
        JoinCachesMsg l1CachesMsg = new JoinCachesMsg(l1Caches);
        database.tell(l1CachesMsg, ActorRef.noSender());

        System.out.println("Created the tree structure");
    }

    /**
     * Initialize the database with random values
     * @return The initialized database as a HashMap
     */
    private static HashMap<Integer, Integer> initializeDatabase() {
        HashMap<Integer, Integer> db = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            db.put(i, (int)(Math.random() * (100)));
        }
        return db;
    }
}
