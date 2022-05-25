package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.actors.Cache;
import it.unitn.disi.ds1.actors.Client;
import it.unitn.disi.ds1.actors.Database;
import it.unitn.disi.ds1.messages.JoinCachesMsg;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/***
 * Main class of the project
 */
public class Main {
    /**
     * Logger
     */
    private final static Logger LOGGER = Logger.getLogger(Main.class.getName());

    /***
     * Main function of the distributed systems project
     * @param args command line arguments
     */
    public static void main(String[] args) {
        // Create the actor system
        final ActorSystem system = ActorSystem.create("distributed-cache");

        // Logger, TODO, maybe a better logger, at least a file where to save this?
        LOGGER.setLevel(Level.INFO);
        System.out.println("Log file will be available at " + Main.class.getClassLoader().getResource("logging.properties"));

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
        LOGGER.info("Creating the tree structure...");
        LOGGER.info("Starting with "  + Config.N_CLIENTS + " clients, " + Config.N_L1 + " caches having " +
                Config.N_L2 + " associated caches each");

        // ids
        int id = 0;

        // Create the database
        HashMap<Integer, Integer> db = initializeDatabase();
        ActorRef database = system.actorOf(Database.props(id++, db), "database");

        // Create N_L1 cache servers
        List<ActorRef> l1Caches = new ArrayList<>();
        for (int i = 0; i < Config.N_L1; i++) {
            l1Caches.add(system.actorOf(Cache.props(id++, database), "l1-cache-" + i));
        }

        // Create N_L2 cache servers
        for (int i = 0; i < l1Caches.size(); i++) {
            List<ActorRef> l2Caches = new ArrayList<>();
            for (int j = 0; j < Config.N_L2; j++) {
                // Create the L2 cache server
                l2Caches.add(system.actorOf(Cache.props(id++, database), "l2-cache-" + i + "-" + j));

                // Create N_CLIENTS that will communicate with it
                List<ActorRef> clients = new ArrayList<>();
                for (int k = 0; k < Config.N_CLIENTS; k++) {
                    clients.add(system.actorOf(Client.props(id++), "client-" + j + "-" + k));

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
        LOGGER.info("Tree structure created");
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
