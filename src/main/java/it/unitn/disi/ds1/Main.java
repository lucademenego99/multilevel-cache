package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.actors.Cache;
import it.unitn.disi.ds1.actors.Client;
import it.unitn.disi.ds1.actors.Database;
import it.unitn.disi.ds1.messages.JoinCachesMessage;
import it.unitn.disi.ds1.messages.ReadMessage;
import it.unitn.disi.ds1.structures.Architecture;
import it.unitn.disi.ds1.structures.DistributedCacheTree;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

/***
 * Main class of the project
 */
public class Main {
    /***
     * Main function of the distributed systems project
     * @param args command line arguments
     */
    public static void main(String[] args) {
        // Create the actor system
        final ActorSystem system = ActorSystem.create("distributed-cache");

        // Initialize the Logger
        Logger.initLogger();

        // Create a database initialized with random values
        Map<Integer, Integer> database = initializeDatabase();
        database.put(21, 99);   // DEBUG

        // Set up the main architecture of the distributed cache system
        Architecture architecture = setupStructure(system, database);
        Logger.INSTANCE.info(architecture.toString());

        try {
            Thread.sleep(500);
        } catch (Exception e) {
            Logger.INSTANCE.severe(e.toString());
        }

        // Read request for key 21
        architecture.clients.get(0).tell(new ReadMessage(21, new ArrayList<>(), null), ActorRef.noSender());

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            Logger.INSTANCE.severe(e.toString());
        }

        // Write request for key 21: new value is 1
        // architecture.clients.get(0).tell(new WriteMessage(21, 1), ActorRef.noSender());

        // inputContinue();

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            Logger.INSTANCE.severe(e.toString());
        }

        // Start distributed snapshot -> the cached values for key 21 should now contain 1
        // architecture.cacheTree.database.actor.tell(new StartSnapshotMessage(), ActorRef.noSender());

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
     * @param db The database containing some initial values
     * @return A tree representing the complete architecture of the system
     */
    private static Architecture setupStructure(ActorSystem system, Map<Integer, Integer> db) {
        System.out.println("Creating tree structure...");
        Logger.INSTANCE.info("Creating the tree structure...");
        Logger.INSTANCE.info("Starting with "  + Config.N_CLIENTS + " clients, " + Config.N_L1 + " caches having " +
                Config.N_L2 + " associated caches each");

        // ids
        int id = 0;

        // Create the database
        ActorRef database = system.actorOf(Database.props(id++, db), "database");

        // Initialize a new Cache Tree
        DistributedCacheTree cacheTree = new DistributedCacheTree(database);

        // Initialize the arrays that will contain all the L1 and L2 caches
        List<ActorRef> l1Caches = new ArrayList<>();
        List<ActorRef> l2Caches = new ArrayList<>();

        // Create N_L1 cache servers
        for (int i = 0; i < Config.N_L1; i++) {
            l1Caches.add(system.actorOf(Cache.props(id++, database, database), "l1-cache-" + i));
        }
        cacheTree.database.putAll(l1Caches);

        // Create N_L2 cache servers
        for (int i = 0; i < l1Caches.size(); i++) {
            for (int j = 0; j < Config.N_L2; j++) {
                // Create the L2 cache server
                ActorRef newL2 = system.actorOf(Cache.props(id++, l1Caches.get(i), database), "l2-cache-" + i + "-" + j);
                l2Caches.add(newL2);

                cacheTree.database.children.get(i).put(newL2);
            }

            // Send to the i-th l1 cache server its children
            JoinCachesMessage l2CachesMsg = new JoinCachesMessage(l2Caches);
            l1Caches.get(i).tell(l2CachesMsg, ActorRef.noSender());
        }

        // Send to the database the list of L1 cache servers
        JoinCachesMessage l1CachesMsg = new JoinCachesMessage(l1Caches);
        database.tell(l1CachesMsg, ActorRef.noSender());

        // Create N_CLIENTS clients
        List<ActorRef> clients = new ArrayList<>();
        for (int k = 0; k < Config.N_CLIENTS; k++) {
            clients.add(system.actorOf(Client.props(id++), "client-" + k));

            // Send the L2 cache servers to the generated client
            JoinCachesMessage cachesMsg = new JoinCachesMessage(l2Caches);
            clients.get(k).tell(cachesMsg, ActorRef.noSender());
        }

        Logger.INSTANCE.info("Tree structure created");

        return new Architecture(cacheTree, clients);
    }

    /**
     * Initialize the database with random values
     * @return The initialized database as a HashMap
     */
    private static Map<Integer, Integer> initializeDatabase() {
        Map<Integer, Integer> db = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            db.put(i, (int)(Math.random() * (100)));
        }
        return db;
    }

    public static void inputContinue() {
        try {
            System.out.println(">>> Press ENTER to continue <<<");
            System.in.read();
        }
        catch (IOException ignored) {}
    }
}
