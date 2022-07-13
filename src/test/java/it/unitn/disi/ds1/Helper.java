package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.actors.Cache;
import it.unitn.disi.ds1.actors.Client;
import it.unitn.disi.ds1.actors.Database;
import it.unitn.disi.ds1.messages.JoinCachesMessage;
import it.unitn.disi.ds1.structures.Architecture;
import it.unitn.disi.ds1.structures.DistributedCacheTree;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test helper class
 * It implements some functions which are useful for testing
 */
public class Helper {

    /**
     * Creates the actor system
     * @return Akka Actor System
     */
    public static ActorSystem createActorSystem(){
        // Create the actor system
        final ActorSystem system = ActorSystem.create("distributed-cache");
        return system;
    }

    /**
     * Initialize the logger
     */
    public static void initializeLogger(){
        // Initialize the Logger
        Logger.initLogger();
    }

    /**
     * Initialize the database with random values (Integer, Integer)
     * @return The initialized database
     */
    public static Map<Integer, Integer> createDatabase(){
        Map<Integer, Integer> db = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            db.put(i, (int) (Math.random() * (100)));
        }
        return db;
    }

    /**
     * Prepare the architecture for the tests
     * @param system Akka Actor System
     * @param db Database of key-value pairs (Integer, Integer)
     * @return A tree representing the entire Actors architecture
     */
    public static Architecture createArchiteture(ActorSystem system, Map<Integer, Integer> db){
        System.out.println("Creating tree structure...");
        Logger.DEBUG.info("Creating the tree structure...");
        Logger.DEBUG.info("Starting with " + Config.N_CLIENTS + " clients, " + Config.N_L1 + " caches having " +
                Config.N_L2 + " associated caches each");

        // ids
        int id = -1;

        // Create the database
        ActorRef database = system.actorOf(Database.props(++id, db), "database-" + id);

        // Initialize a new Cache Tree
        DistributedCacheTree cacheTree = new DistributedCacheTree(database);

        // Initialize the arrays that will contain all the L1 and L2 caches
        List<ActorRef> l1Caches = new ArrayList<>();
        List<ActorRef> l2Caches = new ArrayList<>();

        // Create N_L1 cache servers
        for (int i = 0; i < Config.N_L1; i++) {
            l1Caches.add(system.actorOf(Cache.props(++id, database, database), "l1-cache-" + i + "-" + id));
        }
        cacheTree.database.putAll(l1Caches);

        // Create N_L2 cache servers
        for (int i = 0; i < l1Caches.size(); i++) {
            List<ActorRef> l2CachesTmp = new ArrayList<>();
            for (int j = 0; j < Config.N_L2; j++) {
                // Create the L2 cache server
                ActorRef newL2 = system.actorOf(Cache.props(++id, l1Caches.get(i), database),
                        "l2-cache-" + i + "-" + j + "-" + id);
                l2CachesTmp.add(newL2);
                cacheTree.database.children.get(i).put(newL2);
            }

            // Send to the i-th l1 cache server its children
            JoinCachesMessage l2CachesMsg = new JoinCachesMessage(l2CachesTmp);
            l1Caches.get(i).tell(l2CachesMsg, ActorRef.noSender());

            l2Caches.addAll(l2CachesTmp);
        }

        // Send to the database the list of L1 cache servers
        JoinCachesMessage l1CachesMsg = new JoinCachesMessage(l1Caches);
        database.tell(l1CachesMsg, ActorRef.noSender());

        // Create N_CLIENTS clients
        List<ActorRef> clients = new ArrayList<>();
        for (int k = 0; k < Config.N_CLIENTS; k++) {
            clients.add(system.actorOf(Client.props(++id), "client-" + k + "-" + id));

            // Send the L2 cache servers to the generated client
            JoinCachesMessage cachesMsg = new JoinCachesMessage(l2Caches);
            clients.get(k).tell(cachesMsg, ActorRef.noSender());
        }

        Logger.DEBUG.info("Tree structure created");

        return new Architecture(cacheTree, clients);
    }

    /**
     * Clears the file at filename
     * @param filename
     */
    public static void clearLogFile(String filename){
        try{
            // Reset the log file
            File file = new File(filename);
            PrintWriter writer = new PrintWriter(file);
            writer.print("");
            writer.close();
        }catch(Exception exception){
            exception.printStackTrace();
        }
    }

    public static void timeout(Integer milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (Exception e) {
            Logger.DEBUG.severe(e.toString());
        }
    }
}
