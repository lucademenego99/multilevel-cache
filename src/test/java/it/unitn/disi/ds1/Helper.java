package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.actors.Cache;
import it.unitn.disi.ds1.actors.Client;
import it.unitn.disi.ds1.actors.Database;
import it.unitn.disi.ds1.messages.JoinCachesMessage;
import it.unitn.disi.ds1.messages.Message;
import it.unitn.disi.ds1.structures.Architecture;
import it.unitn.disi.ds1.structures.DistributedCacheTree;
import scala.concurrent.duration.Duration;

import java.io.PrintWriter;
import java.util.Random;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
    public static Architecture createArchiteture(
            ActorSystem system,
            Map<Integer, Integer> db,
            Integer countL1,
            Integer countL2,
            Integer countClients
    ){
        System.out.println("Creating tree structure...");
        Logger.DEBUG.info("Creating the tree structure...");
        Logger.DEBUG.info("Starting with " + countClients + " clients, " + countL1 + " caches having " +
                countL2 + " associated caches each");

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
        for (int i = 0; i < countL1; i++) {
            l1Caches.add(system.actorOf(Cache.props(++id, database, database), "l1-cache-" + i + "-" + id));
        }
        cacheTree.database.putAll(l1Caches);

        // Create N_L2 cache servers
        for (int i = 0; i < l1Caches.size(); i++) {
            List<ActorRef> l2CachesTmp = new ArrayList<>();
            for (int j = 0; j < countL2; j++) {
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
        for (int k = 0; k < countClients; k++) {
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
            new PrintWriter(filename).close();
        }catch(Exception exception){
            exception.printStackTrace();
        }
    }

    /**
     * Timeout
     * @param milliseconds number of milliseconds
     */
    public static void timeout(Integer milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (Exception e) {
            Logger.DEBUG.severe(e.toString());
        }
    }

    /**
     * Returns a pseudo-random number between min and max, inclusive.
     * The difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min Minimum value
     * @param max Maximum value.  Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see java.util.Random#nextInt(int)
     */
    public static int randInt(int min, int max) {
        final Random rand = new Random();
        return rand.nextInt((max - min) + 1) + min;
    }

    /**
     * Random action to take
     * TODO, to finish
     * @param upperBoundMilliseconds
     */
    public static void randomAction(Integer upperBoundMilliseconds, Float crashProbability){
        int crashOrRequest = randInt(0, 100);
        // Basically schedule the messages
        if(crashOrRequest/100.0 < crashProbability){
            // Crash
            int randomCrash = randInt(1, Config.CrashType.values().length - 1); // All crashe types but the none
            Config.CrashType crash = Config.CrashType.values()[randomCrash];
            switch (crash){
                case L1_AFTER_CRIT_READ:
                    break;
                case L1_AFTER_CRIT_WRITE:
                    break;
                case L1_AFTER_READ:
                    break;
                case L1_AFTER_RESPONSE:
                    break;
                case L1_AFTER_WRITE:
                    break;
                case L1_BEFORE_CRIT_READ:
                    break;
                case L1_BEFORE_CRIT_WRITE:
                    break;
                case L1_BEFORE_READ:
                    break;
                case L1_BEFORE_RESPONSE:
                    break;
                case L1_BEFORE_WRITE:
                    break;
                case L2_AFTER_CRIT_READ:
                    break;
                case L2_AFTER_CRIT_WRITE:
                    break;
                case L2_AFTER_READ:
                    break;
                case L2_AFTER_RESPONSE:
                    break;
                case L2_AFTER_WRITE:
                    break;
                case L2_BEFORE_CRIT_READ:
                    break;
                case L2_BEFORE_CRIT_WRITE:
                    break;
                case L2_BEFORE_READ:
                    break;
                case L2_BEFORE_RESPONSE:
                    break;
                case L2_BEFORE_WRITE:
                    break;
            }
        }else{
            // Message
            int randomMessage = randInt(0, Config.RequestType.values().length - 2); // All crash types but the flush
            Config.RequestType message = Config.RequestType.values()[randomMessage];
            switch (message){
                case READ:
                    break;
                case WRITE:
                    break;
                case CRITREAD:
                    break;
                case CRITWRITE:
                    break;
            }
        }
    }

    /**
     * Schedules a message in an actor system to a recipient
     * @param system actor system
     * @param recipient recipient reference
     * @param msg message to send
     * @param timeoutMillis after how much time to send
     */
    public static void scheduleMessage(ActorSystem system, ActorRef recipient, Message msg, Integer timeoutMillis) {
        system.scheduler().scheduleOnce(Duration.create(timeoutMillis, TimeUnit.MILLISECONDS),
                recipient,                                                    // destination actor reference
                msg,                                                          // Timeout message
                system.dispatcher(),                                          // system dispatcher
                recipient                                                     // source of the message (myself)
        );
    }
}
