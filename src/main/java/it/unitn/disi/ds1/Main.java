package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.actors.Cache;
import it.unitn.disi.ds1.actors.Client;
import it.unitn.disi.ds1.actors.Database;
import it.unitn.disi.ds1.messages.*;
import it.unitn.disi.ds1.structures.Architecture;
import it.unitn.disi.ds1.structures.DistributedCacheTree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        // Log the architecture configuration
        Logger.logConfig(Config.N_L1, Config.N_L2, Config.N_CLIENTS);

        // Create a database initialized with random values
        Map<Integer, Integer> database = initializeDatabase();
        database.put(21, 99);   // DEBUG
        database.put(62, 88);   // DEBUG

        // Log the database for later checks
        Logger.logDatabase(database);

        // Set up the main architecture of the distributed cache system
        Architecture architecture = setupStructure(system, database);
        Logger.DEBUG.info(architecture.toString());

        try {
            Thread.sleep(500);
        } catch (Exception e) {
            Logger.DEBUG.severe(e.toString());
        }

        /**
         * Test crash L2 before read
         */
        // CrashMessage crash = new CrashMessage(Config.CrashType.L2_BEFORE_READ);
        // architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        /**
         * Test send some random read messages
         */
        /*
        for (int i = 0; i < Config.N_ITERATIONS; i++) {
            // Read request for key 21
            for (int j = 0; j < Config.N_CLIENTS; j++) {
                int requestKey = (int) database.keySet().toArray()[Config.RANDOM.nextInt(database.keySet().toArray().length)];
                if (j == 0) {
                    architecture.clients.get(j).tell(new ReadMessage(21, new ArrayList<>(), null,
                            false, -1), ActorRef.noSender());
                } else {
                    architecture.clients.get(j).tell(new ReadMessage(21, new ArrayList<>(), null,
                            false, -1), ActorRef.noSender());
                }
            }

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                Logger.DEBUG.severe(e.toString());
            }

            architecture.clients.get(0).tell(
                    new WriteMessage(21, i, new ArrayList<>(), null, false),
                    ActorRef.noSender()
            );
            try {
                Thread.sleep(2000);
            } catch (Exception e) {
                Logger.DEBUG.severe(e.toString());
            }
        }
        */


        /**
         * Test simple write
         */
        // architecture.clients.get(0).tell(new WriteMessage(21, 99, new ArrayList<>(), null, false), ActorRef.noSender());

        /**
         * Test Critical Read
         */
        // architecture.clients.get(1).tell(new ReadMessage(21, new ArrayList<>(), null, true, -1), ActorRef.noSender());
        // Sleep
        // try {
        //     Thread.sleep(3000);
        // } catch (Exception e) {
        //     Logger.INSTANCE.severe(e.toString());
        // }
        // architecture.clients.get(0).tell(new ReadMessage(21, new ArrayList<>(), null, true, -1), ActorRef.noSender());

        /**
         * Test Critical Write
         */
        // Perform a read
        // architecture.clients.get(0).tell(new ReadMessage(21, new ArrayList<>(), null, false, -1), ActorRef.noSender());
        // architecture.clients.get(1).tell(new ReadMessage(62, new ArrayList<>(), null, false, -1), ActorRef.noSender());

        // Sleep
        try {
            Thread.sleep(3000);
        } catch (Exception e) {
            Logger.DEBUG.severe(e.toString());
        }

        // Perform the critical write
        // architecture.clients.get(0).tell(new WriteMessage(21, 6, new ArrayList<>(), null, true), ActorRef.noSender());
        try {
            Thread.sleep(50);
        } catch (Exception e) {
            Logger.DEBUG.severe(e.toString());
        }
        // architecture.clients.get(3).tell(new ReadMessage(21, new ArrayList<>(), null, false, -1), ActorRef.noSender());
        // architecture.clients.get(1).tell(new WriteMessage(62, 2, new ArrayList<>(), null, true), ActorRef.noSender());

        // Start performing read requests with other clients on the same key
        // Someone should get null responses if the critical write operation hasn't completed yet
        for (int i = 0; i < Config.N_ITERATIONS; i++) {
            // Read request for key 21
            for (int j = 2; j < Config.N_CLIENTS; j++) {
                // architecture.clients.get(j).tell(new ReadMessage(21, new ArrayList<>(), null, false, -1), ActorRef.noSender());
            }
            try {
                Thread.sleep(20);
            } catch (Exception e) {
                Logger.DEBUG.severe(e.toString());
            }
        }

        // inputContinue();

        try {
            Thread.sleep(3000);
        } catch (Exception e) {
            Logger.DEBUG.severe(e.toString());
        }

        // Start distributed snapshot -> the cached values for key 21 should now contain 1
        architecture.cacheTree.database.actor.tell(new StartSnapshotMessage(), ActorRef.noSender());

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            Logger.DEBUG.severe(e.toString());
        }

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
     * @param db     The database containing some initial values
     * @return A tree representing the complete architecture of the system
     */
    private static Architecture setupStructure(ActorSystem system, Map<Integer, Integer> db) {
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
     * Initialize the database with random values
     *
     * @return The initialized database as a HashMap
     */
    private static Map<Integer, Integer> initializeDatabase() {
        Map<Integer, Integer> db = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            db.put(i, (int) (Math.random() * (100)));
        }
        return db;
    }

    public static void inputContinue() {
        try {
            System.out.println(">>> Press ENTER to continue <<<");
            System.in.read();
        } catch (IOException ignored) {
        }
    }

    /**
     * Useful TODOs
     * TODO: critwrite needs to be implemented
     * TODO:
     * - client manda crit write al database
     * - database manda a tutte le L1 crit_update con il valore aspetta un acknowledgement, manda errore a tutte le write/read/crit_write per quel valore
     *   [ fa partire un timeout, se finisce manda un abort a tutte le L1 e non fa commit del suo vecchio valore ]
     * - L1 manda crit_update alle L2 e aspetta un acknowledgement (blocca le read al valore nel crit_update) [ fa partire un timeout ]
     *   [ se finisce manda un abort al database ]
     * - L2 blocca la read del valore nella crit_update e lo tiene in una mappa, manda un acknowledgement a L1
     * - L1 aspetta tutti gli acknowledgment e manda uno al database
     * - database: appena ricevuto quello fa commit di quel valore (sappiamo che nessuno lo manderà ancora) e è tranquillo di andare
     * - database: manda commit a tutte le L1 (database si sblocca) e fa update
     * - L1 si sblocca e manda a L2 il commit e fa l'update e manda commit a tutte le L1 (database si sblocca) e fa
     *
     * In entrambi i casi:
     * - se riceve abort un processo -> non fa commit del valore nella cache
     * - se un processo va in crash, svuota la cache e esce dalla procedura,
     * nessun problema se un client chiede un valore al processo anche quando gli altri non se ne sono accorti che è in crash,
     * perché farà in caso (per il valore bloccato)  una richiesta al padre che può rispondergli errore (da implementare, on response della cache, risposta null ricevuta)
     * Mentre, se il database o il padre hanno fatto commit, allora nessun problema
     */
}
