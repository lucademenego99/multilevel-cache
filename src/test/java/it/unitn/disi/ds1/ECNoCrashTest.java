package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.messages.ReadMessage;
import it.unitn.disi.ds1.messages.WriteMessage;
import it.unitn.disi.ds1.structures.Architecture;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the following setting:
 * - no crashes
 * - an architecture with 3 L1s, 3 L2s and 5 clients
 */
public class ECNoCrashTest {

    private ActorSystem system;
    private Architecture architecture;
    private Map<Integer, Integer> database;

    /**
     * Basic information about the created architecture
     */
    private final int countL1 = 3, countL2 = 3, countClients = 5;

    /**
     * Number of iterations each test should have
     */
    private final int numberOfIterations = 50;

    @BeforeEach
    void resetState() {
        this.system = Helper.createActorSystem();
        this.database = Helper.createDatabase();

        this.architecture = Helper.createArchiteture(this.system, this.database, countL1, countL2, countClients);
        // Clear the log file
        Helper.clearLogFile("logs.txt");
        // Log config
        Logger.logConfig(countL1, countL2, countClients);
        Logger.logDatabase(this.database);
    }

    @DisplayName("Testing various READs and WRITEs")
    @ParameterizedTest
    @ValueSource(ints = {3000})
        // Milleseconds to wait
    void testVariousReadsAndWrites(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        // Perform this.numberOfIterations iterations
        for (int i = 0; i < this.numberOfIterations; i++) {

            // Loop through every client
            for (int j = 0; j < this.countClients; j++) {

                // Read request for a random key
                int requestKey = (int) database.keySet().toArray()[Config.RANDOM.nextInt(database.keySet().toArray().length)];
                architecture.clients.get(j).tell(new ReadMessage(requestKey, new ArrayList<>(), null,
                        false, -1), ActorRef.noSender());
            }

            // Wait for the reads to finish
            Helper.timeout(500);

            // Perform a write with a random client, on a random key
            int requestKey = (int) database.keySet().toArray()[Config.RANDOM.nextInt(database.keySet().toArray().length)];
            int randomClient = Config.RANDOM.nextInt(this.countClients - 1);
            architecture.clients.get(randomClient).tell(
                    new WriteMessage(requestKey, i, new ArrayList<>(), null, false),
                    ActorRef.noSender()
            );

            // Wait for the write to finish
            Helper.timeout(200);
        }

        // Wait for everything to finish
        Helper.timeout(timeToWait);

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing various READs and CRITWRITESs")
    @ParameterizedTest
    @ValueSource(ints = {3000})
        // Milleseconds to wait
    void testVariousReadsAndCritwrites(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        // Perform this.numberOfIterations iterations
        for (int i = 0; i < this.numberOfIterations; i++) {

            // Loop through every client
            for (int j = 0; j < this.countClients; j++) {

                // Read request for a random key
                int requestKey = (int) database.keySet().toArray()[Config.RANDOM.nextInt(database.keySet().toArray().length)];
                architecture.clients.get(j).tell(new ReadMessage(requestKey, new ArrayList<>(), null,
                        false, -1), ActorRef.noSender());
            }

            // Wait for the reads to finish
            Helper.timeout(500);

            // Perform a write with a random client, on a random key
            int requestKey = (int) database.keySet().toArray()[Config.RANDOM.nextInt(database.keySet().toArray().length)];
            int randomClient = Config.RANDOM.nextInt(this.countClients - 1);
            architecture.clients.get(randomClient).tell(
                    new WriteMessage(requestKey, i, new ArrayList<>(), null, true),
                    ActorRef.noSender()
            );

            // Wait for the write to finish
            Helper.timeout(200);
        }

        // Wait for everything to finish
        Helper.timeout(timeToWait);

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing various CRITREADs and WRITESs")
    @ParameterizedTest
    @ValueSource(ints = {3000})
        // Milleseconds to wait
    void testVariousCritreadsAndWrites(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        // Perform this.numberOfIterations iterations
        for (int i = 0; i < this.numberOfIterations; i++) {

            // Loop through every client
            for (int j = 0; j < this.countClients; j++) {

                // Read request for a random key
                int requestKey = (int) database.keySet().toArray()[Config.RANDOM.nextInt(database.keySet().toArray().length)];
                architecture.clients.get(j).tell(new ReadMessage(requestKey, new ArrayList<>(), null,
                        true, -1), ActorRef.noSender());
            }

            // Wait for the reads to finish
            Helper.timeout(500);

            // Perform a write with a random client, on a random key
            int requestKey = (int) database.keySet().toArray()[Config.RANDOM.nextInt(database.keySet().toArray().length)];
            int randomClient = Config.RANDOM.nextInt(this.countClients - 1);
            architecture.clients.get(randomClient).tell(
                    new WriteMessage(requestKey, i, new ArrayList<>(), null, false),
                    ActorRef.noSender()
            );

            // Wait for the write to finish
            Helper.timeout(200);
        }

        // Wait for everything to finish
        Helper.timeout(timeToWait);

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing various CRITREADs and CRITWRITESs")
    @ParameterizedTest
    @ValueSource(ints = {3000})
        // Milleseconds to wait
    void testVariousOperations(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        // Perform this.numberOfIterations iterations
        for (int i = 0; i < this.numberOfIterations; i++) {

            // Loop through every client
            for (int j = 0; j < this.countClients; j++) {

                // Read request (or CRITREAD) for a random key
                int requestKey = (int) database.keySet().toArray()[Config.RANDOM.nextInt(database.keySet().toArray().length)];
                boolean isCritical = Config.RANDOM.nextBoolean();
                architecture.clients.get(j).tell(new ReadMessage(requestKey, new ArrayList<>(), null,
                        isCritical, -1), ActorRef.noSender());
            }

            // Wait for the reads to finish
            Helper.timeout(500);

            // Perform a write (or CRITWRITE) with a random client, on a random key
            int requestKey = (int) database.keySet().toArray()[Config.RANDOM.nextInt(database.keySet().toArray().length)];
            int randomClient = Config.RANDOM.nextInt(this.countClients - 1);
            boolean isCritical = Config.RANDOM.nextBoolean();
            architecture.clients.get(randomClient).tell(
                    new WriteMessage(requestKey, i, new ArrayList<>(), null, isCritical),
                    ActorRef.noSender()
            );

            // Wait for the write to finish
            Helper.timeout(200);
        }

        // Wait for everything to finish
        Helper.timeout(timeToWait);

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the the program with random message exchanges")
    @RepeatedTest(value = 10, name = "Repeat testMultipleRunWithoutCrash {currentRepetition} of {totalRepetition}")
    void testMultipleRunWithoutCrash() {
        // TODO
    }
}
