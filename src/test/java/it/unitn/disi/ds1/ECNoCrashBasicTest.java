package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.messages.ReadMessage;
import it.unitn.disi.ds1.messages.WriteMessage;
import it.unitn.disi.ds1.structures.Architecture;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Map;

/**
 * Tests for the following setting:
 * - no crashes
 * - an architecture with only 1 L1, 1 L2 and 2 clients
 */
public class ECNoCrashBasicTest {

    private ActorSystem system;
    private Architecture architecture;
    private Map<Integer, Integer> database;

    /**
     * Basic information about the created architecture
     */
    private final int countL1 = 1, countL2 = 1, countClients = 2;

    @BeforeEach
    void resetState() {
        Helper.initializeLogger();
        this.system = Helper.createActorSystem();
        this.database = Helper.createDatabase();

        this.architecture = Helper.createArchiteture(this.system, this.database, countL1, countL2, countClients);
        // Clear the log file
        Helper.clearLogFile("logs.txt");
        // Log config
        Logger.logConfig(this.countL1, this.countL2, this.countClients);
        Logger.logDatabase(this.database);
    }

    @DisplayName("Testing the READ functionality")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the WRITE functionality")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the CRITREAD functionality")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testCritRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, true, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the CRITWRITE functionality")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testCritWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, true), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a READ after a WRITE on the same key")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testWriteAndRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(100);

        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The last read should return the new value of the last write
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a Read, Write and Read on the same key")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testReadAndWriteAndRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        // Client 1 reads keyToAskFor
        this.architecture.clients.get(1).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        // Wait for the READ to finish
        Helper.timeout(100);

        // Client 0 performs a write updating the value of keyToAskFor
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(100);

        // Client 1 reads again keyToAskFor
        this.architecture.clients.get(1).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The second read should return the latest value
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a READ after a CRITWRITE on the same key")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testCritwriteAndRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, true), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(300);

        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The last read should return the new value of the last write
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a Read, CRITWRITE and Read on the same key")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testReadAndCritwriteAndRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        // Client 1 reads keyToAskFor
        this.architecture.clients.get(1).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        // Wait for the READ to finish
        Helper.timeout(100);

        // Client 0 performs a write updating the value of keyToAskFor
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, true), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(300);

        // Client 1 reads again keyToAskFor
        this.architecture.clients.get(1).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The second read should return the latest value
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a Read, Write and CRITREAD on the same key")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testReadAndWriteAndCritRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        // Client 0 reads keyToAskFor
        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        // Wait for the READ to finish
        Helper.timeout(100);

        // Client 0 performs a write updating the value of keyToAskFor
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(300);

        // Client 0 performs a CRITREAD again keyToAskFor
        this.architecture.clients.get(1).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, true, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The second CRITREAD should return the latest value
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a Read right after a CRITWRITE")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testReadRightAfterCritwrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        // Client 0 performs a write updating the value of keyToAskFor
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, true), ActorRef.noSender());

        Helper.timeout(20);

        // Client 1 reads keyToAskFor
        this.architecture.clients.get(1).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The read will return an error
        assertTrue(Checker.check(), "Not consistent");
    }
}
