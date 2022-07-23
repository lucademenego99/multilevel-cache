package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.messages.CrashMessage;
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
 * - an architecture with only 1 L1, 1 L2 and 2 clients
 *
 */
public class ECCrashBasicTest {

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

    @DisplayName("Testing the READ functionality, crash L2 before read")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testReadCrashL2BeforeRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_BEFORE_READ);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the READ functionality, crash L2 after read")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testReadCrashL2AfterRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_AFTER_READ);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the READ functionality, crash L1 before read")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testReadCrashL1BeforeRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        CrashMessage crash = new CrashMessage(Config.CrashType.L1_BEFORE_READ);
        architecture.cacheTree.database.children.get(0).actor.tell(crash, ActorRef.noSender());

        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the READ functionality, crash L1 after read")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testReadCrashL1AfterRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        CrashMessage crash = new CrashMessage(Config.CrashType.L1_AFTER_READ);
        architecture.cacheTree.database.children.get(0).actor.tell(crash, ActorRef.noSender());

        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the WRITE functionality, crash L2 before write")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testWriteCrashL2BeforeWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_BEFORE_WRITE);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the WRITE functionality, crash L2 after write")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testWriteCrashL2AfterWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_AFTER_WRITE);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the WRITE functionality, crash L1 before write")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testWriteCrashL1BeforeWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        CrashMessage crash = new CrashMessage(Config.CrashType.L1_BEFORE_WRITE);
        architecture.cacheTree.database.children.get(0).actor.tell(crash, ActorRef.noSender());

        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the WRITE functionality, crash L1 after write")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testWriteCrashL1AfterWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        CrashMessage crash = new CrashMessage(Config.CrashType.L1_AFTER_WRITE);
        architecture.cacheTree.database.children.get(0).actor.tell(crash, ActorRef.noSender());

        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a READ after a WRITE on the same key, crash L2 before READ")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testWriteAndReadCrashL2BeforeRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_BEFORE_READ);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(200);

        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The last read should return the new value of the last write
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a READ after a WRITE on the same key, crash L2 after READ")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testWriteAndReadCrashL2AfterRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_AFTER_READ);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(200);

        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The last read should return the new value of the last write
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a READ after a WRITE on the same key, crash L1 before READ")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testWriteAndReadCrashL1BeforeRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L1_BEFORE_READ);
        architecture.cacheTree.database.children.get(0).actor.tell(crash, ActorRef.noSender());

        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(200);

        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The last read should return the new value of the last write
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a READ after a WRITE on the same key, crash L1 after READ")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testWriteAndReadCrashL1AfterRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L1_AFTER_READ);
        architecture.cacheTree.database.children.get(0).actor.tell(crash, ActorRef.noSender());

        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(200);

        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The last read should return the new value of the last write
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a READ after a WRITE on the same key, crash L2 before WRITE")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testWriteAndReadCrashL2BeforeWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_BEFORE_WRITE);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(2000);

        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The last read should return the new value of the last write
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a READ after a WRITE on the same key, crash L1 before WRITE")
    @ParameterizedTest
    @ValueSource(ints = {500})
        // Milleseconds to wait
    void testWriteAndReadCrashL1BeforeWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L1_BEFORE_WRITE);
        architecture.cacheTree.database.children.get(0).actor.tell(crash, ActorRef.noSender());

        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(2000);

        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The last read should return the new value of the last write
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a Read, Write and Read on the same key, crash L2 after WRITE")
    @ParameterizedTest
    @ValueSource(ints = {2000})
        // Milleseconds to wait
    void testReadAndWriteAndReadCrashL2AfterWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_AFTER_WRITE);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        // Client 1 reads keyToAskFor
        this.architecture.clients.get(1).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        // Wait for the READ to finish
        Helper.timeout(200);

        // Client 0 performs a write updating the value of keyToAskFor
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(300);

        // Client 1 reads again keyToAskFor
        this.architecture.clients.get(1).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The second read should return the latest value
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a Read, Write and Read on the same key, crash L1 after WRITE")
    @ParameterizedTest
    @ValueSource(ints = {2000}) // Milleseconds to wait
    void testReadAndWriteAndReadCrashL1AfterWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L1_AFTER_WRITE);
        architecture.cacheTree.database.children.get(0).actor.tell(crash, ActorRef.noSender());

        // Client 1 reads keyToAskFor
        this.architecture.clients.get(1).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        // Wait for the READ to finish
        Helper.timeout(200);

        // Client 0 performs a write updating the value of keyToAskFor
        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, false), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(300);

        // Client 1 reads again keyToAskFor
        this.architecture.clients.get(1).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The second read should return the latest value
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a CRITWRITE with an L2 crash after it")
    @ParameterizedTest
    @ValueSource(ints = {2000}) // Milleseconds to wait
    void testCritwriteCrashL2AfterCritwrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_AFTER_CRIT_WRITE);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, true), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The last read should return the new value of the last write
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a READ after a CRITWRITE on the same key, crash L2 before CRITWRITE")
    @ParameterizedTest
    @ValueSource(ints = {500}) // Milleseconds to wait
    void testCritwriteAndReadCrashL2BeforeCritwrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_BEFORE_CRIT_WRITE);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, true), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(2000);

        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The last read should return the new value of the last write
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a READ after a CRITWRITE on the same key, crash L2 after CRITWRITE")
    @ParameterizedTest
    @ValueSource(ints = {2000}) // Milleseconds to wait
    void testCritwriteAndReadCrashL2AfterCritwrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_AFTER_CRIT_WRITE);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

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

    @DisplayName("Testing a READ after a CRITWRITE on the same key, crash L1 before CRITWRITE")
    @ParameterizedTest
    @ValueSource(ints = {500}) // Milleseconds to wait
    void testCritwriteAndReadCrashL1BeforeCritwrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L1_BEFORE_CRIT_WRITE);
        architecture.cacheTree.database.children.get(0).actor.tell(crash, ActorRef.noSender());

        this.architecture.clients.get(0).tell(new WriteMessage(keyToAskFor, 5, new ArrayList<>(),
                null, true), ActorRef.noSender());

        // Wait for the WRITE to finish
        Helper.timeout(2000);

        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        // The last read should return the new value of the last write
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing a READ after a CRITWRITE on the same key, crash L1 after CRITWRITE")
    @ParameterizedTest
    @ValueSource(ints = {2000}) // Milleseconds to wait
    void testCritwriteAndReadCrashL1AfterCritwrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];

        CrashMessage crash = new CrashMessage(Config.CrashType.L1_AFTER_CRIT_WRITE);
        architecture.cacheTree.database.children.get(0).actor.tell(crash, ActorRef.noSender());

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

    @DisplayName("Testing the the program with random message exchanges")
    @RepeatedTest(value = 10, name = "Repeat testMultipleRunWithoutCrash {currentRepetition} of {totalRepetition}")
    void testMultipleRunWithoutCrash() {

    }
}
