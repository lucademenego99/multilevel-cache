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

public class ECNoCrashTest {

    private ActorSystem system;
    private Architecture architecture;
    private Map<Integer, Integer> database;

    @BeforeAll
    static void beforeAll() {
        Helper.initializeLogger();
    }

    @BeforeEach
    void resetState() {
        this.system = Helper.createActorSystem();
        this.database = Helper.createDatabase();
        this.architecture = Helper.createArchiteture(this.system, this.database);
        // Clear the log file
        Helper.clearLogFile("logs.txt");
        // Log config
        Logger.logConfig(Config.N_L1, Config.N_L2, Config.N_CLIENTS);
        Logger.logDatabase(this.database);
    }

    @DisplayName("Testing the READ functionality")
    @ParameterizedTest
    @ValueSource(ints = { 500, 200}) // Milleseconds to wait
    void testRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(), null, false, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the WRITE functionality")
    @ParameterizedTest
    @ValueSource(ints = { 500 }) // Milleseconds to wait
    void testWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(
                new WriteMessage(keyToAskFor, 5, new ArrayList<>(), null, false),
                ActorRef.noSender()
        );

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the CRITREAD functionality")
    @ParameterizedTest
    @ValueSource(ints = { 500 }) // Milleseconds to wait
    void testCritRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(), null, true, -1), ActorRef.noSender());

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the CRITWRITE functionality")
    @ParameterizedTest
    @ValueSource(ints = { 500 }) // Milleseconds to wait
    void testCritWrite(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");
        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(
                new WriteMessage(keyToAskFor, 5, new ArrayList<>(), null, true),
                ActorRef.noSender()
        );

        Helper.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @Test
    @DisplayName("Testing the the program with random message exchange")
    @RepeatedTest(value = 10, name = "Repeat testMultipleRunWithoutCrash {currentRepetition} of {totalRepetition}")
    void testMultipleRunWithoutCrash() {

    }
}
