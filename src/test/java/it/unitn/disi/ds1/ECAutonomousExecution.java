package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.messages.CrashMessage;
import it.unitn.disi.ds1.messages.ReadMessage;
import it.unitn.disi.ds1.structures.Architecture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the following setting:
 * - crashes and no crashes
 * - random actions/messages exchange
 */
public class ECAutonomousExecution {
    /**
     * Basic information about the created architecture
     */
    private final int countL1 = 3, countL2 = 3, countClients = 5;
    /**
     * Number of iterations each test should have
     */
    private final int numberOfIterations = 50;
    private ActorSystem system;
    private Architecture architecture;
    private Map<Integer, Integer> database;

    /**
     * Just a function which provides some parameters, namely:
     * <p>
     * - min time to wait in current time millis
     * - max time to wait in current time millis
     * - seconds to run the program
     *
     * @return stream of arguments
     */
    private static Stream<Arguments> provideParameters() {
        return Stream.of(
                Arguments.of(100, 300, 20)
        );
    }

    @BeforeEach
    void resetState() {
        Utils.initializeLogger();
        this.system = Utils.createActorSystem();
        this.database = Utils.createDatabase();
        this.architecture = Utils.createArchiteture(this.system, this.database, countL1, countL2, countClients);
        // Clear the log file
        Helper.clearLogFile("logs.txt");
        // Log config
        Logger.logConfig(this.countL1, this.countL2, this.countClients);
        Logger.logDatabase(this.database);
    }

    @DisplayName("Testing the READ functionality, crash L2 before read")
    @ParameterizedTest
    @ValueSource(ints = {2000}) // Milleseconds to wait
    void testReadCrashL2BeforeRead(int timeToWait) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        CrashMessage crash = new CrashMessage(Config.CrashType.L2_BEFORE_READ);
        architecture.cacheTree.database.children.get(0).children.get(0).actor.tell(crash, ActorRef.noSender());

        int keyToAskFor = (int) this.database.keySet().toArray()[0];
        this.architecture.clients.get(0).tell(new ReadMessage(keyToAskFor, new ArrayList<>(),
                null, false, -1), ActorRef.noSender());

        Utils.timeout(timeToWait);

        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the the program with random message exchanges without crashes for 5 times")
    @RepeatedTest(value = 5, name = "Repeat testMultipleRunWithoutCrash {currentRepetition} of {totalRepetitions}")
    void testMultipleRunWithoutCrash() {
        assertTrue(this.database.size() > 0, "Database not initialized");

        int maxTimeToWait = 300;
        int minTimeToWait = 100;

        // Perform this.numberOfIterations iterations
        for (int i = 0; i < this.numberOfIterations; i++) {
            // Random message
            Utils.randomMessage(this.system, this.architecture, this.database, minTimeToWait, maxTimeToWait);
            // Wait for something to finish
            Utils.timeout((minTimeToWait + maxTimeToWait) / 2);
        }

        // Wait for everything to finish
        Utils.timeout(this.numberOfIterations * maxTimeToWait);

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the the program with random message exchanges with crashes for 5 times")
    @RepeatedTest(value = 5, name = "Repeat testMultipleRunWithoutCrash {currentRepetition} of {totalRepetitions}")
    void testMultipleRunWithCrash() {
        assertTrue(this.database.size() > 0, "Database not initialized");

        int maxTimeToWait = 300;
        int minTimeToWait = 100;
        float crashProbability = (float) 0.05;

        // Perform this.numberOfIterations iterations
        for (int i = 0; i < this.numberOfIterations; i++) {
            // Random message
            Utils.randomAction(this.system, this.architecture, this.database,
                    minTimeToWait, maxTimeToWait, crashProbability);
            // Wait for something to finish
            Utils.timeout((minTimeToWait + maxTimeToWait) / 2);
        }

        // Wait for everything to finish
        Utils.timeout(this.numberOfIterations * maxTimeToWait);

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the the program with random message exchanges without crashes for some seconds")
    @ParameterizedTest
    @MethodSource("provideParameters")
    void testRandomActionForPredefinitetTimeNoCrash(int minTimeToWait, int maxTimeToWait, int durationSeconds) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        float crashProbability = (float) 0.05;

        LocalDateTime then = LocalDateTime.now();

        // Perform this until enter is pressed
        while (true) {
            // Random message
            Utils.randomMessage(this.system, this.architecture, this.database,
                    minTimeToWait, maxTimeToWait);
            // Wait for something to finish
            Utils.timeout(maxTimeToWait);
            if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= durationSeconds) break;
        }

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }

    @DisplayName("Testing the the program with random message exchanges with crashes for some seconds")
    @ParameterizedTest
    @MethodSource("provideParameters")
    void testRandomActionForPredefinedTimeWithCrash(int minTimeToWait, int maxTimeToWait, int durationSeconds) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        float crashProbability = (float) 0.05;

        LocalDateTime then = LocalDateTime.now();

        // Perform this until enter is pressed
        while (true) {
            // Random message
            Utils.randomAction(this.system, this.architecture, this.database,
                    minTimeToWait, maxTimeToWait, crashProbability);
            // Wait for something to finish
            Utils.timeout(maxTimeToWait);
            if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= durationSeconds) break;
        }

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }
}
