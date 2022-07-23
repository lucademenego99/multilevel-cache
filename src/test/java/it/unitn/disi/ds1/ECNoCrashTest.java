package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.messages.ReadMessage;
import it.unitn.disi.ds1.messages.WriteMessage;
import it.unitn.disi.ds1.structures.Architecture;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Stream;

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
    @ValueSource(ints = {3000}) // Milleseconds to wait
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
    @ValueSource(ints = {3000}) // Milleseconds to wait
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
    @ValueSource(ints = {3000}) // Milleseconds to wait
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
    @ValueSource(ints = {3000}) // Milleseconds to wait
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

    @DisplayName("Testing the the program with random message exchanges without crashes for 5 times")
    @RepeatedTest(value = 5, name = "Repeat testMultipleRunWithoutCrash {currentRepetition} of {totalRepetition}")
    void testMultipleRunWithoutCrash() {
        assertTrue(this.database.size() > 0, "Database not initialized");

        int maxTimeToWait = 300;
        int minTimeToWait = 100;

        // Perform this.numberOfIterations iterations
        for (int i = 0; i < this.numberOfIterations; i++) {
            // Random message
            Helper.randomMessage(this.system, this.architecture, this.database, minTimeToWait, maxTimeToWait);
            // Wait for something to finish
            Helper.timeout((minTimeToWait + maxTimeToWait)/2);
        }

        // Wait for everything to finish
        Helper.timeout(this.numberOfIterations * maxTimeToWait);

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }

    // TODO move this
    @DisplayName("Testing the the program with random message exchanges with crashes for 5 times")
    @RepeatedTest(value = 5, name = "Repeat testMultipleRunWithoutCrash {currentRepetition} of {totalRepetition}")
    void testMultipleRunWithCrash() {
        assertTrue(this.database.size() > 0, "Database not initialized");

        int maxTimeToWait = 300;
        int minTimeToWait = 100;
        float crashProbability = (float) 0.05;

        // Perform this.numberOfIterations iterations
        for (int i = 0; i < this.numberOfIterations; i++) {
            // Random message
            Helper.randomAction(this.system, this.architecture, this.database,
                    minTimeToWait, maxTimeToWait, crashProbability);
            // Wait for something to finish
            Helper.timeout((minTimeToWait + maxTimeToWait)/2);
        }

        // Wait for everything to finish
        Helper.timeout(this.numberOfIterations * maxTimeToWait);

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }

    /**
     * Just a function which provides some parameters, namely:
     *
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

    // TODO move this
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
            Helper.randomMessage(this.system, this.architecture, this.database,
                    minTimeToWait, maxTimeToWait);
            // Wait for something to finish
            Helper.timeout(maxTimeToWait);
            if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= durationSeconds) break;
        }

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }

    // TODO move this
    @DisplayName("Testing the the program with random message exchanges with crashes for some seconds")
    @ParameterizedTest
    @MethodSource("provideParameters")
    void testRandomActionForPredefinitetTimeWithCrash(int minTimeToWait, int maxTimeToWait, int durationSeconds) {
        assertTrue(this.database.size() > 0, "Database not initialized");

        float crashProbability = (float) 0.05;

        LocalDateTime then = LocalDateTime.now();

        // Perform this until enter is pressed
        while (true) {
            // Random message
            Helper.randomAction(this.system, this.architecture, this.database,
                    minTimeToWait, maxTimeToWait, crashProbability);
            // Wait for something to finish
            Helper.timeout(maxTimeToWait);
            if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= durationSeconds) break;
        }

        // The run should be consistent
        assertTrue(Checker.check(), "Not consistent");
    }

    // Example of a possible MAIN
    // TODO probably put the Helper's new functions in a class which is easily accessible by the main
    void sampleMain(){
        float crashProbability = (float) 0.05;

        int maxTimeToWait = 300;
        int minTimeToWait = 100;
        int secondsForIteration = 20;
        int timePassedInSeconds = 0;
        boolean keepLooping = true;
        boolean repeat;

        LocalDateTime then = LocalDateTime.now();

        do{
            // Iterate for secondsForIteration seconds and do random actions
            while (keepLooping) {
                // Random message
                Helper.randomAction(this.system, this.architecture, this.database,
                        minTimeToWait, maxTimeToWait, crashProbability);
                // Wait for something to finish
                Helper.timeout(maxTimeToWait);
                if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= secondsForIteration) {
                    keepLooping = false;
                }
            }

            // Update the current execution time
            timePassedInSeconds += secondsForIteration;

            // Ask for repetition
            repeat = askToContinue(timePassedInSeconds);
        }while(repeat);

        assertTrue(true, "Just an idea of a main");
    }

    private boolean askToContinue(Integer time){
        Scanner scan = new Scanner(System.in);
        boolean answer = false;
        boolean continuing = false;

        System.out.println(time + " seconds has passed, do you want to continue? [y/yes - n/no]");

        do {
            // Acquire answer
            String input = scan.nextLine().trim().toLowerCase();

            // Check the answer
            if (input.equals("yes") || input.equals("y")) {
                continuing = true;
                answer = true;
            } else if(input.equals("no") || input.equals("n")) {
                continuing = false;
                answer = true;
            } else {
                // Re-ask
                System.out.println("Sorry, I could not understand what you have typed, do you want to continue? [y/yes - n/no]");
            }
        } while(!answer);

        return continuing;
    }
}
