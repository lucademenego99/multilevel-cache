package it.unitn.disi.ds1;

import akka.actor.ActorSystem;
import it.unitn.disi.ds1.structures.Architecture;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Scanner;

/***
 * Main class of the project
 */
public class Main {
    /***
     * Main function of the distributed systems project
     * @param args command line arguments
     */
    public static void main(String[] args) {
        /**
         * Defaults
         */
        int countL1 = 5;
        int countL2 = 5;
        int countClients = 3;
        int secondsForIteration = 20;

        /**
         * Command line parser and helper
         */
        CommandLineParser cmdLineParser = new DefaultParser();
        HelpFormatter helper = new HelpFormatter();

        Options options = new Options();
        options.addOption(Option.builder().
                longOpt("l1")
                .argName("Number of l1 caches")
                .hasArg(true)
                .desc("Number of L1 caches which will be present in the hierarchical distributed cache")
                .type(Number.class)
                .build()
        );

        options.addOption(Option.builder().
                longOpt("l2")
                .argName("Number of l2 caches")
                .hasArg(true)
                .desc("Number of L2 caches which will be present in the hierarchical distributed cache")
                .type(Number.class)
                .build()
        );

        options.addOption(Option.builder().
                longOpt("clients")
                .argName("Number of clients")
                .hasArg(true)
                .desc("Number of clients connected to the hierarchical distributed cache")
                .type(Number.class)
                .build()
        );

        options.addOption(Option.builder().
                longOpt("seconds")
                .argName("Number of seconds per iteration")
                .hasArg(true)
                .desc("Number of seconds each iteration takes")
                .type(Number.class)
                .build()
        );

        /**
         * Parse the arguments
         */
        try {
            CommandLine cmdLine = cmdLineParser.parse(options, args);

            // Help command
            if (cmdLine.hasOption('h') || cmdLine.hasOption("help")) {
                helper.printHelp("Multilevel cache", options, true);
                System.exit(0);
            }

            if (cmdLine.hasOption("l1") && ((Number) cmdLine.getParsedOptionValue("l1")).intValue() > 0) {
                countL1 = ((Number) cmdLine.getParsedOptionValue("l1")).intValue();
            } else {
                System.out.println("l1 argument not found or invalid, using default: " + countL1);
            }

            if (cmdLine.hasOption("l2") && ((Number) cmdLine.getParsedOptionValue("l2")).intValue() > 0) {
                countL2 = ((Number) cmdLine.getParsedOptionValue("l2")).intValue();
            } else {
                System.out.println("l2 argument not found or invalid, using default: " + countL2);
            }

            if (cmdLine.hasOption("clients") && ((Number) cmdLine.getParsedOptionValue("clients")).intValue() > 0) {
                countClients = ((Number) cmdLine.getParsedOptionValue("clients")).intValue();
            } else {
                System.out.println("clients argument not found or invalid, using default: " + countClients);
            }

            if (cmdLine.hasOption("seconds") && ((Number) cmdLine.getParsedOptionValue("seconds")).intValue() > 0) {
                secondsForIteration = ((Number) cmdLine.getParsedOptionValue("seconds")).intValue();
            } else {
                System.out.println("seconds argument not found or invalid, using default: " + secondsForIteration);
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            helper.printHelp("Usage:", options);
            System.exit(0);
        }

        /**
         * Initialize the logger
         */
        Utils.initializeLogger();

        /**
         * Setup actor system, database and architecture
         */
        ActorSystem system = Utils.createActorSystem();
        Map<Integer, Integer> database = Utils.createDatabase();
        Architecture architecture = Utils.createArchiteture(system, database, countL1, countL2, countClients);

        /**
         * Setup log file
         */
        Logger.logConfig(countL1, countL2, countClients);
        Logger.logDatabase(database);

        /* Log the architecture */
        Logger.DEBUG.info(architecture.toString());

        /**
         * Main
         */
        float crashProbability = (float) 0.05;
        int maxTimeToWait = 300;
        int minTimeToWait = 100;
        int timePassedInSeconds = 0;
        boolean keepLooping = true;
        boolean repeat;

        LocalDateTime then = LocalDateTime.now();

        do {
            // Iterate for secondsForIteration seconds and do random actions
            while (keepLooping) {
                // Random message
                Utils.randomAction(system, architecture, database,
                        minTimeToWait, maxTimeToWait, crashProbability);
                // Wait for something to finish
                Utils.timeout(maxTimeToWait);
                if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= secondsForIteration) {
                    keepLooping = false;
                }
            }

            // Update the current execution time
            timePassedInSeconds += secondsForIteration;

            // Small timeout
            Utils.timeout(maxTimeToWait * 10);

            // Ask for repetition
            repeat = askToContinue(timePassedInSeconds);

            // Reset loop
            then = LocalDateTime.now();
            keepLooping = true;
        } while (repeat);

        // Consistency check
        boolean consistent = Checker.check();
        if (consistent) {
            System.out.println("The system is in a consistent state [eventual consistency]");
        } else {
            System.out.println("The system is NOT in a consistent state [eventual consistency]");
        }

        // Shutdown system
        system.terminate();
    }

    /**
     * Method which asks the user to keep running the distributed cache
     *
     * @param time second passed so far
     * @return boolean which states whether to continue or not
     */
    private static boolean askToContinue(Integer time) {
        Scanner scan = new Scanner(System.in);
        boolean answer = false;
        boolean continuing = false;

        System.out.println(time + " seconds has passed, do you want to continue? [y/yes - n/no]");

        do {
            try{
                // Acquire answer
                String input = scan.nextLine().trim().toLowerCase();

                // Check the answer
                if (input.equals("yes") || input.equals("y")) {
                    continuing = true;
                    answer = true;
                } else if (input.equals("no") || input.equals("n")) {
                    continuing = false;
                    answer = true;
                } else {
                    // Re-ask
                    System.out.println("Sorry, I could not understand what you have typed, do you want to continue? [y/yes - n/no]");
                }
            } catch(Exception e){
                System.out.println("Sorry, there was a misunderstanding due to Scanner error, do you want to continue? [y/yes - n/no]");
            }

        } while (!answer);

        return continuing;
    }
}
