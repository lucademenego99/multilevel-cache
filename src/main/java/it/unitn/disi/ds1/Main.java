package it.unitn.disi.ds1;

import akka.actor.ActorSystem;
import it.unitn.disi.ds1.structures.Architecture;
import org.apache.commons.cli.*;

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
         * Command line parser
         */
        CommandLineParser cmdLineParser = new DefaultParser();

        Options options = new Options();
        options.addOption(Option.builder().
                longOpt("l1")
                .argName("Number of l1 caches")
                .hasArg(true)
                .desc("Number of L1 caches which will be present in the hierarchical distributed cache")
                .type(Integer.class)
                .build()
        );

        options.addOption(Option.builder().
                longOpt("l2")
                .argName("Number of l2 caches")
                .hasArg(true)
                .desc("Number of L2 caches which will be present in the hierarchical distributed cache")
                .type(Integer.class)
                .build()
        );

        options.addOption(Option.builder().
                longOpt("clients")
                .argName("Number of clients")
                .hasArg(true)
                .desc("Number of clients connected to the hierarchical distributed cache")
                .type(Integer.class)
                .build()
        );

        options.addOption(Option.builder().
                longOpt("seconds")
                .argName("Number of seconds per iteration")
                .hasArg(true)
                .desc("Number of seconds each iteration takes")
                .type(Integer.class)
                .build()
        );

        /**
         * Parse the arguments
         */
        try {
            CommandLine cmdLine = cmdLineParser.parse(options, args);

            if (cmdLine.hasOption("l1") && (Integer) cmdLine.getParsedOptionValue("l1") > 0) {
                countL1 = (Integer) cmdLine.getParsedOptionValue("l1");
            } else {
                System.out.println("l1 argument not found or invalid, using default: " + countL1);
            }

            if (cmdLine.hasOption("l2") && (Integer) cmdLine.getParsedOptionValue("l2") > 0) {
                countL2 = (Integer) cmdLine.getParsedOptionValue("l2");
            } else {
                System.out.println("l2 argument not found or invalid, using default: " + countL2);
            }

            if (cmdLine.hasOption("clients") && (Integer) cmdLine.getParsedOptionValue("clients") > 0) {
                countClients = (Integer) cmdLine.getParsedOptionValue("clients");
            } else {
                System.out.println("clients argument not found or invalid, using default: " + countClients);
            }

            if (cmdLine.hasOption("seconds") && (Integer) cmdLine.getParsedOptionValue("seconds") > 0) {
                secondsForIteration = (Integer) cmdLine.getParsedOptionValue("seconds");
            } else {
                System.out.println("seconds argument not found or invalid, using default: " + secondsForIteration);
            }
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
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
            Utils.timeout(maxTimeToWait * 2);

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
        } while (!answer);

        return continuing;
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
