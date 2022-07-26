package it.unitn.disi.ds1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Checker class
 * <p>
 * This class checks the log file 'logs.txt' verifying whether in the last run
 * the program behaved correctly without any inconsistencies with respect to the
 * project requirements
 * <p>
 */
public class Checker {

    public static void main(String[] args) {
        boolean result = check();

        if (result) {
            System.out.println("CONSISTENT!");
        } else {
            System.out.println("NOT CONSISTENT");
        }
    }

    /**
     * Check if the log file 'logs.txt' represents a consistent run
     *
     * @return True if the run is consistent, False otherwise
     */
    public static boolean check() {
        // Database created during the run
        Map<Integer, Integer> database = null;

        // Information about the architecture
        int countL1, countL2, countClients;

        // State of all caches
        Map<Integer, Map<Integer, Integer>> cachesState = new HashMap<>();

        // Remember which L1 is the parent for each L2
        Map<Integer, Integer> parentOf = new HashMap<>();

        // Remember which CRITWRITE are currently being handled by the database
        Set<Integer> handledCritWrites = new HashSet<>();

        // Remember if we are expecting some errors due to CRITWRITES for some requests
        Set<UUID> errorsDueToCritWrites = new HashSet<>();

        // All requests
        Map<UUID, LogCheck> requests = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader("logs.txt"))) {
            int count = 0;
            String line;
            while ((line = br.readLine()) != null) {
                if (count == 0) {
                    // The first line contains information about the architecture: process it
                    String[] parts = line.split("\t");
                    countL1 = Integer.parseInt(parts[1]);
                    countL2 = Integer.parseInt(parts[2]);
                    countClients = Integer.parseInt(parts[3]);

                    // Initialize the system's state based on the received information
                    // Keep a counter to give a unique ID to each entity
                    int counterID = 0;
                    // L1s
                    for (int i = 0; i < countL1; i++) {
                        cachesState.put(++counterID, new HashMap<>());
                    }
                    // For each L1, create the L2s
                    for (int i = 0; i < countL1; i++) {
                        for (int j = 0; j < countL2; j++) {
                            parentOf.put(++counterID, i + 1);
                            cachesState.put(counterID, new HashMap<>());
                        }
                    }
                } else if (count == 1) {
                    // The second line contains the database's values: process it
                    database = processDatabase(line);
                } else {
                    assert database != null;
                    // All the other lines contain details about the run
                    LogCheck logCheck = new LogCheck(line);
                    if (logCheck.requestType == Config.RequestType.FLUSH) {
                        // If a cache crashed, clear the values it contained
                        cachesState.get(logCheck.sender).clear();
                    } else {
                        if (!logCheck.isResponse) {
                            // If it's a request, store it in a map where the key is the request's UUID
                            if (!requests.containsKey(logCheck.uuid)) {
                                requests.put(logCheck.uuid, logCheck);
                            }

                            // If there is already a CRITWRITE on this key
                            if (handledCritWrites.contains(logCheck.key)) {
                                System.out.println("There should be an error due to a CRITWRITE being handled for key " + logCheck.key + " and uuid " + logCheck.uuid);
                                errorsDueToCritWrites.add(logCheck.uuid);
                            }

                            if (logCheck.receiver == 0 && logCheck.requestType == Config.RequestType.CRITWRITE) {
                                // If this is a CRITWRITE request, remember that a CRITWRITE
                                // is currently handled by the database for the specified key
                                handledCritWrites.add(logCheck.key);
                            }
                        } else {
                            // We are dealing with a response, so we need to check that everything is consistent

                            // Get the original request
                            LogCheck original = requests.get(logCheck.uuid);

                            if (Objects.equals(logCheck.receiver, original.sender)) {
                                // If the response is for the final client who performed the request
                                if (logCheck.value == null) {
                                    // There was an error

                                    // If the error was due to the fact that a CRITWRITE was handled
                                    // when the request was received, we expected it!
                                    if (errorsDueToCritWrites.contains(original.uuid)) {
                                        System.out.println("Error due to CRITWRITE gotten as expected for key " + logCheck.key + " and uuid " + logCheck.uuid);
                                    }
                                    errorsDueToCritWrites.remove(original.uuid);
                                } else {
                                    // No error - check everything is consistent
                                    switch (original.requestType) {
                                        case READ:
                                            // The returned value should be consistent with what the caches had,
                                            // or with the value contained by the database if the key was not in the cache
                                            if (cachesState.get(logCheck.sender).containsKey(original.key)) {
                                                if (!Objects.equals(cachesState.get(logCheck.sender).get(original.key), logCheck.value)) {
                                                    System.err.println("Not consistent - the read returned a wrong value");
                                                    return false;
                                                }
                                            } else if (cachesState.get(parentOf.get(logCheck.sender)).containsKey(original.key)) {
                                                if (!Objects.equals(cachesState.get(parentOf.get(logCheck.sender)).get(original.key), logCheck.value)) {
                                                    System.err.println("Not consistent - the read returned a wrong value");
                                                    return false;
                                                }
                                            } else {
                                                if (!Objects.equals(database.get(original.key), logCheck.value)) {
                                                    System.err.println("Not consistent - the read returned a wrong value");
                                                    return false;
                                                }
                                            }
                                            break;
                                        case WRITE:
                                            // Nothing to do here?
                                            break;
                                        case CRITREAD:
                                            // It should return the correct value from the database
                                            if (!Objects.equals(database.get(original.key), logCheck.value)) {
                                                System.err.println("Not consistent - the critread returned a wrong value");
                                                return false;
                                            }
                                            break;
                                        case CRITWRITE:
                                            // Nothing to do here?
                                            break;
                                    }
                                }
                            } else {
                                // The response is for another cache
                                if (logCheck.value == null) {
                                    // If the error was due to the fact that a CRITWRITE was handled
                                    // when the request was received, we expected it!
                                    if (errorsDueToCritWrites.contains(original.uuid)) {
                                        System.out.println("Error due to CRITWRITE gotten as expected for key " + logCheck.key + " and uuid " + logCheck.uuid);
                                    }
                                    errorsDueToCritWrites.remove(original.uuid);

                                    // We got the response from the database, so even if it's an error
                                    // we have to remove the request from the handledCritWrites
                                    if (original.requestType == Config.RequestType.CRITWRITE) {
                                        handledCritWrites.remove(original.key);
                                    }
                                } else {
                                    if (original.requestType == Config.RequestType.WRITE || original.requestType == Config.RequestType.CRITWRITE) {
                                        // We have to check if the returned value is correct
                                        if (logCheck.sender == 0) {
                                            // The sender is the database, update the value
                                            database.remove(original.key);
                                            database.put(original.key, logCheck.value);

                                            if (original.requestType == Config.RequestType.CRITWRITE) {
                                                handledCritWrites.remove(original.key);
                                            }
                                        } else {
                                            // The sender is an L1 cache, update the value if needed for both sender (L1) and receiver (L2)
                                            if (cachesState.get(logCheck.sender).containsKey(original.key)) {
                                                cachesState.get(logCheck.sender).remove(original.key);
                                                cachesState.get(logCheck.sender).put(original.key, logCheck.value);
                                            }
                                            if (cachesState.get(logCheck.receiver).containsKey(original.key)) {
                                                cachesState.get(logCheck.receiver).remove(original.key);
                                                cachesState.get(logCheck.receiver).put(original.key, logCheck.value);
                                            }
                                        }
                                    } else if (original.requestType == Config.RequestType.READ || original.requestType == Config.RequestType.CRITREAD) {
                                        // Add in the cache's state the key-value pair
                                        cachesState.get(logCheck.receiver).remove(original.key);
                                        cachesState.get(logCheck.receiver).put(original.key, logCheck.value);
                                    }
                                }
                            }
                        }
                    }
                }
                count++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // If we are still expecting some errors due to the CRITWRITES, it means the state is not consistent
        if (!errorsDueToCritWrites.isEmpty()) {
            System.err.println("Not consistent - errors due to critwrites is not empty: " + errorsDueToCritWrites);
        }
        return errorsDueToCritWrites.isEmpty();
    }

    /**
     * Process the database from the log file
     *
     * @param line String containing the database information
     * @return A map modelling the database as a key-value map of integers
     */
    private static Map<Integer, Integer> processDatabase(String line) {
        String[] parts = line.split("\t");

        Map<Integer, Integer> database = new HashMap<>();

        // For i=0, we only have the log level - we don't need it, so we start from 1
        for (int i = 1; i < parts.length; i++) {
            String[] keyValuePair = parts[i].split("-");
            database.put(Integer.parseInt(keyValuePair[0]), Integer.parseInt(keyValuePair[1]));
        }

        return database;
    }
}

/**
 * LogCheck Class
 * <p>
 * Class used to represent a specific log line, containing an event
 * happened during the run
 */
class LogCheck {
    final String timestamp;
    final Integer sender, receiver;
    final Config.RequestType requestType;
    final boolean isResponse;
    final Integer key, value;
    final Integer seqno;
    final UUID uuid;


    /**
     * Standard constructor
     *
     * @param timestamp   Timestamp of the log
     * @param sender      Sender Actor
     * @param receiver    Receiver Actor
     * @param requestType Type of request associated with the event
     * @param isResponse  Is it a response or a request?
     * @param key         Key associated with the event
     * @param value       Value associated with the event
     * @param seqno       Sequence number associated with the event
     * @param uuid        Unique Identifier (UUID) associated with the event
     */
    public LogCheck(String timestamp, Integer sender, Integer receiver, Config.RequestType requestType, boolean isResponse,
                    Integer key, Integer value, Integer seqno, UUID uuid) {
        this.timestamp = timestamp;
        this.sender = sender;
        this.receiver = receiver;
        this.requestType = requestType;
        this.isResponse = isResponse;
        this.key = key;
        this.value = value;
        this.seqno = seqno;
        this.uuid = uuid;
    }

    /**
     * Constructor overload - Instantiate a LogCheck
     * starting from a String, a line from the log file
     *
     * @param line String containing the information needed to create a LogCheck object
     */
    public LogCheck(String line) {
        String[] parts = line.split("\t");

        this.timestamp = parts[1];
        this.sender = Integer.parseInt(parts[2]);
        this.receiver = Integer.parseInt(parts[3]);
        this.requestType = Config.RequestType.valueOf(parts[4]);
        this.isResponse = Boolean.parseBoolean(parts[5]);
        this.key = isParsable(parts[6]) ? Integer.parseInt(parts[6]) : null;
        this.value = isParsable(parts[7]) ? Integer.parseInt(parts[7]) : null;
        this.seqno = isParsable(parts[8]) ? Integer.parseInt(parts[8]) : null;
        this.uuid = !Objects.equals(parts[9], "null") ? UUID.fromString(parts[9]) : UUID.randomUUID();
    }

    /**
     * Check if the input string is parsable as an Integer
     *
     * @param input Input to check
     * @return True if it's parsable, False otherwise
     */
    public static boolean isParsable(String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (final NumberFormatException e) {
            return false;
        }
    }
}

/**
 * OTHER TODOs
 * - put some enums in place of true and false in Checker
 * - maybe let the LogChecker deal with LogCheck(String line)
 * - think alternative ways of implementing CRIT WRITE
 */