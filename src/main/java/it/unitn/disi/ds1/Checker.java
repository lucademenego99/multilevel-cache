package it.unitn.disi.ds1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Checker class
 *
 * This class checks the log file 'logs.txt' verifying whether in the last run
 * the program behaved correctly without any inconsistencies with respect to the
 * project requirements
 *
 * TODO: we have to recreate the structure of db+L1+L2. Every entity will have its own state that
 *       we need to keep track of. We should add to the logs file the number of L1 and L2 caches
 *       to recreate the architecture.
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
     * @return True if the run is consistent, False otherwise
     */
    public static boolean check() {
        // Database created during the run
        Map<Integer, Integer> database = null;

        Map<UUID, LogCheck> requests = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader("logs.txt"))) {
            int count = 0;
            String line;
            while ((line = br.readLine()) != null) {
                if (count == 0) {
                    // The first line contains the database's values: process it
                    database = processDatabase(line);
                } else {
                    // All the other lines contain details about the run
                    LogCheck logCheck = new LogCheck(line);
                    if (!logCheck.isResponse) {
                        // If it's a request, store it in a map where the key is the request's UUID
                        requests.put(logCheck.uuid, logCheck);
                    } else {
                        // We are dealing with a response, so we need to check that everything is consistent

                        // Get the original request
                        LogCheck original = requests.get(logCheck.uuid);

                        if (Objects.equals(logCheck.receiver, original.sender)) {
                            // If the response is for the final client who performed the request
                            if (logCheck.value == null) {
                                // There was an error
                                // Nothing to do here?
                            } else {
                                // No error - check everything is consistent
                                switch (original.requestType) {
                                    case READ:
                                        // It should return the value logCheck.sender contains in the cache,
                                        // or if the sender didn't have the key in its cache it should return
                                        // the correct value from the database
                                        break;
                                    case WRITE:
                                        // Nothing to do here?
                                        break;
                                    case CRITREAD:
                                        // It should return the correct value from the database
                                        break;
                                    case CRITWRITE:
                                        // If we arrive here but there was another CRITWRITE for the same key
                                        // previously requested by another client, the run is not consistent
                                        // and we should return false
                                        break;
                                }
                            }
                        } else {
                            // The response is for another cache
                            if (original.requestType == Config.RequestType.WRITE || original.requestType == Config.RequestType.CRITWRITE) {
                                // We have to check whether we should update the cache with the new value or not
                                // We have to check if the returned value is correct
                                //   ( if it came from the database, check the database's state )
                                //   ( if it came from the L1, check the L1's state )
                            }
                        }
                    }
                }
                count ++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    /**
     * Process the database from the log file
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
 *
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
     * @param timestamp Timestamp of the log
     * @param sender Sender Actor
     * @param receiver Receiver Actor
     * @param requestType Type of request associated with the event
     * @param isResponse Is it a response or a request?
     * @param key Key associated with the event
     * @param value Value associated with the event
     * @param seqno Sequence number associated with the event
     * @param uuid Unique Identifier (UUID) associated with the event
     */
    LogCheck(String timestamp, Integer sender, Integer receiver, Config.RequestType requestType, boolean isResponse, Integer key, Integer value, Integer seqno, UUID uuid) {
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
    LogCheck(String line) {
        String[] parts = line.split("\t");

        this.timestamp = parts[1];
        this.sender = Integer.parseInt(parts[2]);
        this.receiver = Integer.parseInt(parts[3]);
        this.requestType = Config.RequestType.valueOf(parts[4]);
        this.isResponse = Boolean.parseBoolean(parts[5]);
        this.key = isParsable(parts[6]) ? Integer.parseInt(parts[6]) : null;
        this.value = isParsable(parts[7]) ? Integer.parseInt(parts[7]) : null;
        this.seqno = isParsable(parts[8]) ? Integer.parseInt(parts[8]) : null;
        this.uuid = UUID.fromString(parts[9]);
    }

    /**
     * Check if the input string is parsable as an Integer
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
