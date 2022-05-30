package it.unitn.disi.ds1;

import java.util.Random;

/**
 * General configuration class
 *
 * Here there will be listed all the configuration which can be employed for the
 * program
 *
 * TODO, would a YAML file be better?
 */
public class Config {
    /**
     * Number of L1 caches
     */
    public final static int N_L1 = 4;

    /**
     * Number of L2 caches associated to an L1 cache
     */
    public final static int N_L2 = 4;

    /**
     * Total number of clients
     */
    public final static int N_CLIENTS = 5;

    /**
     * Random number generator
     */
    public final static Random RANDOM = new Random();

    /**
     * Network delay in milliseconds
     */
    public final static int NETWORK_DELAY_MS = 10;

    /**
     * Timeout after which the client will make a request to a new cache
     */
    public final static int CLIENT_TIMEOUT = 1000;

    /**
     * Minimum milliseconds to wait to recover a crash
     */
    public final static int MIN_RECOVERY_IN = 250;

    /**
     * Maximum milliseconds to wait to recover a crash
     */
    public final static int MAX_RECOVERY_IN = 750;

    /**
     * Timeout after which the L2 cache will become a new L1 cache
     */
    public final static int L2_TIMEOUT = 500;

    /**
     * Number of iterations
     */
    public final static int N_ITERATIONS = 5;

    /**
     * Crash type
     */
    public enum CrashType {
        NONE,
        L1_BEFORE_READ,
        L1_AFTER_READ,
        L2_BEFORE_READ,
        L2_AFTER_READ,
        L1_BEFORE_WRITE,
        L1_AFTER_WRITE,
        L2_BEFORE_WRITE,
        L2_AFTER_WRITE,
        L1_BEFORE_RESPONSE,
        L1_AFTER_RESPONSE,
        L2_BEFORE_RESPONSE,
        L2_AFTER_RESPONSE,
    }

    /**
     * Discriminate the response message
     */
    public enum RequestType {
        READ,
        WRITE,
        CRITREAD,
        CRITWRITE
    }
}
