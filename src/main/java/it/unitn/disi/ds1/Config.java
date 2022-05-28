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
    public final static int N_L1 = 3;

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
     * Timeout after which the L2 cache will become a new L1 cache
     */
    public final static int L2_TIMEOUT = 500;

    /**
     * Crash type
     */
    public enum CrashType {
        NONE,
    }
}
