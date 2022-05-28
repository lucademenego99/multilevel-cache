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
     * Crash type
     */
    public enum CrashType {
        NONE,
    }
}
