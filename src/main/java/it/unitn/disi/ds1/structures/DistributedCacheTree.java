package it.unitn.disi.ds1.structures;

import akka.actor.ActorRef;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Distributed Cache tree
 */
public class DistributedCacheTree {
    /**
     * Debug Logger instance {@link java.util.logging.Logger logger}
     */
    private final static Logger LOGGER = Logger.getLogger(DistributedCacheTree.class.getName());

    /**
     * Database instance
     */
    public DistributedCacheNode database;

    /**
     * Constructor of the Tree Cache
     *
     * @param database database
     */
    public DistributedCacheTree(ActorRef database) {
        LOGGER.setLevel(Level.INFO);
        this.database = new DistributedCacheNode(database, null);
    }

    /**
     * To string
     *
     * @return toString string
     */
    @Override
    public String toString() {
        return database.toString(0);
    }
}
