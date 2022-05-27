package it.unitn.disi.ds1.structures;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.actors.Actor;
import it.unitn.disi.ds1.actors.Database;

import java.util.logging.Level;
import java.util.logging.Logger;


public class DistributedCacheTree {

    private final static Logger LOGGER = Logger.getLogger(DistributedCacheTree.class.getName());
    
    public DistributedCacheNode database;

    public DistributedCacheTree(Actor database) {
        LOGGER.setLevel(Level.INFO);
        this.database = new DistributedCacheNode(database, null);
    }

    @Override
    public String toString() {
        return database.toString();
    }
}
