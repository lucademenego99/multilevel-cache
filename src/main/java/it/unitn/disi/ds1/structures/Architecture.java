package it.unitn.disi.ds1.structures;

import akka.actor.ActorRef;

import java.util.List;

/**
 * Cache three architecture of:
 * - {@link it.unitn.disi.ds1.actors.Cache caches}
 * - {@link it.unitn.disi.ds1.actors.Client database}
 */
public class Architecture {
    /**
     * Tree of caches
     */
    public DistributedCacheTree cacheTree;
    /**
     * List of clients
     */
    public List<ActorRef> clients;

    /**
     * Constructor of the Architecture
     *
     * @param cacheTree tree of caches
     * @param clients   list of clients
     */
    public Architecture(DistributedCacheTree cacheTree, List<ActorRef> clients) {
        this.cacheTree = cacheTree;
        this.clients = clients;
    }

    /**
     * Prints the architecture structure
     *
     * @return architecture to string
     */
    @Override
    public String toString() {
        return "ARCHITECTURE" +
                "\n-----------" +
                "\nCACHE-TREE\n" + cacheTree +
                "-----------" +
                "\nCLIENTS\n" + clients;
    }
}
