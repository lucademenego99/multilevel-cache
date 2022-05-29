package it.unitn.disi.ds1.structures;

import akka.actor.ActorRef;

import java.util.List;

public class Architecture {
    public DistributedCacheTree cacheTree;
    public List<ActorRef> clients;

    public Architecture(DistributedCacheTree cacheTree, List<ActorRef> clients) {
        this.cacheTree = cacheTree;
        this.clients = clients;
    }

    @Override
    public String toString() {
        return "ARCHITECTURE" +
                "\n-----------" +
                "\nCACHE-TREE\n" + cacheTree +
                "-----------" +
                "\nCLIENTS\n" + clients;
    }
}
