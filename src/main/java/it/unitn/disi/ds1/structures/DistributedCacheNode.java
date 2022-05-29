package it.unitn.disi.ds1.structures;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.actors.Actor;

import java.util.ArrayList;
import java.util.List;

public class DistributedCacheNode {
    public ArrayList<DistributedCacheNode> children;
    public DistributedCacheNode parent;
    public ActorRef actor;

    public DistributedCacheNode(ActorRef actor, DistributedCacheNode parent) {
        this.actor = actor;
        this.parent = parent;
        this.children = new ArrayList<>();
    }

    public void put(ActorRef newActor) {
        DistributedCacheNode newNode = new DistributedCacheNode(newActor, this);
        this.children.add(newNode);
    }

    public void putAll(List<ActorRef> newActors) {
        ArrayList<DistributedCacheNode> newNodes = new ArrayList<>();
        for (ActorRef actor : newActors) {
            newNodes.add(new DistributedCacheNode(actor, this));
        }
        children.addAll(newNodes);
    }

    public String toString(int depth) {
        StringBuilder res = new StringBuilder(actor.path().name() + "\n");
        if (!children.isEmpty()) {
            for (DistributedCacheNode child :
                    children) {
                for (int i = 0; i < depth; i++) {
                    res.append("\t");
                }
                res.append(child.toString(depth+1));
            }
        }
        return res.toString();
    }
}
