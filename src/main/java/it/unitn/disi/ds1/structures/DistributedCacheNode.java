package it.unitn.disi.ds1.structures;

import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.List;

/**
 * Node of the distributed cache
 * {@link it.unitn.disi.ds1.actors.Cache cache}
 */
public class DistributedCacheNode {
    /**
     * Children caches if any
     */
    public ArrayList<DistributedCacheNode> children;
    /**
     * Parent cache if any
     */
    public DistributedCacheNode parent;
    /**
     * Actor reference
     */
    public ActorRef actor;

    /**
     * Constructor of the distributed cache
     *
     * @param actor  reference actor
     * @param parent parent reference
     */
    public DistributedCacheNode(ActorRef actor, DistributedCacheNode parent) {
        this.actor = actor;
        this.parent = parent;
        this.children = new ArrayList<>();
    }

    /**
     * Insert a children {@link it.unitn.disi.ds1.actors.Cache cache}
     *
     * @param newActor actor reference of the cache to add
     */
    public void put(ActorRef newActor) {
        DistributedCacheNode newNode = new DistributedCacheNode(newActor, this);
        this.children.add(newNode);
    }

    /**
     * Insert a list of children {@link it.unitn.disi.ds1.actors.Cache caches}
     *
     * @param newActors actor reference list to add
     */
    public void putAll(List<ActorRef> newActors) {
        ArrayList<DistributedCacheNode> newNodes = new ArrayList<>();
        for (ActorRef actor : newActors) {
            newNodes.add(new DistributedCacheNode(actor, this));
        }
        children.addAll(newNodes);
    }

    /**
     * To String method of the Cached node
     *
     * @param depth depth of the node to print
     * @return node to string
     */
    public String toString(int depth) {
        StringBuilder res = new StringBuilder(actor.path().name() + "\n");
        if (!children.isEmpty()) {
            for (DistributedCacheNode child :
                    children) {
                for (int i = 0; i < depth; i++) {
                    res.append("\t");
                }
                res.append(child.toString(depth + 1));
            }
        }
        return res.toString();
    }
}
