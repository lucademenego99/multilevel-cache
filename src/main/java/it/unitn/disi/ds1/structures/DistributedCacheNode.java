package it.unitn.disi.ds1.structures;

import it.unitn.disi.ds1.actors.Actor;

import java.util.ArrayList;
import java.util.List;

public class DistributedCacheNode {
    public ArrayList<DistributedCacheNode> children;
    public DistributedCacheNode parent;
    public Actor actor;

    public DistributedCacheNode(Actor actor, DistributedCacheNode parent) {
        this.actor = actor;
        this.parent = parent;
        children = new ArrayList<>();
    }

    public void put(Actor newActor) {
        DistributedCacheNode newNode = new DistributedCacheNode(newActor, this);
        children.add(newNode);
    }

    public void putAll(List<Actor> newActors) {
        ArrayList<DistributedCacheNode> newNodes = new ArrayList<>();
        for (Actor actor : newActors) {
            newNodes.add(new DistributedCacheNode(actor, this));
        }
        children.addAll(newNodes);
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder("\t" + actor.id);
        if (!children.isEmpty()) {
            for (DistributedCacheNode child :
                    children) {
                res.append("\n\t" + child.toString());
            }
        }
        return res.toString();
    }
}
