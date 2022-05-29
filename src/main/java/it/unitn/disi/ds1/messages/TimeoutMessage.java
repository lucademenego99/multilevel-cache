package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;

import java.io.Serializable;

public class TimeoutMessage extends Message {
    public final Serializable msg;
    public final ActorRef whoCrashed;

    public TimeoutMessage(Serializable msg, ActorRef whoCrashed) {
        this.msg = msg;
        this.whoCrashed = whoCrashed;
    }
}
