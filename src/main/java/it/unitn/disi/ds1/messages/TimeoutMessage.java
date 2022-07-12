package it.unitn.disi.ds1.messages;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Timeout message class
 */
public class TimeoutMessage extends Message {
    /**
     * Message we were waiting a response for
     */
    public final Serializable msg;
    /**
     * Who we think has crashed
     */
    public final ActorRef whoCrashed;

    /**
     * Timeout message constructor
     *
     * @param msg        message we are waiting
     * @param whoCrashed actor reference of who is crashed
     */
    public TimeoutMessage(Serializable msg, ActorRef whoCrashed) {
        this.msg = msg;
        this.whoCrashed = whoCrashed;
    }
}
