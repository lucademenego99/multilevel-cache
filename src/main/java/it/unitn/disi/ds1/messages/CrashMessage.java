package it.unitn.disi.ds1.messages;

import it.unitn.disi.ds1.Config;

/**
 * Crash Message
 * Tells to crash after some iterations and what type of crash to do
 */
public class CrashMessage extends Message {

    /**
     * Type of the next simulated crash
     */
    public final Config.CrashType nextCrash;

    /**
     * After how many milliseconds the node should recover after the crash
     */
    public final Integer recoverIn;

    /**
     * Config.CrashType nextCrash constructor
     *
     * @param nextCrash next crash
     */
    public CrashMessage(Config.CrashType nextCrash) {
        this.nextCrash = nextCrash;
        // The last number is not included, thus we have to add 1
        this.recoverIn = Config.RANDOM.nextInt(Config.MAX_RECOVERY_IN - Config.MIN_RECOVERY_IN + 1) + Config.MIN_RECOVERY_IN;
    }
}
