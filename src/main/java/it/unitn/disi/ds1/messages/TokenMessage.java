package it.unitn.disi.ds1.messages;

/**
 * TokenMessage class
 */
public class TokenMessage extends Message {
    /**
     * Snapshot identifier
     */
    public final int snapId;

    /**
     * Token Message constructor
     *
     * @param snapId snapshot identifier
     */
    public TokenMessage(int snapId) {
        this.snapId = snapId;
    }
}
