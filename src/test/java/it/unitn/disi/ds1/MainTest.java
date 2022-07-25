package it.unitn.disi.ds1;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Test of the Main
 */
public class MainTest {
    /**
     * Test whether no exception are thrown in the main function
     */
    @Test
    public void verifyNoExceptionThrown() {
        // Prepare "no" in the standard input of the Main
        InputStream sysInBackup = System.in; // backup System.in to restore it later
        ByteArrayInputStream in = new ByteArrayInputStream("no".getBytes());
        System.setIn(in);
        // Main
        Main.main(new String[]{});
        // Restore system.in
        System.setIn(sysInBackup);
    }
}
