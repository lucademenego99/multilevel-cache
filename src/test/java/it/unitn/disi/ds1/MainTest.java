package it.unitn.disi.ds1;

import org.junit.jupiter.api.Test;

/**
 * Test of the Main
 */
public class MainTest{
    /**
     * Test whether no exception are thrown in the main function
     */
    @Test
    public void verifyNoExceptionThrown(){
        Main.main(new String[]{});
    }
}
