package it.unitn.disi.ds1;

import java.io.File;

/**
 * Test helper class
 * It implements some functions which are useful for testing
 */
public class Helper {
    /**
     * Clears the file at filename
     *
     * @param filename
     */
    public static void clearLogFile(String filename) {
        try {
            // Reset the log file by deleting and recreating the log file
            File file = new File(filename);
            file.delete();
            file.createNewFile();
        } catch (Exception exception) {
            System.err.println("Impossible to delete and recreate the log file");
            exception.printStackTrace();
        }
    }
}
