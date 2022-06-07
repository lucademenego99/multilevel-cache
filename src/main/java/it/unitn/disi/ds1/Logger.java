package it.unitn.disi.ds1;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

/**
 * Logger class
 */
public class Logger {
    /**
     * Logger instance {@link java.util.logging.Logger logger}
     */
    public final static java.util.logging.Logger INSTANCE = java.util.logging.Logger.getLogger(Main.class.getName());

    /**
     * Init logger function
     */
    public static void initLogger() {
        // Set up a basic logger - log severity to consider: info, warning and severe
        INSTANCE.setLevel(Level.INFO);
        // Provide a file where to save the logs
        try {
            FileHandler logFile = new FileHandler("logs.txt");
            Formatter txtFormatter = new SimpleFormatter();
            logFile.setFormatter(txtFormatter);
            INSTANCE.addHandler(logFile);
            System.out.println("Log file will be available at ./logs.txt");
        } catch (IOException e) {
            INSTANCE.severe("Error creating a file handler for logs.\n" + e);
        }
    }
}
