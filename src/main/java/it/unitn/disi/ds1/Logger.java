package it.unitn.disi.ds1;

import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.*;


class VerySimpleFormatter extends Formatter {

    /*
     * @see java.util.logging.Formatter#format(java.util.logging.LogRecord)
     */
    @Override
    public String format(LogRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append(record.getLevel()).append(':');
        sb.append(record.getMessage()).append('\n');
        return sb.toString();
    }
}

/**
 * Logger class
 */
public class Logger {
    /**
     * Logger instance {@link java.util.logging.Logger logger}
     */
    private final static java.util.logging.Logger CHECK = java.util.logging.Logger.getLogger(Main.class.getName());

    public final static java.util.logging.Logger DEBUG = java.util.logging.Logger.getLogger(Main.class.getName());

    /**
     * Init logger function
     */
    public static void initLogger() {
        // Set up a basic logger - log severity to consider: info, warning and severe
        CHECK.setLevel(Level.ALL);
        DEBUG.setLevel(Level.ALL);

        // Provide a file where to save the logs
        try {
            FileHandler logFile = new FileHandler("logs.txt");
            Formatter txtFormatter = new VerySimpleFormatter();
            logFile.setFormatter(txtFormatter);
            CHECK.addHandler(logFile);
            System.out.println("Log file will be available at ./logs.txt");
        } catch (IOException e) {
            DEBUG.severe("Error creating a file handler for logs.\n" + e);
        }
    }

    /**
     * Log values according to a tab-separated value format
     * @param logLevel level of the log
     * @param requesterProcessId id of the process which performed the request
     * @param receiverProcessId id of the process which has received the request
     * @param requestType type of the request
     * @param isResponse whether it is a response or not
     * @param key key of the message
     * @param value value of the message
     * @param seqNo sequence number of the data
     * @param message message additional
     */
    public static void logCheck(Level logLevel, Integer requesterProcessId, Integer receiverProcessId, Config.RequestType requestType,
                           Boolean isResponse, Integer key, Integer value, Integer seqNo, String message){
        String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
        String logMessage = MessageFormat.format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}",
                logLevel, timeStamp, requesterProcessId, receiverProcessId,
                requestType.name(), isResponse, key, value, seqNo, message
        );
        // Debug log level
        Logger.CHECK.log(logLevel, logMessage);
    }
}
