package it.unitn.disi.ds1;

import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.UUID;
import java.util.logging.*;


class VerySimpleFormatter extends Formatter {

    /*
     * @see java.util.logging.Formatter#format(java.util.logging.LogRecord)
     */
    @Override
    public String format(LogRecord record) {
        return String.valueOf(record.getLevel()) + ':' +
                record.getMessage() + '\n';
    }
}

/**
 * Logger class
 */
public class Logger {
    public final static java.util.logging.Logger DEBUG = java.util.logging.Logger.getLogger(Main.class.getName());
    /**
     * Logger instance {@link java.util.logging.Logger logger}
     */
    private final static java.util.logging.Logger CHECK = java.util.logging.Logger.getLogger("check-solution-logger");

    /**
     * Init logger function
     */
    public static void initLogger() {
        // Set up a basic logger - log severity to consider: info, warning and severe
        CHECK.setLevel(Level.ALL);
        DEBUG.setLevel(Level.ALL);

        for (Handler handler :
                CHECK.getHandlers()) {
            handler.close();
        }

        // Provide a file where to save the logs
        try {
            FileHandler logFile = new FileHandler("logs.txt", false);
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
     *
     * @param logLevel           level of the log
     * @param requesterProcessId id of the process which performed the request
     * @param receiverProcessId  id of the process which has received the request
     * @param requestType        type of the request
     * @param isResponse         whether it is a response or not
     * @param key                key of the message
     * @param value              value of the message
     * @param seqNo              sequence number of the data
     * @param message            message additional
     */
    public static void logCheck(
            Level logLevel,
            Integer requesterProcessId,
            Integer receiverProcessId,
            Config.RequestType requestType,
            Boolean isResponse,
            Integer key,
            Integer value,
            Integer seqNo,
            String message,
            UUID queryID
    ) {
        String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
        String logMessage = MessageFormat.format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}",
                logLevel, timeStamp, requesterProcessId, receiverProcessId,
                requestType.name(), isResponse, key, value, seqNo, queryID, message
        );
        // Debug log level
        Logger.CHECK.log(logLevel, logMessage);
    }

    public static void logConfig(int countL1, int countL2, int countClients) {
        Logger.CHECK.log(Level.CONFIG, MessageFormat.format("\t{0}\t{1}\t{2}", countL1, countL2, countClients));
    }

    public static void logDatabase(Map<Integer, Integer> database) {
        StringBuilder keyValuePairs = new StringBuilder();
        for (Map.Entry<Integer, Integer> entry : database.entrySet()) {
            keyValuePairs.append("\t").append(entry.getKey()).append("-").append(entry.getValue());
        }
        Logger.CHECK.log(Level.CONFIG, keyValuePairs.toString());
    }
}
