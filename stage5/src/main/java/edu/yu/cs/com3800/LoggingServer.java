package edu.yu.cs.com3800;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.*;

public interface LoggingServer {

    default Logger initializeLogging(String packageName, String fileNamePreface) throws IOException {
        return initializeLogging(packageName,fileNamePreface, false);
    }

    default Logger initializeLogging(String packageName, String fileNamePreface, boolean disableParentHandlers) throws IOException {
        return createLogger(packageName,this.getClass().getName(), fileNamePreface, disableParentHandlers);
    }

    static Logger createLogger(String packageName,String loggerName, String fileNamePreface, boolean disableParentHandlers) throws IOException {
        // Create a logger with the given name
        Logger logger = Logger.getLogger(loggerName+fileNamePreface);

        // Set the logger's level
        logger.setLevel(Level.ALL); // You can change the log level as needed

        String filePath = packageName.replace('.', '/');
        Path logDirectory = Path.of(filePath);
        Files.createDirectories(logDirectory);

        // Create a FileHandler to write log messages to a file in the specified package
        filePath = filePath + "/" + fileNamePreface + ".log";
        FileHandler fileHandler = new FileHandler(filePath);
        fileHandler.setFormatter(new SimpleFormatter());

        // Add the FileHandler to the logger
        logger.addHandler(fileHandler);

        // Optionally, disable parent handlers
        if (disableParentHandlers) {
            for (Handler handler : logger.getParent().getHandlers()) {
                logger.getParent().removeHandler(handler);
            }
        }

        return logger;
    }
}
