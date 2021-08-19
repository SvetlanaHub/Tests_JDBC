package utils;

import org.apache.log4j.Logger;

public class Log {
    private static final Logger log = Logger.getLogger(Log.class);

    public static void trace(String message) {
        log.trace(message);
    }

    public static void trace(String message, Throwable exception) {
        log.trace(message, exception);
    }

    public static void debug(String message) {
        log.debug(message);
    }

    public static void debug(String message, Throwable exception) {
        log.debug(message, exception);
    }

    public static void info(String message) {
        log.info(message);
    }

    public static void info(String message, Throwable exception) {
        log.info(message, exception);
    }

    public static void warn(String message) {
        log.warn(message);
    }

    public static void warn(String message, Throwable exception) {
        log.warn(message, exception);
    }

    public static void error(String message, Throwable throwable) {
        log.error(message, throwable);
        throw new AssertionError(throwable);
    }

    public static void error(String message) {
        log.error(message);
    }

    public static void error(Exception error) {
        log.error(" --- Exception occurs --- ", error);
        throw new AssertionError(error);
    }

}
