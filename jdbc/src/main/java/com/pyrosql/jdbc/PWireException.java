package com.pyrosql.jdbc;

/**
 * Runtime exception thrown when the PWire protocol encounters malformed data.
 */
public class PWireException extends RuntimeException {
    public PWireException(String message) {
        super(message);
    }

    public PWireException(String message, Throwable cause) {
        super(message, cause);
    }
}
