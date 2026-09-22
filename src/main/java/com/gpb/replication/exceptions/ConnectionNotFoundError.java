package com.gpb.replication.exceptions;

public class ConnectionNotFoundError extends RuntimeException {

    public ConnectionNotFoundError(String message) {
        super(message);
    }

    public ConnectionNotFoundError(String message, Throwable cause) {
        super(message, cause);
    }

}
