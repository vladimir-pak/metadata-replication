package com.gpb.replication.exceptions;

public class MetadataReplicationException extends RuntimeException {

    public MetadataReplicationException(String message) {
        super(message);
    }

    public MetadataReplicationException(
            String message,
            Throwable cause) {

        super(message, cause);
    }
}
