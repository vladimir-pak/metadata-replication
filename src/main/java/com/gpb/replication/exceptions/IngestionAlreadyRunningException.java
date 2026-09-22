package com.gpb.replication.exceptions;

public class IngestionAlreadyRunningException extends RuntimeException {

    public IngestionAlreadyRunningException(String serviceName) {
        super(
                String.format(
                        "Ingestion for service \"%s\" is already running",
                        serviceName
                )
        );
    }
}
