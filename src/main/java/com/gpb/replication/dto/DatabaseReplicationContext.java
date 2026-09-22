package com.gpb.replication.dto;

public record DatabaseReplicationContext(
        String databaseName,
        String databaseFqn
) {
}