package com.gpb.replication.dto;

public record ReplicationStats(
        long databases,
        long schemas,
        long tables
) {
}