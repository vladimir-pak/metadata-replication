package com.gpb.replication.enums;

import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum ReplicationPipeline {
    STANDARD,
    HYBRID;

    @JsonCreator
    public static ReplicationPipeline from(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return ReplicationPipeline.valueOf(
                value.trim().toUpperCase(Locale.ROOT)
        );
    }
}
