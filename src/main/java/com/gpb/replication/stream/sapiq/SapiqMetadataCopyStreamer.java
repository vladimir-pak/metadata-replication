package com.gpb.replication.stream.sapiq;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class SapiqMetadataCopyStreamer
        extends AbstractSapiqMetadataCopyStreamer {

    public SapiqMetadataCopyStreamer(
            ObjectMapper objectMapper,
            @Value("${replication.sapiq.fetch-size:5000}")
            int fetchSize,
            @Value("${replication.sapiq.query-timeout-seconds:0}")
            int queryTimeoutSeconds) {

        super(
                objectMapper,
                fetchSize,
                queryTimeoutSeconds
        );
    }
}
