package com.gpb.replication.stream.sapiq;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class SapiqHybridMetadataCopyStreamer
        extends AbstractSapiqMetadataCopyStreamer {

    public SapiqHybridMetadataCopyStreamer(
            ObjectMapper objectMapper,
            @Value("${replication.sapiq.hybrid.fetch-size:10000}")
            int fetchSize,
            @Value("${replication.sapiq.hybrid.query-timeout-seconds:0}")
            int queryTimeoutSeconds) {

        super(
                objectMapper,
                fetchSize,
                queryTimeoutSeconds
        );
    }
}
