package com.gpb.replication.dto;

import java.time.LocalDateTime;

import com.fasterxml.jackson.databind.JsonNode;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TableMetadata {

    private Long id;

    private String fqn;

    private String serviceName;

    private String dbName;

    private String schemaName;

    private String name;

    private String description;

    private String parentFqn;

    private JsonNode data;

    private String hashData;

    private LocalDateTime createdAt;
}
