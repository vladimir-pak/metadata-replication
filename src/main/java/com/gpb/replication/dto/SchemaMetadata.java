package com.gpb.replication.dto;

import java.time.LocalDateTime;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter 
public class SchemaMetadata {

    private Long id;

    private String fqn;

    private String parentFqn;

    private String dbName;

    private String name;

    private String serviceName;

    private String hashData;

    private LocalDateTime createdAt;
}
