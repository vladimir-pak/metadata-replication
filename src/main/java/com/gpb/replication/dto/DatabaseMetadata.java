package com.gpb.replication.dto;

import java.time.LocalDateTime;

import lombok.Getter;
import lombok.Setter;

@Getter 
@Setter
public class DatabaseMetadata {

    private Long id;

    private String fqn;

    private String name;

    private String serviceName;

    private String hashData;

    private LocalDateTime createdAt;
}
