package com.gpb.replication.dto;

import lombok.Getter;
import lombok.Setter;

@Getter 
@Setter
public class ReplicationRequestDto {
    private String serviceName;
    private boolean async;
}
