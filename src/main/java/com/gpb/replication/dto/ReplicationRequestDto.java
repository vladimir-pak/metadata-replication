package com.gpb.replication.dto;

import com.gpb.replication.enums.ReplicationPipeline;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ReplicationRequestDto {
    private String serviceName;
    private boolean async;

    /*
     * Optional.
     * Если null — используется default из application.yaml.
     */
    private ReplicationPipeline pipeline;
}
