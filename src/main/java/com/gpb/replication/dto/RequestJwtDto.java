package com.gpb.replication.dto;

import lombok.Getter;
import lombok.Setter;

@Getter 
@Setter 
public class RequestJwtDto {
    private String secret;
    private String service;
}
