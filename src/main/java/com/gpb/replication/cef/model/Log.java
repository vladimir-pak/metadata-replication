package com.gpb.replication.cef.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Log {
    private Integer id;
    private LocalDateTime created;
    private String log;
    private String type;

    public Log(LocalDateTime created, String log, String type) {
        this.created = created;
        this.log = log;
        this.type = type;
    }
}
