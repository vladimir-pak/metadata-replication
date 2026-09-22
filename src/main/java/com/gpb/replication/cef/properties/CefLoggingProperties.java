package com.gpb.replication.cef.properties;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Data
@Component
@ConfigurationProperties(prefix = "logging.cef")
public class CefLoggingProperties {
    private Path path = Paths.get("logs");
    private String fileName = "cef.log";
    private int retentionDays = 30;
}
