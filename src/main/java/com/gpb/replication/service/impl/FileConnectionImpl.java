package com.gpb.replication.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.replication.dto.SourceConnection;
import com.gpb.replication.exceptions.ConnectionNotFoundError;
import com.gpb.replication.service.ConnectionService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service 
@RequiredArgsConstructor 
@Slf4j 
@ConditionalOnProperty(
        name = "sources.connections.provider",
        havingValue = "file"
)
public class FileConnectionImpl implements ConnectionService {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ResourceLoader resourceLoader;

    @Value ("${sources.connections.file-path:classpath:db-connections.json}")
    private String filePath;

    public SourceConnection getConn(String serviceName) {
        if (serviceName == null || serviceName.isBlank()) {
            throw new IllegalArgumentException(
                    "serviceName must not be blank"
            );
        }

        Resource resource = resourceLoader.getResource(filePath);

        log.debug(
                "Searching connection '{}' in file: {}",
                serviceName,
                filePath
        );

        try (InputStream is = resource.getInputStream()) {

            List<SourceConnection> connections =
                    objectMapper.readValue(
                            is,
                            new TypeReference<List<SourceConnection>>() {}
                    );

            return connections.stream()
                    .filter(connection ->
                            Objects.equals(
                                    connection.getServiceName(),
                                    serviceName
                            )
                    )
                    .findFirst()
                    .orElseThrow(() ->
                            new ConnectionNotFoundError(
                                    "Connection not found. serviceName="
                                            + serviceName
                                            + ", filePath="
                                            + filePath
                            )
                    );

        } catch (IOException e) {
            throw new ConnectionNotFoundError(
                    "Failed to read connections file. filePath="
                            + filePath,
                    e
            );
        }
    }
}
