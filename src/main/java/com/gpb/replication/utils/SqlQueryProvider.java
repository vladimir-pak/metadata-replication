package com.gpb.replication.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.enums.MetadataType;

@Component
public class SqlQueryProvider {

    private final ResourceLoader resourceLoader;
    private final Map<String, String> cache = new ConcurrentHashMap<>();

    public SqlQueryProvider(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    public String getQuery(
            DatabaseType databaseType,
            MetadataType metadataType) {

        return getQuery(
                databaseType,
                metadataType.name().toLowerCase()
        );
    }

    public String getQuery(
            DatabaseType databaseType,
            String queryName) {

        if (queryName == null
                || !queryName.matches("[A-Za-z0-9_-]+")) {

            throw new IllegalArgumentException(
                    "Invalid SQL query name: " + queryName
            );
        }

        String path = String.format(
                "classpath:sql/%s/%s.sql",
                databaseType.name().toLowerCase(),
                queryName.toLowerCase()
        );

        return cache.computeIfAbsent(path, this::load);
    }

    private String load(String path) {
        Resource resource = resourceLoader.getResource(path);

        try (InputStream inputStream = resource.getInputStream()) {
            return new String(
                    inputStream.readAllBytes(),
                    StandardCharsets.UTF_8
            );
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Cannot load SQL query: " + path,
                    e
            );
        }
    }
}
