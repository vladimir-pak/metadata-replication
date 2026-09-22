package com.gpb.replication.service;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.gpb.replication.enums.DatabaseType;

@Component
public class ReplicationServiceRegistry {

    private final Map<DatabaseType, ReplicationService> services;

    public ReplicationServiceRegistry(
            List<ReplicationService> replicationServices) {

        this.services = replicationServices.stream()
                .collect(Collectors.toMap(
                        ReplicationService::getDatabaseType,
                        Function.identity(),
                        (first, second) -> {
                            throw new IllegalStateException(
                                    "Multiple replication services registered for "
                                    + first.getDatabaseType()
                            );
                        },
                        () -> new EnumMap<>(DatabaseType.class)
                ));
    }

    public ReplicationService get(DatabaseType databaseType) {

        ReplicationService service = services.get(databaseType);

        if (service == null) {
            throw new IllegalArgumentException(
                    "Replication is not supported for database type: "
                    + databaseType
            );
        }

        return service;
    }
}