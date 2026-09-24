package com.gpb.replication.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.enums.ReplicationPipeline;

@Component
public class ReplicationServiceRegistry {

    private final Map<ServiceKey, ReplicationService> services;

    public ReplicationServiceRegistry(
            List<ReplicationService> replicationServices) {

        Map<ServiceKey, ReplicationService> result = new HashMap<>();

        for (ReplicationService service : replicationServices) {
            ServiceKey key = new ServiceKey(
                    service.getDatabaseType(),
                    service.getPipeline()
            );

            ReplicationService previous = result.putIfAbsent(
                    key,
                    service
            );

            if (previous != null) {
                throw new IllegalStateException(
                        "Multiple replication services registered for "
                                + key.databaseType()
                                + "/"
                                + key.pipeline()
                );
            }
        }

        this.services = Map.copyOf(result);
    }

    public ReplicationService get(
            DatabaseType databaseType) {

        return get(
                databaseType,
                ReplicationPipeline.STANDARD
        );
    }

    public ReplicationService get(
            DatabaseType databaseType,
            ReplicationPipeline pipeline) {

        ServiceKey key = new ServiceKey(
                databaseType,
                pipeline
        );

        ReplicationService service = services.get(key);

        if (service == null) {
            throw new IllegalArgumentException(
                    "Replication is not supported for database type/pipeline: "
                            + databaseType
                            + "/"
                            + pipeline
            );
        }

        return service;
    }

    private record ServiceKey(
            DatabaseType databaseType,
            ReplicationPipeline pipeline) {
    }
}
