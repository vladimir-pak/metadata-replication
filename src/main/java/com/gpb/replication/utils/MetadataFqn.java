package com.gpb.replication.utils;

public final class MetadataFqn {

    private MetadataFqn() {
    }

    public static String database(
            String serviceName,
            String databaseName) {

        return serviceName
                + "."
                + databaseName;
    }

    public static String schema(
            String serviceName,
            String databaseName,
            String schemaName) {

        return database(
                serviceName,
                databaseName
        )
                + "."
                + schemaName;
    }

    public static String table(
            String serviceName,
            String databaseName,
            String schemaName,
            String tableName) {

        return schema(
                serviceName,
                databaseName,
                schemaName
        )
                + "."
                + tableName;
    }
}