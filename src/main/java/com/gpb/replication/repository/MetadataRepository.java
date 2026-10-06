package com.gpb.replication.repository;

import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.replication.enums.DatabaseType;

@Repository 
public class MetadataRepository {

    private final JdbcTemplate jdbcTemplate;

    public MetadataRepository(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbcTemplate
    ) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public int deleteDatabaseMetadata(
            String serviceName,
            DatabaseType databaseType) {

        String tableName = getDatabaseMetadataTable(databaseType);

        return delete(tableName, serviceName);
    }

    public int deleteSchemaMetadata(
            String serviceName,
            DatabaseType databaseType) {

        String tableName = getSchemaMetadataTable(databaseType);

        return delete(tableName, serviceName);
    }

    public int deleteTableMetadata(
            String serviceName,
            DatabaseType databaseType) {

        String tableName = getTableMetadataTable(databaseType);

        return delete(tableName, serviceName);
    }

    private int delete(
        String tableName,
        String serviceName) {

        String sql = """
                DELETE FROM metadata_replication.%s
                WHERE service_name = ?
                """.formatted(tableName);

        return jdbcTemplate.update(
            sql,
            serviceName
        );
    }

    public int deleteSchemaMetadata(
            String serviceName,
            String databaseName,
            DatabaseType databaseType) {

        String tableName = getSchemaMetadataTable(databaseType);

        return deleteByDatabase(
            tableName,
            serviceName,
            databaseName
        );
    }

    public int deleteTableMetadata(
            String serviceName,
            String databaseName,
            DatabaseType databaseType) {

        String tableName = getTableMetadataTable(databaseType);

        return deleteByDatabase(
            tableName,
            serviceName,
            databaseName
        );
    }

    public int deleteSchemaMetadataNotInDatabases(
            String serviceName,
            List<String> databaseNames,
            DatabaseType databaseType) {

        String tableName = getSchemaMetadataTable(databaseType);

        return deleteNotInDatabases(
            tableName,
            serviceName,
            databaseNames
        );
    }

    public int deleteTableMetadataNotInDatabases(
            String serviceName,
            List<String> databaseNames,
            DatabaseType databaseType) {

        String tableName = getTableMetadataTable(databaseType);

        return deleteNotInDatabases(
            tableName,
            serviceName,
            databaseNames
        );
    }

    private int deleteNotInDatabases(
            String tableName,
            String serviceName,
            List<String> databaseNames) {

        if (databaseNames == null || databaseNames.isEmpty()) {

            throw new IllegalArgumentException(
                    "databaseNames must not be empty"
            );
        }

        String placeholders =
                String.join(
                    ", ",
                    java.util.Collections.nCopies(
                        databaseNames.size(),
                        "?"
                    )
                );

        String sql = """
                DELETE FROM metadata_replication.%s
                WHERE service_name = ?
                AND db_name NOT IN (%s)
                """.formatted(
                    tableName,
                    placeholders
                );

        Object[] params = new Object[databaseNames.size() + 1];

        params[0] = serviceName;

        for (int i = 0;
            i < databaseNames.size();
            i++) {
            params[i + 1] = databaseNames.get(i);
        }

        return jdbcTemplate.update(
            sql,
            params
        );
    }

    private int deleteByDatabase(
            String tableName,
            String serviceName,
            String databaseName) {

        String sql = """
                DELETE FROM metadata_replication.%s
                WHERE service_name = ?
                AND db_name = ?
                """.formatted(tableName);

        return jdbcTemplate.update(
            sql,
            serviceName,
            databaseName
        );
    }

    private String getDatabaseMetadataTable(DatabaseType databaseType) {

        return switch (databaseType) {
            case POSTGRES -> "database_metadata_postgres";
            case ORACLE -> "database_metadata_oracle";
            case MSSQL -> "database_metadata_mssql";
            case SAPIQ -> "database_metadata_sapiq";
            case SAPASE -> "database_metadata_sapase";
        };
    }

    private String getSchemaMetadataTable(DatabaseType databaseType) {

        return switch (databaseType) {
            case POSTGRES -> "schema_metadata_postgres";
            case ORACLE -> "schema_metadata_oracle";
            case MSSQL -> "schema_metadata_mssql";
            case SAPIQ -> "schema_metadata_sapiq";
            case SAPASE -> "schema_metadata_sapase";
        };
    }

    private String getTableMetadataTable(DatabaseType databaseType) {

        return switch (databaseType) {
            case POSTGRES -> "table_metadata_postgres";
            case ORACLE -> "table_metadata_oracle";
            case MSSQL -> "table_metadata_mssql";
            case SAPIQ -> "table_metadata_sapiq";
            case SAPASE -> "table_metadata_sapase";
        };
    }

}
