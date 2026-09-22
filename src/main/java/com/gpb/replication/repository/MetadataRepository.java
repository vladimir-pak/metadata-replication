package com.gpb.replication.repository;

import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.replication.dto.DatabaseMetadata;
import com.gpb.replication.dto.SchemaMetadata;
import com.gpb.replication.dto.TableMetadata;
import com.gpb.replication.enums.DatabaseType;

@Repository 
public class MetadataRepository {

    private final JdbcTemplate jdbcTemplate;

    private final static int BATCH_SIZE = 10_000;

    private static final String INSERT_DATABASE_SQL = """
        INSERT INTO metadata_replication.%s
        (
            id,
            fqn,
            service_name,
            name,
            hash_data,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """;

    private static final String INSERT_SCHEMA_SQL = """
            INSERT INTO metadata_replication.%s
                (
                    id,
                    parent_fqn,
                    fqn,
                    service_name,
                    db_name,
                    name,
                    hash_data,
                    created_at
                )
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String INSERT_TABLE_SQL = """
            INSERT INTO metadata_replication.%s
                (
                    id,
                    parent_fqn,
                    fqn,
                    service_name,
                    db_name,
                    schema_name,
                    name,
                    description,
                    data,
                    hash_data,
                    created_at
                )
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

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

    public int[][] saveAllDatabases(
            List<DatabaseMetadata> metadata,
            DatabaseType databaseType) {

        if (metadata == null || metadata.isEmpty()) {
            return new int[0][];
        }

        String sql = INSERT_DATABASE_SQL.formatted(
                getDatabaseMetadataTable(databaseType)
        );

        return jdbcTemplate.batchUpdate(
                sql,
                metadata,
                BATCH_SIZE,
                (ps, item) -> {
                    ps.setLong(1, item.getId());
                    ps.setString(2, item.getFqn());
                    ps.setString(3, item.getServiceName());
                    ps.setString(4, item.getName());
                    ps.setString(5, item.getHashData());
                    ps.setObject(6, item.getCreatedAt());
                }
        );
    }

    public int[][] saveAllSchemas(
            List<SchemaMetadata> metadata,
            DatabaseType databaseType) {

        if (metadata == null || metadata.isEmpty()) {
            return new int[0][];
        }

        String sql = INSERT_SCHEMA_SQL.formatted(
                getSchemaMetadataTable(databaseType)
        );

        return jdbcTemplate.batchUpdate(
                sql,
                metadata,
                BATCH_SIZE,
                (ps, item) -> {
                    ps.setLong(1, item.getId());
                    ps.setString(2, item.getParentFqn());
                    ps.setString(3, item.getFqn());
                    ps.setString(4, item.getServiceName());
                    ps.setString(5, item.getDbName());
                    ps.setString(6, item.getName());
                    ps.setString(7, item.getHashData());
                    ps.setObject(8, item.getCreatedAt());
                }
        );
    }

    public int[][] saveAllTables(
            List<TableMetadata> metadata,
            DatabaseType databaseType) {

        if (metadata == null || metadata.isEmpty()) {
            return new int[0][];
        }

        String sql = INSERT_TABLE_SQL.formatted(
                getTableMetadataTable(databaseType)
        );

        return jdbcTemplate.batchUpdate(
                sql,
                metadata,
                BATCH_SIZE,
                (ps, item) -> {
                    ps.setLong(1, item.getId());
                    ps.setString(2, item.getParentFqn());
                    ps.setString(3, item.getFqn());
                    ps.setString(4, item.getServiceName());
                    ps.setString(5, item.getDbName());
                    ps.setString(6, item.getSchemaName());
                    ps.setString(7, item.getName());
                    ps.setString(8, item.getDescription());
                    ps.setObject(9, item.getData(), java.sql.Types.OTHER);
                    ps.setString(10, item.getHashData());
                    ps.setObject(11, item.getCreatedAt());
                }
        );
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

    private String getDatabaseMetadataTable(DatabaseType databaseType) {

        return switch (databaseType) {
            case POSTGRES -> "database_metadata_postgres";
            case ORACLE -> "database_metadata_oracle";
            case MSSQL -> "database_metadata_mssql";
            case SAPIQ -> "database_metadata_sapiq";
        };
    }

    private String getSchemaMetadataTable(DatabaseType databaseType) {

        return switch (databaseType) {
            case POSTGRES -> "schema_metadata_postgres";
            case ORACLE -> "schema_metadata_oracle";
            case MSSQL -> "schema_metadata_mssql";
            case SAPIQ -> "schema_metadata_sapiq";
        };
    }

    private String getTableMetadataTable(DatabaseType databaseType) {

        return switch (databaseType) {
            case POSTGRES -> "table_metadata_postgres";
            case ORACLE -> "table_metadata_oracle";
            case MSSQL -> "table_metadata_mssql";
            case SAPIQ -> "table_metadata_sapiq";
        };
    }

}
