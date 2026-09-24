package com.gpb.replication.stream.mssql;

import java.sql.Connection;
import java.sql.ResultSet;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gpb.replication.dto.DatabaseReplicationContext;
import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.stream.AbstractMetadataCopyStreamer;
import com.gpb.replication.stream.PostgresCopyCsvEncoder;
import com.gpb.replication.stream.SourceSqlBinder;
import com.gpb.replication.utils.MetadataFqn;
import com.gpb.replication.utils.MetadataHash;

@Component
public class MssqlMetadataCopyStreamer
        extends AbstractMetadataCopyStreamer {

    private final int fetchSize;
    private final int queryTimeoutSeconds;

    public MssqlMetadataCopyStreamer(
            @Value("${replication.mssql.fetch-size:5000}")
            int fetchSize,
            @Value("${replication.mssql.query-timeout-seconds:0}")
            int queryTimeoutSeconds) {

        this.fetchSize = fetchSize;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    @Override
    protected int getFetchSize() {
        return fetchSize;
    }

    @Override
    protected int getQueryTimeoutSeconds() {
        return queryTimeoutSeconds;
    }

    public long streamDatabases(
            Connection sourceConnection,
            Connection targetConnection,
            String sourceSql,
            String serviceName) {

        return copyDatabases(
                sourceConnection,
                targetConnection,
                DatabaseType.MSSQL,
                sourceSql,
                SourceSqlBinder.NONE,
                rs -> serializeDatabase(rs, serviceName)
        );
    }

    public long streamSchemas(
            Connection sourceConnection,
            Connection targetConnection,
            String sourceSql,
            String serviceName,
            DatabaseReplicationContext database) {

        return copySchemas(
                sourceConnection,
                targetConnection,
                DatabaseType.MSSQL,
                sourceSql,
                SourceSqlBinder.NONE,
                rs -> serializeSchema(
                        rs,
                        serviceName,
                        database
                )
        );
    }

    public long streamTables(
            Connection sourceConnection,
            Connection targetConnection,
            String sourceSql,
            String serviceName,
            DatabaseReplicationContext database) {

        return copyTables(
                sourceConnection,
                targetConnection,
                DatabaseType.MSSQL,
                sourceSql,
                SourceSqlBinder.NONE,
                rs -> serializeTable(
                        rs,
                        serviceName,
                        database
                )
        );
    }

    private byte[] serializeDatabase(
            ResultSet rs,
            String serviceName)
            throws Exception {

        long id = requiredLong(rs, "OID");
        String databaseName = requiredString(rs, "DATNAME");
        String fqn = MetadataFqn.database(serviceName, databaseName);
        String hash = MetadataHash.sha256(databaseName);

        return PostgresCopyCsvEncoder.encode(
                id,
                fqn,
                serviceName,
                databaseName,
                hash
        );
    }

    private byte[] serializeSchema(
            ResultSet rs,
            String serviceName,
            DatabaseReplicationContext database)
            throws Exception {

        long id = requiredLong(rs, "OID");
        String schemaName = requiredString(rs, "SCHEMA_NAME");

        String fqn = MetadataFqn.schema(
                serviceName,
                database.databaseName(),
                schemaName
        );

        String hash = MetadataHash.sha256(schemaName);

        return PostgresCopyCsvEncoder.encode(
                id,
                fqn,
                serviceName,
                database.databaseName(),
                schemaName,
                database.databaseFqn(),
                hash
        );
    }

    private byte[] serializeTable(
            ResultSet rs,
            String serviceName,
            DatabaseReplicationContext database)
            throws Exception {

        long id = requiredLong(rs, "OID");
        String schemaName = requiredString(rs, "SCHEMA_NAME");
        String tableName = requiredString(rs, "TABLE_NAME");
        String description = rs.getString("DESCRIPTION");
        String data = requiredString(rs, "TABLE_STRUCTURE");

        String fqn = MetadataFqn.table(
                serviceName,
                database.databaseName(),
                schemaName,
                tableName
        );

        String parentFqn = MetadataFqn.schema(
                serviceName,
                database.databaseName(),
                schemaName
        );

        String hash = MetadataHash.sha256(description, data);

        return PostgresCopyCsvEncoder.encode(
                id,
                fqn,
                serviceName,
                database.databaseName(),
                schemaName,
                description,
                tableName,
                parentFqn,
                data,
                hash
        );
    }

    private long requiredLong(
            ResultSet rs,
            String column)
            throws Exception {

        Object value = rs.getObject(column);

        if (value == null) {
            throw new IllegalStateException(
                    "Required column is NULL: " + column
            );
        }

        if (value instanceof Number number) {
            return number.longValue();
        }

        return Long.parseLong(value.toString());
    }

    private String requiredString(
            ResultSet rs,
            String column)
            throws Exception {

        String value = rs.getString(column);

        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Required column is empty: " + column
            );
        }

        return value;
    }
}
