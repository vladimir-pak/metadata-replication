package com.gpb.replication.stream.oracle;

import java.sql.Connection;
import java.sql.ResultSet;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.gpb.replication.dto.DatabaseReplicationContext;
import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.stream.AbstractMetadataCopyStreamer;
import com.gpb.replication.stream.PostgresCopyCsvEncoder;
import com.gpb.replication.stream.SourceSqlBinder;
import com.gpb.replication.utils.MetadataFqn;
import com.gpb.replication.utils.MetadataHash;

@Component
public class OracleMetadataCopyStreamer
        extends AbstractMetadataCopyStreamer {

    private final ObjectMapper objectMapper;

    private final int fetchSize;

    private final int queryTimeoutSeconds;

    public OracleMetadataCopyStreamer(
            ObjectMapper objectMapper,
            @Value("${replication.oracle.fetch-size:5000}")
            int fetchSize,
            @Value("${replication.oracle.query-timeout-seconds:0}")
            int queryTimeoutSeconds) {

        this.objectMapper =
                objectMapper;

        this.fetchSize =
                fetchSize;

        this.queryTimeoutSeconds =
                queryTimeoutSeconds;
    }

    @Override
    protected int getFetchSize() {
        return fetchSize;
    }

    @Override
    protected int getQueryTimeoutSeconds() {
        return queryTimeoutSeconds;
    }

    /*
     * ==========================================================
     * DATABASE
     * ==========================================================
     */

    public long streamDatabases(
            Connection sourceConnection,
            Connection targetConnection,
            String sourceSql,
            String serviceName) {

        return copyDatabases(
                sourceConnection,
                targetConnection,
                DatabaseType.ORACLE,
                sourceSql,
                SourceSqlBinder.NONE,
                rs ->
                        serializeDatabase(
                                rs,
                                serviceName
                        )
        );
    }

    /*
     * ==========================================================
     * SCHEMA
     * ==========================================================
     */

    public long streamSchemas(
            Connection sourceConnection,
            Connection targetConnection,
            String sourceSql,
            String serviceName,
            DatabaseReplicationContext database) {

        return copySchemas(
                sourceConnection,
                targetConnection,
                DatabaseType.ORACLE,
                sourceSql,
                SourceSqlBinder.NONE,
                rs ->
                        serializeSchema(
                                rs,
                                serviceName,
                                database
                        )
        );
    }

    /*
     * ==========================================================
     * REGULAR TABLE
     * ==========================================================
     */

    public long streamTables(
            Connection sourceConnection,
            Connection targetConnection,
            String sourceSql,
            String serviceName,
            DatabaseReplicationContext database) {

        return copyTables(
                sourceConnection,
                targetConnection,
                DatabaseType.ORACLE,
                sourceSql,
                SourceSqlBinder.NONE,
                rs ->
                        serializeTable(
                                rs,
                                serviceName,
                                database
                        )
        );
    }

    /*
     * ==========================================================
     * FAST VIEW
     *
     * view.sql
     *
     * VIEW_DEFINITION = VARCHAR2(4000)
     * ==========================================================
     */

    public long streamViews(
            Connection sourceConnection,
            Connection targetConnection,
            String sourceSql,
            String serviceName,
            DatabaseReplicationContext database) {

        return copyTables(
                sourceConnection,
                targetConnection,
                DatabaseType.ORACLE,
                sourceSql,
                SourceSqlBinder.NONE,
                rs ->
                        serializeView(
                                rs,
                                serviceName,
                                database
                        )
        );
    }

    /*
     * ==========================================================
     * LONG VIEW
     *
     * view_long.sql
     *
     * VIEW_DEFINITION = LONG
     * ==========================================================
     */

    public long streamLongViews(
            Connection sourceConnection,
            Connection targetConnection,
            String sourceSql,
            String serviceName,
            DatabaseReplicationContext database) {

        return copyTables(
                sourceConnection,
                targetConnection,
                DatabaseType.ORACLE,
                sourceSql,
                SourceSqlBinder.NONE,
                rs ->
                        serializeView(
                                rs,
                                serviceName,
                                database
                        )
        );
    }

    /*
     * ==========================================================
     * MATERIALIZED VIEW
     *
     * mview.sql
     *
     * VIEW_DEFINITION = LONG
     * ==========================================================
     */

    public long streamMaterializedViews(
            Connection sourceConnection,
            Connection targetConnection,
            String sourceSql,
            String serviceName,
            DatabaseReplicationContext database) {

        return copyTables(
                sourceConnection,
                targetConnection,
                DatabaseType.ORACLE,
                sourceSql,
                SourceSqlBinder.NONE,
                rs ->
                        serializeView(
                                rs,
                                serviceName,
                                database
                        )
        );
    }

    /*
     * ==========================================================
     * DATABASE SERIALIZER
     * ==========================================================
     */

    private byte[] serializeDatabase(
            ResultSet rs,
            String serviceName)
            throws Exception {

        long id =
                requiredLong(
                        rs,
                        "ID"
                );

        String databaseName =
                requiredString(
                        rs,
                        "DB_NAME"
                );

        String fqn =
                MetadataFqn.database(
                        serviceName,
                        databaseName
                );

        String hash =
                MetadataHash.sha256(
                        databaseName
                );

        return PostgresCopyCsvEncoder.encode(
                id,
                fqn,
                serviceName,
                databaseName,
                hash
        );
    }

    /*
     * ==========================================================
     * SCHEMA SERIALIZER
     * ==========================================================
     */

    private byte[] serializeSchema(
            ResultSet rs,
            String serviceName,
            DatabaseReplicationContext database)
            throws Exception {

        long id =
                requiredLong(
                        rs,
                        "ID"
                );

        String schemaName =
                requiredString(
                        rs,
                        "SCHEMA_NAME"
                );

        String fqn =
                MetadataFqn.schema(
                        serviceName,
                        database.databaseName(),
                        schemaName
                );

        String hash =
                MetadataHash.sha256(
                        schemaName
                );

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

    /*
     * ==========================================================
     * REGULAR TABLE SERIALIZER
     * ==========================================================
     */

    private byte[] serializeTable(
            ResultSet rs,
            String serviceName,
            DatabaseReplicationContext database)
            throws Exception {

        /*
         * Порядок обязан совпадать с table.sql.
         */

        long id =
                requiredLong(
                        rs,
                        "ID"
                );

        String schemaName =
                requiredString(
                        rs,
                        "SCHEMA_NAME"
                );

        String tableName =
                requiredString(
                        rs,
                        "TABLE_NAME"
                );

        String tableType =
                requiredString(
                        rs,
                        "TABLE_TYPE"
                );

        /*
         * Для table.sql это всегда NULL/VARCHAR2,
         * LONG здесь больше нет.
         */
        String viewDefinition =
                rs.getString(
                        "VIEW_DEFINITION"
                );

        String description =
                rs.getString(
                        "DESCRIPTION"
                );

        String columnsJson =
                rs.getString(
                        "COLUMNS_JSON"
                );

        String constraintsJson =
                rs.getString(
                        "TABLE_CONSTRAINTS_JSON"
                );

        return serializeTableMetadata(
                id,
                serviceName,
                database,
                schemaName,
                tableName,
                tableType,
                description,
                viewDefinition,
                columnsJson,
                constraintsJson
        );
    }

    /*
     * ==========================================================
     * VIEW SERIALIZER
     * ==========================================================
     *
     * Используется одновременно для:
     *
     * view.sql
     * view_long.sql
     * mview.sql
     *
     * Все SQL имеют одинаковый порядок columns.
     */

    private byte[] serializeView(
            ResultSet rs,
            String serviceName,
            DatabaseReplicationContext database)
            throws Exception {

        long id =
                requiredLong(
                        rs,
                        "ID"
                );

        String schemaName =
                requiredString(
                        rs,
                        "SCHEMA_NAME"
                );

        String tableName =
                requiredString(
                        rs,
                        "TABLE_NAME"
                );

        String tableType =
                requiredString(
                        rs,
                        "TABLE_TYPE"
                );

        String description =
                rs.getString(
                        "DESCRIPTION"
                );

        String columnsJson =
                rs.getString(
                        "COLUMNS_JSON"
                );

        String constraintsJson =
                rs.getString(
                        "TABLE_CONSTRAINTS_JSON"
                );

        /*
         * ОБЯЗАТЕЛЬНО читаем последней.
         *
         * view.sql:
         *      VARCHAR2
         *
         * view_long.sql / mview.sql:
         *      LONG
         */
        String viewDefinition =
                rs.getString(
                        "VIEW_DEFINITION"
                );

        return serializeTableMetadata(
                id,
                serviceName,
                database,
                schemaName,
                tableName,
                tableType,
                description,
                viewDefinition,
                columnsJson,
                constraintsJson
        );
    }

    /*
     * ==========================================================
     * COMMON TABLE METADATA SERIALIZER
     * ==========================================================
     */

    private byte[] serializeTableMetadata(
            long id,
            String serviceName,
            DatabaseReplicationContext database,
            String schemaName,
            String tableName,
            String tableType,
            String description,
            String viewDefinition,
            String columnsJson,
            String constraintsJson)
            throws Exception {

        String fqn =
                MetadataFqn.table(
                        serviceName,
                        database.databaseName(),
                        schemaName,
                        tableName
                );

        String parentFqn =
                MetadataFqn.schema(
                        serviceName,
                        database.databaseName(),
                        schemaName
                );

        String data =
                buildTableData(
                        tableType,
                        viewDefinition,
                        columnsJson,
                        constraintsJson
                );

        String hash =
                MetadataHash.sha256(
                        description,
                        data
                );

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

    /*
     * ==========================================================
     * DATA JSON
     * ==========================================================
     */

    private String buildTableData(
            String tableType,
            String viewDefinition,
            String columnsJson,
            String constraintsJson)
            throws Exception {

        ObjectNode data =
                objectMapper.createObjectNode();

        if (tableType == null) {
            data.putNull(
                    "tableType"
            );
        } else {
            data.put(
                    "tableType",
                    tableType
            );
        }

        if (viewDefinition == null) {
            data.putNull(
                    "viewDefinition"
            );
        } else {
            data.put(
                    "viewDefinition",
                    viewDefinition
            );
        }

        data.set(
                "columns",
                objectMapper.readTree(
                        columnsJson != null
                                ? columnsJson
                                : "[]"
                )
        );

        data.set(
                "tableConstraints",
                objectMapper.readTree(
                        constraintsJson != null
                                ? constraintsJson
                                : "[]"
                )
        );

        return objectMapper
                .writeValueAsString(
                        data
                );
    }

    /*
     * ==========================================================
     * RESULT SET HELPERS
     * ==========================================================
     */

    private Long getLong(
            ResultSet rs,
            String column)
            throws Exception {

        Object value =
                rs.getObject(
                        column
                );

        if (value == null) {
            return null;
        }

        if (value instanceof Number number) {
            return number.longValue();
        }

        return Long.valueOf(
                value.toString()
        );
    }

    private long requiredLong(
            ResultSet rs,
            String column)
            throws Exception {

        Long value =
                getLong(
                        rs,
                        column
                );

        if (value == null) {

            throw new IllegalStateException(
                    "Required column is NULL: "
                            + column
            );
        }

        return value;
    }

    private String requiredString(
            ResultSet rs,
            String column)
            throws Exception {

        String value =
                rs.getString(
                        column
                );

        if (value == null
                || value.isBlank()) {

            throw new IllegalStateException(
                    "Required column is empty: "
                            + column
            );
        }

        return value;
    }
}