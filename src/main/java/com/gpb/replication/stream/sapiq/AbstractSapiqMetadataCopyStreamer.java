package com.gpb.replication.stream.sapiq;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.replication.dto.DatabaseReplicationContext;
import com.gpb.replication.exclusion.MetadataExclusionRules;
import com.gpb.replication.exceptions.MetadataReplicationException;
import com.gpb.replication.stream.PostgresCopyCsvEncoder;
import com.gpb.replication.utils.MetadataFqn;
import com.gpb.replication.utils.MetadataHash;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSapiqMetadataCopyStreamer {

    private static final String DATABASE_COPY_SQL = """
            COPY metadata_replication.database_metadata_sapiq
            (
                id,
                fqn,
                service_name,
                name,
                hash_data
            )
            FROM STDIN
            WITH
            (
                FORMAT CSV,
                DELIMITER E'\\t',
                NULL '\\N',
                QUOTE '"',
                ESCAPE '"',
                ENCODING 'UTF8'
            )
            """;

    private static final String SCHEMA_COPY_SQL = """
            COPY metadata_replication.schema_metadata_sapiq
            (
                id,
                fqn,
                service_name,
                db_name,
                name,
                parent_fqn,
                hash_data
            )
            FROM STDIN
            WITH
            (
                FORMAT CSV,
                DELIMITER E'\\t',
                NULL '\\N',
                QUOTE '"',
                ESCAPE '"',
                ENCODING 'UTF8'
            )
            """;

    private static final String TABLE_COPY_SQL = """
            COPY metadata_replication.table_metadata_sapiq
            (
                id,
                fqn,
                service_name,
                db_name,
                schema_name,
                description,
                name,
                parent_fqn,
                data,
                hash_data
            )
            FROM STDIN
            WITH
            (
                FORMAT CSV,
                DELIMITER E'\\t',
                NULL '\\N',
                QUOTE '"',
                ESCAPE '"',
                ENCODING 'UTF8'
            )
            """;

    protected final ObjectMapper objectMapper;
    private final int fetchSize;
    private final int queryTimeoutSeconds;

    protected AbstractSapiqMetadataCopyStreamer(
            ObjectMapper objectMapper,
            int fetchSize,
            int queryTimeoutSeconds) {

        this.objectMapper = objectMapper;
        this.fetchSize = fetchSize;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    public DatabaseEntry loadDatabase(
            Connection connection,
            String sql) {

        long started = System.nanoTime();

        try (
                PreparedStatement statement = prepare(connection, sql);
                ResultSet rs = statement.executeQuery()
        ) {
            if (!rs.next()) {
                throw new MetadataReplicationException(
                        "SAP IQ database metadata not found"
                );
            }

            DatabaseEntry result = new DatabaseEntry(
                    requiredLong(rs, "ID"),
                    requiredString(rs, "DB_NAME")
            );

            if (rs.next()) {
                throw new MetadataReplicationException(
                        "More than one SAP IQ database returned for one connection"
                );
            }

            log.info(
                    "SAP IQ database discovered. database={}, elapsedMs={}",
                    result.name(),
                    elapsedMs(started)
            );

            return result;

        } catch (Exception e) {
            throw wrap("DATABASE_DISCOVERY", e);
        }
    }

    public List<SchemaEntry> loadSchemas(
            Connection connection,
            String sql,
            MetadataExclusionRules exclusionRules) {

        long started = System.nanoTime();

        long rows = 0;
        long excluded = 0;
        long invalid = 0;

        List<SchemaEntry> result = new ArrayList<>();

        try (
                PreparedStatement statement = prepare(connection, sql);
                ResultSet rs = statement.executeQuery()
        ) {
            while (rs.next()) {
                rows++;

                long id = requiredLong(rs, "OID");
                String schemaName =
                        trimToNull(
                                rs.getString("SCHEMA_NAME")
                        );

                if (schemaName == null) {
                    invalid++;

                    log.warn(
                            "Skipping SAP IQ schema with empty name. oid={}",
                            id
                    );

                    continue;
                }

                if (exclusionRules.isSchemaExcluded(schemaName)) {
                    excluded++;

                    log.debug(
                            "SAP IQ schema excluded by rule. schema={}",
                            schemaName
                    );

                    continue;
                }

                result.add(
                        new SchemaEntry(
                                id,
                                schemaName
                        )
                );
            }

            log.info(
                    "SAP IQ schemas loaded. "
                            + "rows={}, included={}, excluded={}, invalid={}, elapsedMs={}",
                    rows,
                    result.size(),
                    excluded,
                    invalid,
                    elapsedMs(started)
            );

            return List.copyOf(result);

        } catch (Exception e) {
            throw wrap("SCHEMAS", e);
        }
    }

    /**
     * STANDARD object snapshot. Uses existing sapiq/table.sql and therefore
     * reads VIEW_DEFINITION in the same pass.
     */
    public Snapshot loadStandardObjects(
            Connection connection,
            String sql,
            MetadataExclusionRules exclusionRules) {

        long started = System.nanoTime();

        long rows = 0;
        long excludedBySchema = 0;
        long excludedByTable = 0;
        long invalid = 0;

        LinkedHashMap<Long, ObjectMetadata> objects =
                new LinkedHashMap<>();

        try (
                PreparedStatement statement = prepare(connection, sql);
                ResultSet rs = statement.executeQuery()
        ) {
            while (rs.next()) {
                rows++;

                long tableId = requiredLong(rs, "TABLE_ID");
                long id = requiredLong(rs, "OID");

                String schemaName =
                        trimToNull(
                                rs.getString("SCHEMA_NAME")
                        );

                String tableName =
                        trimToNull(
                                rs.getString("TABLE_NAME")
                        );

                if (schemaName == null || tableName == null) {
                    invalid++;

                    log.warn(
                            "Skipping SAP IQ object with empty schema/table name. "
                                    + "tableId={}, schema={}, table={}",
                            tableId,
                            schemaName,
                            tableName
                    );

                    continue;
                }

                /*
                 * IMPORTANT:
                 * Сначала проверяем schema, затем table.
                 *
                 * Это позволяет:
                 * - не выполнять table regex для уже исключённой схемы;
                 * - сохранять одинаковую семантику STANDARD/HYBRID.
                 */
                if (exclusionRules.isSchemaExcluded(schemaName)) {
                    excludedBySchema++;
                    continue;
                }

                if (exclusionRules.isTableExcluded(tableName)) {
                    excludedByTable++;
                    continue;
                }

                String tableType =
                        normalizeTableType(
                                rs.getString("TABLE_TYPE")
                        );

                String description =
                        rs.getString("DESCRIPTION");

                String viewDefinition =
                        trimToNull(
                                rs.getString("VIEW_DEFINITION")
                        );

                ObjectMetadata metadata =
                        new ObjectMetadata(
                                tableId,
                                id,
                                schemaName,
                                tableName,
                                tableType,
                                description,
                                viewDefinition
                        );

                ObjectMetadata previous =
                        objects.putIfAbsent(
                                tableId,
                                metadata
                        );

                if (previous != null) {
                    throw duplicateObject(
                            tableId,
                            previous,
                            metadata
                    );
                }
            }

            Snapshot snapshot =
                    new Snapshot(
                            new ArrayList<>(
                                    objects.values()
                            ),
                            Map.copyOf(objects)
                    );

            log.info(
                    "SAP IQ STANDARD object catalog loaded. "
                            + "rows={}, objects={}, excludedBySchema={}, "
                            + "excludedByTable={}, invalid={}, elapsedMs={}",
                    rows,
                    snapshot.size(),
                    excludedBySchema,
                    excludedByTable,
                    invalid,
                    elapsedMs(started)
            );

            return snapshot;

        } catch (Exception e) {
            throw wrap(
                    "STANDARD_OBJECTS",
                    e
            );
        }
    }

    /**
     * HYBRID object snapshot. View source is deliberately not fetched here.
     */
    public Snapshot loadObjects(
            Connection connection,
            String sql,
            MetadataExclusionRules exclusionRules) {

        long started = System.nanoTime();

        long rows = 0;
        long excludedBySchema = 0;
        long excludedByTable = 0;
        long invalid = 0;

        LinkedHashMap<Long, ObjectMetadata> objects =
                new LinkedHashMap<>();

        try (
                PreparedStatement statement = prepare(connection, sql);
                ResultSet rs = statement.executeQuery()
        ) {
            while (rs.next()) {
                rows++;

                long tableId = requiredLong(rs, "TABLE_ID");
                long id = requiredLong(rs, "ID");

                String schemaName =
                        trimToNull(
                                rs.getString("SCHEMA_NAME")
                        );

                String tableName =
                        trimToNull(
                                rs.getString("TABLE_NAME")
                        );

                if (schemaName == null || tableName == null) {
                    invalid++;

                    log.warn(
                            "Skipping SAP IQ object with empty schema/table name. "
                                    + "tableId={}, schema={}, table={}",
                            tableId,
                            schemaName,
                            tableName
                    );

                    continue;
                }

                /*
                 * Regex выполняются только при построении object snapshot.
                 *
                 * Detail-workers (columns/constraints/views) повторно regex
                 * не выполняют. Для excluded table snapshot.find(tableId)
                 * вернёт null, и строка будет дешево пропущена.
                 */
                if (exclusionRules.isSchemaExcluded(schemaName)) {
                    excludedBySchema++;
                    continue;
                }

                if (exclusionRules.isTableExcluded(tableName)) {
                    excludedByTable++;
                    continue;
                }

                String tableType =
                        normalizeTableType(
                                rs.getString("TABLE_TYPE")
                        );

                String description =
                        rs.getString("DESCRIPTION");

                ObjectMetadata metadata =
                        new ObjectMetadata(
                                tableId,
                                id,
                                schemaName,
                                tableName,
                                tableType,
                                description,
                                null
                        );

                ObjectMetadata previous =
                        objects.putIfAbsent(
                                tableId,
                                metadata
                        );

                if (previous != null) {
                    throw duplicateObject(
                            tableId,
                            previous,
                            metadata
                    );
                }
            }

            Snapshot snapshot =
                    new Snapshot(
                            new ArrayList<>(
                                    objects.values()
                            ),
                            Map.copyOf(objects)
                    );

            log.info(
                    "SAP IQ HYBRID object catalog loaded. "
                            + "rows={}, objects={}, excludedBySchema={}, "
                            + "excludedByTable={}, invalid={}, elapsedMs={}",
                    rows,
                    snapshot.size(),
                    excludedBySchema,
                    excludedByTable,
                    invalid,
                    elapsedMs(started)
            );

            return snapshot;

        } catch (Exception e) {
            throw wrap(
                    "OBJECTS",
                    e
            );
        }
    }

    public LoadResult loadColumns(
            Connection connection,
            String sql,
            Snapshot snapshot,
            String serviceName,
            DatabaseReplicationContext database) {

        return loadColumnsInternal(
                connection,
                sql,
                snapshot,
                serviceName,
                database,
                null,
                null
        );
    }

    public LoadResult loadColumns(
            Connection connection,
            String sql,
            Snapshot snapshot,
            String serviceName,
            DatabaseReplicationContext database,
            int shard,
            int shardCount) {

        return loadColumnsInternal(
                connection,
                sql,
                snapshot,
                serviceName,
                database,
                shard,
                shardCount
        );
    }

    private LoadResult loadColumnsInternal(
            Connection connection,
            String sql,
            Snapshot snapshot,
            String serviceName,
            DatabaseReplicationContext database,
            Integer shard,
            Integer shardCount) {

        long started = System.nanoTime();
        long rows = 0;
        long applied = 0;
        long skipped = 0;

        Long currentTableId = null;
        ObjectMetadata currentObject = null;
        ArrayNode currentColumns = null;

        String stage = shard == null
                ? "COLUMNS"
                : "COLUMNS[" + shard + "/" + shardCount + "]";

        try (
                PreparedStatement statement = prepare(connection, sql)
        ) {
            if (shard != null) {
                statement.setInt(1, shardCount);
                statement.setInt(2, shard);
            }

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    rows++;

                    long tableId = requiredLong(rs, "TABLE_ID");

                    if (currentTableId == null || tableId != currentTableId) {
                        flushColumns(currentObject, currentColumns);

                        currentTableId = tableId;
                        currentObject = snapshot.find(tableId);
                        currentColumns = currentObject != null
                                ? objectMapper.createArrayNode()
                                : null;
                    }

                    if (currentObject == null) {
                        skipped++;
                        continue;
                    }

                    int ordinalPosition = rs.getInt("ORDINAL_POSITION");

                    String columnName = trimToNull(
                            rs.getString("COLUMN_NAME")
                    );

                    if (columnName == null) {
                        skipped++;

                        log.warn(
                                "Skipping SAP IQ column with empty name. "
                                        + "stage={}, tableId={}, schema={}, table={}, "
                                        + "ordinalPosition={}",
                                stage,
                                tableId,
                                currentObject.schemaName,
                                currentObject.tableName,
                                ordinalPosition
                        );

                        continue;
                    }

                    String dataType = trimToNull(
                            rs.getString("DATA_TYPE")
                    );
                    String dataTypeDisplay = trimToNull(
                            rs.getString("DATA_TYPE_DISPLAY")
                    );
                    Integer dataLength = getInteger(rs, "DATA_LENGTH");
                    String columnConstraint = trimToNull(
                            rs.getString("COLUMN_CONSTRAINT")
                    );
                    String description = rs.getString("DESCRIPTION");

                    ObjectNode column = objectMapper.createObjectNode();

                    /*
                     * Field order intentionally matches the legacy SAP IQ
                     * replication service to keep generated JSON stable.
                     */
                    column.put("ordinalPosition", ordinalPosition);
                    column.put(
                            "fqn",
                            MetadataFqn.table(
                                    serviceName,
                                    database.databaseName(),
                                    currentObject.schemaName,
                                    currentObject.tableName
                            ) + "." + columnName
                    );
                    column.put("name", columnName);

                    if (dataType == null) {
                        column.putNull("dataType");
                    } else {
                        column.put("dataType", dataType);
                    }

                    if (dataTypeDisplay == null) {
                        column.putNull("dataTypeDisplay");
                    } else {
                        column.put("dataTypeDisplay", dataTypeDisplay);
                    }

                    if (dataLength == null) {
                        column.putNull("dataLength");
                    } else {
                        column.put("dataLength", dataLength);
                    }

                    if (columnConstraint == null) {
                        column.putNull("constraint");
                    } else {
                        column.put("constraint", columnConstraint);
                    }

                    if (description == null) {
                        column.putNull("description");
                    } else {
                        column.put("description", description);
                    }

                    currentColumns.add(column);
                    applied++;
                }
            }

            flushColumns(currentObject, currentColumns);

            return logResult(
                    new LoadResult(stage, rows, applied, skipped),
                    started
            );

        } catch (Exception e) {
            throw wrap(stage, e);
        }
    }

    public LoadResult loadConstraints(
            Connection connection,
            String sql,
            Snapshot snapshot) {

        long started = System.nanoTime();
        long rows = 0;
        long applied = 0;
        long skipped = 0;

        LinkedHashMap<ConstraintKey, ConstraintBuilder> builders =
                new LinkedHashMap<>();

        try (
                PreparedStatement statement = prepare(connection, sql);
                ResultSet rs = statement.executeQuery()
        ) {
            while (rs.next()) {
                rows++;

                long tableId = requiredLong(rs, "TABLE_ID");
                long indexId = requiredLong(rs, "INDEX_ID");

                ObjectMetadata object = snapshot.find(tableId);
                if (object == null) {
                    skipped++;
                    continue;
                }

                String constraintType = trimToNull(
                        rs.getString("CONSTRAINT_TYPE")
                );
                String columnName = trimToNull(
                        rs.getString("COLUMN_NAME")
                );

                ConstraintKey key = new ConstraintKey(
                        tableId,
                        indexId
                );

                ConstraintBuilder builder =
                        builders.computeIfAbsent(
                                key,
                                ignored -> new ConstraintBuilder(
                                        tableId,
                                        constraintType
                                )
                        );

                if (columnName != null) {
                    builder.columns.add(columnName);
                }

                applied++;
            }

            /*
             * The legacy service de-duplicates constraints by
             * (tableId, normalized type, ordered columns).
             */
            LinkedHashMap<ConstraintDedupKey, ConstraintValue> deduplicated =
                    new LinkedHashMap<>();

            for (ConstraintBuilder builder : builders.values()) {
                String constraintType =
                        trimToNull(builder.constraintType);

                if (constraintType == null) {
                    constraintType = "OTHER";
                }

                List<String> columns =
                        builder.columns.stream()
                                .map(this::trimToNull)
                                .filter(column ->
                                        column != null
                                                && !column.isBlank())
                                .toList();

                ConstraintDedupKey dedupKey =
                        new ConstraintDedupKey(
                                builder.tableId,
                                constraintType,
                                columns
                        );

                deduplicated.putIfAbsent(
                        dedupKey,
                        new ConstraintValue(
                                builder.tableId,
                                constraintType,
                                columns
                        )
                );
            }

            LinkedHashMap<Long, ArrayNode> constraintsByTable =
                    new LinkedHashMap<>();

            for (ConstraintValue constraint : deduplicated.values()) {
                ObjectMetadata object =
                        snapshot.find(constraint.tableId());

                if (object == null) {
                    continue;
                }

                ArrayNode constraints =
                        constraintsByTable.computeIfAbsent(
                                constraint.tableId(),
                                ignored -> objectMapper.createArrayNode()
                        );

                ObjectNode constraintNode =
                        constraints.addObject();

                ArrayNode constraintColumns =
                        constraintNode.putArray("columns");

                for (String columnName : constraint.columns()) {
                    constraintColumns.add(columnName);
                }

                constraintNode.put(
                        "constraintType",
                        constraint.constraintType()
                );
            }

            for (Map.Entry<Long, ArrayNode> entry
                    : constraintsByTable.entrySet()) {

                ObjectMetadata object =
                        snapshot.find(entry.getKey());

                flushConstraints(
                        object,
                        entry.getValue()
                );
            }

            return logResult(
                    new LoadResult(
                            "CONSTRAINTS",
                            rows,
                            applied,
                            skipped
                    ),
                    started
            );

        } catch (Exception e) {
            throw wrap("CONSTRAINTS", e);
        }
    }

    public LoadResult loadViews(
            Connection connection,
            String sql,
            Snapshot snapshot) {

        long started = System.nanoTime();
        long rows = 0;
        long applied = 0;
        long skipped = 0;

        try (
                PreparedStatement statement = prepare(connection, sql);
                ResultSet rs = statement.executeQuery()
        ) {
            while (rs.next()) {
                rows++;

                long tableId = requiredLong(rs, "TABLE_ID");
                ObjectMetadata object = snapshot.find(tableId);

                if (object == null
                        || !("VIEW".equals(object.tableType)
                        || "MATERIALIZED_VIEW".equals(object.tableType))) {
                    skipped++;
                    continue;
                }

                object.viewDefinition = trimToNull(
                        rs.getString("VIEW_DEFINITION")
                );
                applied++;
            }

            return logResult(
                    new LoadResult("VIEWS", rows, applied, skipped),
                    started
            );

        } catch (Exception e) {
            throw wrap("VIEWS", e);
        }
    }

    public long copyDatabase(
            Connection targetConnection,
            DatabaseEntry database,
            String serviceName) {

        return copy(
                targetConnection,
                DATABASE_COPY_SQL,
                "DATABASE_COPY",
                List.of(database),
                entry -> PostgresCopyCsvEncoder.encode(
                        entry.id(),
                        MetadataFqn.database(serviceName, entry.name()),
                        serviceName,
                        entry.name(),
                        MetadataHash.sha256(entry.name())
                )
        );
    }

    public long copySchemas(
            Connection targetConnection,
            List<SchemaEntry> schemas,
            String serviceName,
            DatabaseReplicationContext database) {

        return copy(
                targetConnection,
                SCHEMA_COPY_SQL,
                "SCHEMA_COPY",
                schemas,
                entry -> PostgresCopyCsvEncoder.encode(
                        entry.id(),
                        MetadataFqn.schema(
                                serviceName,
                                database.databaseName(),
                                entry.name()
                        ),
                        serviceName,
                        database.databaseName(),
                        entry.name(),
                        database.databaseFqn(),
                        MetadataHash.sha256(entry.name())
                )
        );
    }

    public long copyTables(
            Connection targetConnection,
            Snapshot snapshot,
            String serviceName,
            DatabaseReplicationContext database) {

        return copy(
                targetConnection,
                TABLE_COPY_SQL,
                "TABLE_COPY",
                snapshot.objects,
                object -> serializeTableMetadata(
                        object,
                        serviceName,
                        database
                )
        );
    }

    private byte[] serializeTableMetadata(
            ObjectMetadata object,
            String serviceName,
            DatabaseReplicationContext database)
            throws Exception {

        String fqn = MetadataFqn.table(
                serviceName,
                database.databaseName(),
                object.schemaName,
                object.tableName
        );

        String parentFqn = MetadataFqn.schema(
                serviceName,
                database.databaseName(),
                object.schemaName
        );

        ObjectNode data = objectMapper.createObjectNode();

        String tableType = normalizeTableType(object.tableType);
        data.put("tableType", tableType);

        String viewDefinition = trimToNull(object.viewDefinition);

        if (viewDefinition == null) {
            data.putNull("viewDefinition");
        } else {
            data.put("viewDefinition", viewDefinition);
        }

        data.set(
                "columns",
                objectMapper.readTree(object.columnsJson)
        );
        data.set(
                "tableConstraints",
                objectMapper.readTree(object.constraintsJson)
        );

        String dataJson = objectMapper.writeValueAsString(data);
        String hash = MetadataHash.sha256(object.description, dataJson);

        return PostgresCopyCsvEncoder.encode(
                object.id,
                fqn,
                serviceName,
                database.databaseName(),
                object.schemaName,
                object.description,
                object.tableName,
                parentFqn,
                dataJson,
                hash
        );
    }

    private <T> long copy(
            Connection targetConnection,
            String copySql,
            String stage,
            Iterable<T> values,
            RowEncoder<T> encoder) {

        long started = System.nanoTime();

        try {
            PGConnection pgConnection = targetConnection.unwrap(PGConnection.class);
            CopyIn copyIn = pgConnection.getCopyAPI().copyIn(copySql);
            long streamedRows = 0;

            try {
                for (T value : values) {
                    byte[] row = encoder.encode(value);
                    copyIn.writeToCopy(row, 0, row.length);
                    streamedRows++;
                }

                long copiedRows = copyIn.endCopy();

                log.info(
                        "SAP IQ {} completed. streamedRows={}, copiedRows={}, elapsedMs={}",
                        stage,
                        streamedRows,
                        copiedRows,
                        elapsedMs(started)
                );

                return copiedRows >= 0 ? copiedRows : streamedRows;

            } catch (Exception e) {
                try {
                    copyIn.cancelCopy();
                } catch (SQLException cancelException) {
                    e.addSuppressed(cancelException);
                }
                throw e;
            }

        } catch (Exception e) {
            throw wrap(stage, e);
        }
    }

    private void flushColumns(
            ObjectMetadata object,
            ArrayNode columns)
            throws Exception {

        if (object != null && columns != null) {
            object.columnsJson = objectMapper.writeValueAsString(columns);
        }
    }

    private void flushConstraints(
            ObjectMetadata object,
            ArrayNode constraints)
            throws Exception {

        if (object != null && constraints != null) {
            object.constraintsJson = objectMapper.writeValueAsString(constraints);
        }
    }

    protected PreparedStatement prepare(
            Connection connection,
            String sql)
            throws SQLException {

        PreparedStatement statement = connection.prepareStatement(
                sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY
        );

        statement.setFetchSize(fetchSize);

        try {
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        } catch (SQLException e) {
            log.debug("SAP IQ driver does not support fetch direction", e);
        }

        if (queryTimeoutSeconds > 0) {
            statement.setQueryTimeout(queryTimeoutSeconds);
        }

        return statement;
    }

    private long requiredLong(
            ResultSet rs,
            String column)
            throws SQLException {

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

    private Integer getInteger(
            ResultSet rs,
            String column)
            throws SQLException {

        Object value = rs.getObject(column);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.valueOf(value.toString());
    }

    private String requiredString(
            ResultSet rs,
            String column)
            throws SQLException {

        String value = trimToNull(
                rs.getString(column)
        );

        if (value == null) {
            throw new IllegalStateException(
                    "Required column is empty: " + column
            );
        }

        return value;
    }

    private String trimToNull(
            String value) {

        if (value == null) {
            return null;
        }

        String trimmed = value.trim();

        return trimmed.isEmpty()
                ? null
                : trimmed;
    }

    private String normalizeTableType(
            String tableType) {

        String normalized = trimToNull(tableType);

        return normalized == null
                ? "OTHER"
                : normalized;
    }

    private LoadResult logResult(
            LoadResult result,
            long started) {

        log.info(
                "SAP IQ {} loaded. rows={}, applied={}, skipped={}, elapsedMs={}",
                result.stage(),
                result.rows(),
                result.applied(),
                result.skipped(),
                elapsedMs(started)
        );
        return result;
    }

    private MetadataReplicationException duplicateObject(
            long tableId,
            ObjectMetadata previous,
            ObjectMetadata current) {

        return new MetadataReplicationException(
                "Duplicate SAP IQ metadata object. tableId=" + tableId
                        + ", existing=" + previous.schemaName + "." + previous.tableName
                        + ", new=" + current.schemaName + "." + current.tableName
        );
    }

    protected MetadataReplicationException wrap(
            String stage,
            Exception e) {

        if (e instanceof MetadataReplicationException mre) {
            return mre;
        }
        return new MetadataReplicationException(
                "SAP IQ metadata stage failed: " + stage,
                e
        );
    }

    protected long elapsedMs(long started) {
        return (System.nanoTime() - started) / 1_000_000;
    }

    @FunctionalInterface
    private interface RowEncoder<T> {
        byte[] encode(T value) throws Exception;
    }

    private record ConstraintKey(
            long tableId,
            long indexId) {
    }

    private record ConstraintDedupKey(
            long tableId,
            String constraintType,
            List<String> columns) {
    }

    private record ConstraintValue(
            long tableId,
            String constraintType,
            List<String> columns) {
    }

    private static final class ConstraintBuilder {

        private final long tableId;
        private final String constraintType;
        private final List<String> columns = new ArrayList<>();

        private ConstraintBuilder(
                long tableId,
                String constraintType) {

            this.tableId = tableId;
            this.constraintType = constraintType;
        }
    }

    private static final class ObjectMetadata {
        private final long tableId;
        private final long id;
        private final String schemaName;
        private final String tableName;
        private final String tableType;
        private final String description;

        private volatile String columnsJson = "[]";
        private volatile String constraintsJson = "[]";
        private volatile String viewDefinition;

        private ObjectMetadata(
                long tableId,
                long id,
                String schemaName,
                String tableName,
                String tableType,
                String description,
                String viewDefinition) {

            this.tableId = tableId;
            this.id = id;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.tableType = tableType;
            this.description = description;
            this.viewDefinition = viewDefinition;
        }
    }

    public static final class Snapshot {
        private final List<ObjectMetadata> objects;
        private final Map<Long, ObjectMetadata> byTableId;

        private Snapshot(
                List<ObjectMetadata> objects,
                Map<Long, ObjectMetadata> byTableId) {

            this.objects = List.copyOf(objects);
            this.byTableId = byTableId;
        }

        private ObjectMetadata find(long tableId) {
            return byTableId.get(tableId);
        }

        public int size() {
            return objects.size();
        }

        public SnapshotSummary summary() {
            long tables = 0;
            long views = 0;
            long materializedViews = 0;
            long viewsWithoutDefinition = 0;

            for (ObjectMetadata object : objects) {
                switch (object.tableType) {
                    case "REGULAR" -> tables++;
                    case "VIEW" -> {
                        views++;
                        if (object.viewDefinition == null) {
                            viewsWithoutDefinition++;
                        }
                    }
                    case "MATERIALIZED_VIEW" -> {
                        materializedViews++;
                        if (object.viewDefinition == null) {
                            viewsWithoutDefinition++;
                        }
                    }
                    default -> {
                    }
                }
            }

            return new SnapshotSummary(
                    objects.size(),
                    tables,
                    views,
                    materializedViews,
                    viewsWithoutDefinition
            );
        }
    }

    public record DatabaseEntry(
            long id,
            String name) {
    }

    public record SchemaEntry(
            long id,
            String name) {
    }

    public record SnapshotSummary(
            long total,
            long tables,
            long views,
            long materializedViews,
            long viewsWithoutDefinition) {
    }

    public record LoadResult(
            String stage,
            long rows,
            long applied,
            long skipped) {
    }
}
