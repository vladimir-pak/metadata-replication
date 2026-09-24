package com.gpb.replication.stream.mssql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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

@Component
@Slf4j
public class MssqlHybridMetadataCopyStreamer {

    private static final String DATABASE_COPY_SQL = """
            COPY metadata_replication.database_metadata_mssql
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
            COPY metadata_replication.schema_metadata_mssql
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
            COPY metadata_replication.table_metadata_mssql
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

    private final ObjectMapper objectMapper;
    private final int fetchSize;
    private final int queryTimeoutSeconds;

    public MssqlHybridMetadataCopyStreamer(
            ObjectMapper objectMapper,
            @Value("${replication.mssql.hybrid.fetch-size:10000}")
            int fetchSize,
            @Value("${replication.mssql.hybrid.query-timeout-seconds:0}")
            int queryTimeoutSeconds) {

        this.objectMapper = objectMapper;
        this.fetchSize = fetchSize;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    public List<DatabaseEntry> loadDatabases(
            Connection connection,
            String sql) {

        long started = System.nanoTime();
        LinkedHashMap<String, DatabaseEntry> databases = new LinkedHashMap<>();

        try (
                PreparedStatement statement = prepare(connection, sql);
                ResultSet rs = statement.executeQuery()
        ) {
            while (rs.next()) {
                DatabaseEntry entry = new DatabaseEntry(
                        requiredLong(rs, "ID"),
                        requiredString(rs, "DB_NAME")
                );

                DatabaseEntry previous = databases.putIfAbsent(
                        entry.name(),
                        entry
                );

                if (previous != null) {
                    throw new MetadataReplicationException(
                            "Duplicate MSSQL database: " + entry.name()
                    );
                }
            }

            List<DatabaseEntry> result = List.copyOf(databases.values());

            log.info(
                    "MSSQL HYBRID databases discovered. count={}, elapsedMs={}",
                    result.size(),
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

        List<SchemaEntry> result = new ArrayList<>();

        try (
                PreparedStatement statement = prepare(connection, sql);
                ResultSet rs = statement.executeQuery()
        ) {
            while (rs.next()) {
                rows++;

                long id = requiredLong(rs, "ID");
                String schemaName = requiredString(rs, "SCHEMA_NAME");

                if (exclusionRules.isSchemaExcluded(schemaName)) {
                    excluded++;
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
                    "MSSQL HYBRID schemas loaded. "
                            + "rows={}, included={}, excluded={}, elapsedMs={}",
                    rows,
                    result.size(),
                    excluded,
                    elapsedMs(started)
            );

            return List.copyOf(result);

        } catch (Exception e) {
            throw wrap("SCHEMAS", e);
        }
    }

    public Snapshot loadObjects(
            Connection connection,
            String sql,
            MetadataExclusionRules exclusionRules) {

        long started = System.nanoTime();
        long rows = 0;
        long excludedBySchema = 0;
        long excludedByTable = 0;

        LinkedHashMap<ObjectKey, ObjectMetadata> objects =
                new LinkedHashMap<>();

        try (
                PreparedStatement statement = prepare(connection, sql);
                ResultSet rs = statement.executeQuery()
        ) {
            while (rs.next()) {
                rows++;

                long id = requiredLong(rs, "ID");
                String schemaName = requiredString(rs, "SCHEMA_NAME");
                String tableName = requiredString(rs, "TABLE_NAME");

                if (exclusionRules.isSchemaExcluded(schemaName)) {
                    excludedBySchema++;
                    continue;
                }

                if (exclusionRules.isTableExcluded(tableName)) {
                    excludedByTable++;
                    continue;
                }

                String tableType = requiredString(rs, "TABLE_TYPE");
                String rawTableType = requiredString(rs, "RAW_TABLE_TYPE");
                String description = rs.getString("DESCRIPTION");

                ObjectKey key = new ObjectKey(schemaName, tableName);
                ObjectMetadata metadata = new ObjectMetadata(
                        id,
                        schemaName,
                        tableName,
                        tableType,
                        rawTableType,
                        description
                );

                ObjectMetadata previous = objects.putIfAbsent(key, metadata);
                if (previous != null) {
                    throw new MetadataReplicationException(
                            "Duplicate MSSQL metadata object: "
                                    + schemaName + "." + tableName
                                    + ", existingType=" + previous.tableType
                                    + ", newType=" + tableType
                    );
                }
            }

            Snapshot snapshot = new Snapshot(
                    new ArrayList<>(objects.values()),
                    Map.copyOf(objects)
            );

            log.info(
                    "MSSQL HYBRID object catalog loaded. "
                            + "rows={}, objects={}, excludedBySchema={}, "
                            + "excludedByTable={}, elapsedMs={}",
                    rows,
                    snapshot.size(),
                    excludedBySchema,
                    excludedByTable,
                    elapsedMs(started)
            );

            return snapshot;

        } catch (Exception e) {
            throw wrap("OBJECTS", e);
        }
    }

    public LoadResult loadColumns(
            Connection connection,
            String sql,
            Snapshot snapshot,
            String serviceName,
            DatabaseReplicationContext database,
            int shard,
            int shardCount) {

        long started = System.nanoTime();
        long rows = 0;
        long applied = 0;
        long skipped = 0;

        ObjectKey currentKey = null;
        ObjectMetadata currentObject = null;
        ArrayNode currentColumns = null;

        try (
                PreparedStatement statement = prepare(connection, sql)
        ) {
            statement.setInt(1, shardCount);
            statement.setInt(2, shard);

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    rows++;

                    ObjectKey key = new ObjectKey(
                            requiredString(rs, "SCHEMA_NAME"),
                            requiredString(rs, "TABLE_NAME")
                    );

                    if (!key.equals(currentKey)) {
                        flushColumns(currentObject, currentColumns);

                        currentKey = key;
                        currentObject = snapshot.find(key);
                        currentColumns = currentObject != null
                                ? objectMapper.createArrayNode()
                                : null;
                    }

                    if (currentObject == null) {
                        skipped++;
                        continue;
                    }

                    String columnName = requiredString(rs, "COLUMN_NAME");
                    String dataType = requiredString(rs, "DATA_TYPE");
                    Integer dataLength = getInteger(rs, "DATA_LENGTH");
                    Integer precision = getInteger(rs, "DATA_PRECISION");
                    Integer scale = getInteger(rs, "DATA_SCALE");
                    Integer ordinalPosition = getInteger(rs, "ORDINAL_POSITION");
                    boolean nullable = getBoolean(rs, "IS_NULLABLE");
                    String description = rs.getString("DESCRIPTION");

                    ObjectNode column = objectMapper.createObjectNode();
                    column.put("name", columnName);
                    column.put(
                            "fqn",
                            MetadataFqn.table(
                                    serviceName,
                                    database.databaseName(),
                                    currentObject.schemaName,
                                    currentObject.tableName
                            ) + "." + columnName
                    );
                    column.put("dataType", dataType);
                    column.put(
                            "dataTypeDisplay",
                            buildDataTypeDisplay(
                                    dataType,
                                    dataLength,
                                    precision,
                                    scale
                            )
                    );

                    if (dataLength == null) {
                        column.putNull("dataLength");
                    } else {
                        column.put("dataLength", dataLength);
                    }

                    column.put(
                            "description",
                            description != null ? description : ""
                    );

                    if (ordinalPosition == null) {
                        column.putNull("ordinalPosition");
                    } else {
                        column.put("ordinalPosition", ordinalPosition);
                    }

                    column.put(
                            "constraint",
                            nullable ? "NULLABLE" : "NOT_NULL"
                    );

                    currentColumns.add(column);
                    applied++;
                }
            }

            flushColumns(currentObject, currentColumns);

            return logResult(
                    new LoadResult(
                            "COLUMNS[" + shard + "/" + shardCount + "]",
                            rows,
                            applied,
                            skipped
                    ),
                    started
            );

        } catch (Exception e) {
            throw wrap("COLUMNS[" + shard + "]", e);
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

        ObjectKey currentKey = null;
        ObjectMetadata currentObject = null;
        ArrayNode currentConstraints = null;

        String currentConstraintName = null;
        ObjectNode currentConstraint = null;
        ArrayNode currentConstraintColumns = null;

        try (
                PreparedStatement statement = prepare(connection, sql);
                ResultSet rs = statement.executeQuery()
        ) {
            while (rs.next()) {
                rows++;

                ObjectKey key = new ObjectKey(
                        requiredString(rs, "SCHEMA_NAME"),
                        requiredString(rs, "TABLE_NAME")
                );

                String constraintName = requiredString(rs, "CONSTRAINT_NAME");

                if (!key.equals(currentKey)) {
                    if (currentConstraint != null && currentConstraints != null) {
                        normalizeConstraintColumns(
                                currentConstraint,
                                currentConstraintColumns
                        );
                        currentConstraints.add(currentConstraint);
                    }
                    flushConstraints(currentObject, currentConstraints);

                    currentKey = key;
                    currentObject = snapshot.find(key);

                    if (currentObject != null
                            && "REGULAR".equals(currentObject.tableType)) {
                        currentConstraints = objectMapper.createArrayNode();
                    } else {
                        currentConstraints = null;
                    }

                    currentConstraintName = null;
                    currentConstraint = null;
                    currentConstraintColumns = null;
                }

                if (currentObject == null
                        || !"REGULAR".equals(currentObject.tableType)) {
                    skipped++;
                    continue;
                }

                if (!constraintName.equals(currentConstraintName)) {
                    if (currentConstraint != null) {
                        normalizeConstraintColumns(
                                currentConstraint,
                                currentConstraintColumns
                        );
                        currentConstraints.add(currentConstraint);
                    }

                    currentConstraintName = constraintName;
                    currentConstraint = objectMapper.createObjectNode();
                    currentConstraintColumns = currentConstraint.putArray("columns");
                    currentConstraint.put(
                            "constraintType",
                            requiredString(rs, "CONSTRAINT_TYPE")
                    );
                }

                String columnName = rs.getString("COLUMN_NAME");
                if (columnName != null && !columnName.isBlank()) {
                    currentConstraintColumns.add(columnName);
                }

                applied++;
            }

            if (currentConstraint != null && currentConstraints != null) {
                normalizeConstraintColumns(
                        currentConstraint,
                        currentConstraintColumns
                );
                currentConstraints.add(currentConstraint);
            }
            flushConstraints(currentObject, currentConstraints);

            return logResult(
                    new LoadResult("CONSTRAINTS", rows, applied, skipped),
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

                ObjectMetadata object = snapshot.find(
                        new ObjectKey(
                                requiredString(rs, "SCHEMA_NAME"),
                                requiredString(rs, "TABLE_NAME")
                        )
                );

                if (object == null || !"VIEW".equals(object.tableType)) {
                    skipped++;
                    continue;
                }

                object.viewDefinition = rs.getString("VIEW_DEFINITION");
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

    public long copyDatabases(
            Connection targetConnection,
            List<DatabaseEntry> databases,
            String serviceName) {

        return copy(
                targetConnection,
                DATABASE_COPY_SQL,
                "DATABASE_COPY",
                databases,
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
        data.put("tableType", object.tableType);

        /*
         * MSSQL FOR JSON PATH по умолчанию не включал NULL-поля.
         * Сохраняем прежнюю семантику: у REGULAR viewDefinition отсутствует.
         */
        if (object.viewDefinition != null) {
            data.put("viewDefinition", object.viewDefinition);
        }

        data.set(
                "columns",
                objectMapper.readTree(object.columnsJson)
        );
        data.set(
                "tableConstraints",
                objectMapper.readTree(object.constraintsJson)
        );
        data.put("rawTableType", object.rawTableType);

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
                        "MSSQL HYBRID {} completed. streamedRows={}, copiedRows={}, elapsedMs={}",
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

    private void normalizeConstraintColumns(
            ObjectNode constraint,
            ArrayNode columns) {

        if (constraint != null
                && columns != null
                && columns.isEmpty()) {
            constraint.remove("columns");
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

    private String buildDataTypeDisplay(
            String dataType,
            Integer dataLength,
            Integer precision,
            Integer scale) {

        if (dataType == null) {
            return null;
        }

        return switch (dataType.toLowerCase()) {
            case "varchar", "nvarchar", "char", "nchar" ->
                    dataLength == null
                            ? dataType
                            : dataType + "("
                                    + (dataLength == -1 ? "MAX" : dataLength)
                                    + ")";

            case "decimal", "numeric" ->
                    precision == null
                            ? dataType
                            : dataType + "(" + precision + ","
                                    + (scale != null ? scale : 0) + ")";

            default -> dataType;
        };
    }

    private PreparedStatement prepare(
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
            log.debug("MSSQL driver does not support fetch direction", e);
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

    private boolean getBoolean(
            ResultSet rs,
            String column)
            throws SQLException {

        Object value = rs.getObject(column);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof Number number) {
            return number.intValue() != 0;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private String requiredString(
            ResultSet rs,
            String column)
            throws SQLException {

        String value = rs.getString(column);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Required column is empty: " + column
            );
        }
        return value;
    }

    private LoadResult logResult(
            LoadResult result,
            long started) {

        log.info(
                "MSSQL HYBRID {} loaded. rows={}, applied={}, skipped={}, elapsedMs={}",
                result.stage(),
                result.rows(),
                result.applied(),
                result.skipped(),
                elapsedMs(started)
        );
        return result;
    }

    private MetadataReplicationException wrap(
            String stage,
            Exception e) {

        if (e instanceof MetadataReplicationException mre) {
            return mre;
        }
        return new MetadataReplicationException(
                "MSSQL HYBRID metadata stage failed: " + stage,
                e
        );
    }

    private long elapsedMs(long started) {
        return (System.nanoTime() - started) / 1_000_000;
    }

    @FunctionalInterface
    private interface RowEncoder<T> {
        byte[] encode(T value) throws Exception;
    }

    private record ObjectKey(
            String schemaName,
            String tableName) {

        private ObjectKey {
            Objects.requireNonNull(schemaName, "schemaName");
            Objects.requireNonNull(tableName, "tableName");
        }
    }

    private static final class ObjectMetadata {
        private final long id;
        private final String schemaName;
        private final String tableName;
        private final String tableType;
        private final String rawTableType;
        private final String description;

        private volatile String columnsJson = "[]";
        private volatile String constraintsJson = "[]";
        private volatile String viewDefinition;

        private ObjectMetadata(
                long id,
                String schemaName,
                String tableName,
                String tableType,
                String rawTableType,
                String description) {

            this.id = id;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.tableType = tableType;
            this.rawTableType = rawTableType;
            this.description = description;
        }
    }

    public static final class Snapshot {
        private final List<ObjectMetadata> objects;
        private final Map<ObjectKey, ObjectMetadata> byKey;

        private Snapshot(
                List<ObjectMetadata> objects,
                Map<ObjectKey, ObjectMetadata> byKey) {

            this.objects = List.copyOf(objects);
            this.byKey = byKey;
        }

        private ObjectMetadata find(ObjectKey key) {
            return byKey.get(key);
        }

        public int size() {
            return objects.size();
        }

        public SnapshotSummary summary() {
            long tables = 0;
            long views = 0;
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
                    default -> {
                    }
                }
            }

            return new SnapshotSummary(
                    objects.size(),
                    tables,
                    views,
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
            long viewsWithoutDefinition) {
    }

    public record LoadResult(
            String stage,
            long rows,
            long applied,
            long skipped) {
    }
}
