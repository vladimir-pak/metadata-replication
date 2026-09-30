package com.gpb.replication.stream.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

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
public class OracleHybridMetadataCopyStreamer {

    private static final String TABLE_COPY_SQL = """
            COPY metadata_replication.table_metadata_oracle
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

    public OracleHybridMetadataCopyStreamer(
            ObjectMapper objectMapper,
            @Value("${replication.oracle.hybrid.fetch-size:10000}")
            int fetchSize,
            @Value("${replication.oracle.hybrid.query-timeout-seconds:0}")
            int queryTimeoutSeconds) {

        this.objectMapper = objectMapper;
        this.fetchSize = fetchSize;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
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

                ObjectKey key = new ObjectKey(schemaName, tableName);
                ObjectMetadata metadata = new ObjectMetadata(
                        id,
                        schemaName,
                        tableName,
                        tableType
                );

                ObjectMetadata previous = objects.putIfAbsent(key, metadata);
                if (previous != null) {
                    throw new MetadataReplicationException(
                            "Duplicate Oracle metadata object: "
                                    + schemaName + "." + tableName
                                    + ", existingType=" + previous.tableType
                                    + ", newType=" + tableType
                    );
                }
            }

            Snapshot snapshot = new Snapshot(
                    new ArrayList<>(objects.values()),
                    new ConcurrentHashMap<>(objects)
            );

            log.info(
                    "Oracle HYBRID object catalog loaded. "
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
            Snapshot snapshot) {

        long started = System.nanoTime();
        long rows = 0;
        long applied = 0;
        long skipped = 0;

        ObjectKey currentKey = null;
        ObjectMetadata currentObject = null;
        ArrayNode currentColumns = null;

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

                ObjectNode column = objectMapper.createObjectNode();

                String dataType = requiredString(rs, "DATA_TYPE");
                Integer dataLength = getInteger(rs, "DATA_LENGTH");
                Integer precision = getInteger(rs, "DATA_PRECISION");
                Integer scale = getInteger(rs, "DATA_SCALE");
                Integer ordinalPosition = getInteger(rs, "ORDINAL_POSITION");
                String nullable = rs.getString("NULLABLE");

                column.put("name", requiredString(rs, "COLUMN_NAME"));
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
                        "constraint",
                        "N".equalsIgnoreCase(nullable)
                                ? "NOT_NULL"
                                : "NULLABLE"
                );

                if (ordinalPosition == null) {
                    column.putNull("ordinalPosition");
                } else {
                    column.put("ordinalPosition", ordinalPosition);
                }

                currentColumns.add(column);
                applied++;
            }

            flushColumns(currentObject, currentColumns);

            return logResult(
                    new LoadResult("COLUMNS", rows, applied, skipped),
                    started
            );

        } catch (Exception e) {
            throw wrap("COLUMNS", e);
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
                        currentConstraints.add(currentConstraint);
                    }

                    currentConstraintName = constraintName;
                    currentConstraint = objectMapper.createObjectNode();
                    currentConstraint.put(
                            "constraintType",
                            mapConstraintType(
                                    requiredString(rs, "CONSTRAINT_TYPE")
                            )
                    );
                    currentConstraintColumns = currentConstraint.putArray("columns");
                }

                currentConstraintColumns.add(
                        requiredString(rs, "COLUMN_NAME")
                );
                applied++;
            }

            if (currentConstraint != null && currentConstraints != null) {
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

    public LoadResult loadFastViews(
            Connection connection,
            String sql,
            Snapshot snapshot) {

        return loadViewDefinitions(
                connection,
                sql,
                snapshot,
                "VIEW",
                false,
                "VIEW_FAST"
        );
    }

    public LoadResult loadLongViews(
            Connection connection,
            String sql,
            Snapshot snapshot) {

        return loadViewDefinitions(
                connection,
                sql,
                snapshot,
                "VIEW",
                true,
                "VIEW_LONG"
        );
    }

    public LoadResult loadMaterializedViews(
            Connection connection,
            String sql,
            Snapshot snapshot) {

        return loadViewDefinitions(
                connection,
                sql,
                snapshot,
                "MATERIALIZED_VIEW",
                true,
                "MVIEW"
        );
    }

    public long copyTables(
            Connection targetConnection,
            Snapshot snapshot,
            String serviceName,
            DatabaseReplicationContext database) {

        long started = System.nanoTime();

        try {
            PGConnection pgConnection = targetConnection.unwrap(PGConnection.class);
            CopyIn copyIn = pgConnection.getCopyAPI().copyIn(TABLE_COPY_SQL);

            long streamedRows = 0;

            try {
                for (ObjectMetadata object : snapshot.objects) {
                    byte[] row = serializeTableMetadata(
                            object,
                            serviceName,
                            database
                    );

                    copyIn.writeToCopy(row, 0, row.length);
                    streamedRows++;
                }

                long copiedRows = copyIn.endCopy();

                log.info(
                        "Oracle HYBRID TABLE COPY completed. "
                                + "streamedRows={}, copiedRows={}, elapsedMs={}",
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
            throw wrap("TABLE_COPY", e);
        }
    }

    private LoadResult loadViewDefinitions(
            Connection connection,
            String sql,
            Snapshot snapshot,
            String expectedType,
            boolean overwrite,
            String stage) {

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

                String schemaName = requiredString(rs, "SCHEMA_NAME");
                String tableName = requiredString(rs, "TABLE_NAME");

                /*
                 * В VIEW_LONG/MVIEW колонка VIEW_DEFINITION = LONG
                 * и намеренно является последней колонкой SELECT.
                 */
                String definition = sanitizePostgresText(
                        rs.getString(
                                "VIEW_DEFINITION"
                        )
                );

                ObjectMetadata object = snapshot.find(
                        new ObjectKey(schemaName, tableName)
                );

                if (object == null
                        || !expectedType.equals(object.tableType)) {
                    skipped++;
                    continue;
                }

                if (overwrite) {
                    object.viewDefinition.set(definition);
                } else {
                    object.viewDefinition.compareAndSet(null, definition);
                }

                applied++;
            }

            return logResult(
                    new LoadResult(stage, rows, applied, skipped),
                    started
            );

        } catch (Exception e) {
            throw wrap(stage, e);
        }
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

        String viewDefinition = sanitizePostgresText(
                object.viewDefinition.get()
        );
                
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
        String description = null;
        String hash = MetadataHash.sha256(description, dataJson);

        return PostgresCopyCsvEncoder.encode(
                object.id,
                fqn,
                serviceName,
                database.databaseName(),
                object.schemaName,
                description,
                object.tableName,
                parentFqn,
                dataJson,
                hash
        );
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

        return switch (dataType.toUpperCase()) {
            case "VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR" ->
                    dataLength == null
                            ? dataType
                            : dataType + "(" + dataLength + ")";

            case "NUMBER" -> {
                if (precision == null) {
                    yield dataType;
                }
                if (scale == null) {
                    yield dataType + "(" + precision + ")";
                }
                yield dataType + "(" + precision + "," + scale + ")";
            }

            default -> dataType;
        };
    }

    private String sanitizePostgresText(
            String value) {

        if (value == null) {
            return null;
        }

        if (value.indexOf('\u0000') < 0) {
            return value;
        }

        return value.replace(
                "\u0000",
                ""
        );
    }

    private String mapConstraintType(String type) {
        return switch (type) {
            case "P" -> "PRIMARY_KEY";
            case "R" -> "FOREIGN_KEY";
            case "U" -> "UNIQUE";
            default -> "OTHER";
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
            log.debug("Oracle driver does not support fetch direction", e);
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
                "Oracle HYBRID {} loaded. rows={}, applied={}, skipped={}, elapsedMs={}",
                result.stage,
                result.rows,
                result.applied,
                result.skipped,
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
                "Oracle HYBRID metadata stage failed: " + stage,
                e
        );
    }

    private long elapsedMs(long started) {
        return (System.nanoTime() - started) / 1_000_000;
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

        private volatile String columnsJson = "[]";
        private volatile String constraintsJson = "[]";
        private final AtomicReference<String> viewDefinition =
                new AtomicReference<>();

        private ObjectMetadata(
                long id,
                String schemaName,
                String tableName,
                String tableType) {

            this.id = id;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.tableType = tableType;
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
            long materializedViews = 0;
            long viewsWithoutDefinition = 0;

            for (ObjectMetadata object : objects) {
                switch (object.tableType) {
                    case "REGULAR" -> tables++;
                    case "VIEW" -> {
                        views++;
                        if (object.viewDefinition.get() == null) {
                            viewsWithoutDefinition++;
                        }
                    }
                    case "MATERIALIZED_VIEW" -> {
                        materializedViews++;
                        if (object.viewDefinition.get() == null) {
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

        public static LoadResult combine(
                String stage,
                LoadResult first,
                LoadResult second) {

            return new LoadResult(
                    stage,
                    first.rows + second.rows,
                    first.applied + second.applied,
                    first.skipped + second.skipped
            );
        }
    }
}
