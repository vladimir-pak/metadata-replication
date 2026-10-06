package com.gpb.replication.service.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.gpb.replication.connection.SourceJdbcConnectionFactory;
import com.gpb.replication.dto.DatabaseReplicationContext;
import com.gpb.replication.dto.ReplicationStats;
import com.gpb.replication.dto.SourceConnection;
import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.enums.ReplicationPipeline;
import com.gpb.replication.exceptions.MetadataReplicationException;
import com.gpb.replication.exclusion.MetadataExclusionProvider;
import com.gpb.replication.exclusion.MetadataExclusionRules;
import com.gpb.replication.metrics.MetricCounter;
import com.gpb.replication.metrics.enums.IngestionMetricJob;
import com.gpb.replication.repository.MetadataRepository;
import com.gpb.replication.service.IngestionMetricService;
import com.gpb.replication.service.ReplicationService;
import com.gpb.replication.stream.sapase.SapaseMetadataCopyStreamer;
import com.gpb.replication.stream.sapase.SapaseMetadataCopyStreamer.DatabaseEntry;
import com.gpb.replication.stream.sapase.SapaseMetadataCopyStreamer.LoadResult;
import com.gpb.replication.stream.sapase.SapaseMetadataCopyStreamer.SchemaEntry;
import com.gpb.replication.stream.sapase.SapaseMetadataCopyStreamer.Snapshot;
import com.gpb.replication.stream.sapase.SapaseMetadataCopyStreamer.SnapshotSummary;
import com.gpb.replication.utils.MetadataFqn;
import com.gpb.replication.utils.SqlQueryProvider;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSapaseReplicationImpl
        extends ReplicationService {

    private static final int DETAIL_WORKERS = 4;
    private static final int COLUMN_SHARDS = 2;

    private final SqlQueryProvider sqlQueryProvider;
    private final SapaseMetadataCopyStreamer copyStreamer;
    private final int parallelism;
    private final boolean allowEmptyObjectSnapshot;
    private final MetadataExclusionProvider metadataExclusionProvider;
    private final boolean allDatabases;
    private final String driverClassName;

    protected AbstractSapaseReplicationImpl(
            SqlQueryProvider sqlQueryProvider,
            @Qualifier("jdbcTemplate")
            JdbcTemplate jdbcTemplate,
            @Qualifier("transactionManager")
            PlatformTransactionManager transactionManager,
            MetadataRepository metadataRepository,
            SourceJdbcConnectionFactory sourceConnectionFactory,
            IngestionMetricService ingestionMetricService,
            SapaseMetadataCopyStreamer copyStreamer,
            int parallelism,
            boolean allowEmptyObjectSnapshot,
            MetadataExclusionProvider metadataExclusionProvider,
            String databaseScope,
            String driverClassName) {

        super(
                jdbcTemplate,
                transactionManager,
                metadataRepository,
                sourceConnectionFactory,
                ingestionMetricService
        );

        this.sqlQueryProvider = sqlQueryProvider;
        this.copyStreamer = copyStreamer;
        this.parallelism = Math.max(1, Math.min(parallelism, DETAIL_WORKERS));
        this.allowEmptyObjectSnapshot = allowEmptyObjectSnapshot;
        this.metadataExclusionProvider = metadataExclusionProvider;
        String scope = databaseScope == null
                ? "" : databaseScope.trim().toUpperCase(Locale.ROOT);
        if (!"CURRENT".equals(scope) && !"ALL".equals(scope)) {
            throw new IllegalArgumentException(
                    "replication.sapase.database-scope must be CURRENT or ALL"
            );
        }
        this.allDatabases = "ALL".equals(scope);
        if (driverClassName == null || driverClassName.isBlank()) {
            throw new IllegalArgumentException(
                    "replication.sapase.driver-class-name must not be blank"
            );
        }
        this.driverClassName = driverClassName.trim();
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.SAPASE;
    }

    @Override
    protected void execute(
            SourceConnection source,
            String runId) {

        long started = System.nanoTime();
        validateSource(source);

        MetadataExclusionRules exclusionRules =
                metadataExclusionProvider.load(
                        DatabaseType.SAPASE
                );

        String serviceName = source.getServiceName();

        String sqlDatabases = sqlQueryProvider.getQuery(
                DatabaseType.SAPASE,
                allDatabases ? "hybrid_databases" : "database"
        );
        String sqlSchemas = sqlQueryProvider.getQuery(
                DatabaseType.SAPASE,
                "hybrid_schemas"
        );
        String sqlObjects = sqlQueryProvider.getQuery(
                DatabaseType.SAPASE,
                "hybrid_objects"
        );
        String sqlColumns = sqlQueryProvider.getQuery(
                DatabaseType.SAPASE,
                "hybrid_columns"
        );
        String sqlConstraints = sqlQueryProvider.getQuery(
                DatabaseType.SAPASE,
                "hybrid_constraints"
        );
        String sqlViews = sqlQueryProvider.getQuery(
                DatabaseType.SAPASE,
                "hybrid_views"
        );

        log.info(
                "Starting SAP ASE metadata replication. "
                        + "serviceName={}, pipeline={}, databaseScope={}, "
                        + "detailParallelism={}, columnShards={}",
                serviceName,
                getPipeline(),
                allDatabases ? "ALL" : "CURRENT",
                parallelism,
                getPipeline() == ReplicationPipeline.STANDARD ? 1 : COLUMN_SHARDS
        );

        try {
            List<DatabaseEntry> databases;

            try (
                    Connection discoveryConnection =
                            openDatabaseConnection(source, null)
            ) {
                databases = copyStreamer.loadDatabases(
                        discoveryConnection,
                        sqlDatabases
                );
            }

            if (databases.isEmpty()) {
                throw new MetadataReplicationException(
                        "No SAP ASE databases found. serviceName=" + serviceName
                );
            }

            List<DatabaseReplicationContext> contexts =
                    toDatabaseContexts(databases, serviceName);

            long databaseCount = ingestionMetricService.execute(
                    runId,
                    IngestionMetricJob.DATABASE_REPLICATION,
                    counter -> replicateDatabases(
                            databases,
                            serviceName,
                            counter
                    )
            );

            boolean databaseSnapshotCommitted =
                    databaseCount == databases.size();

            List<String> currentDatabaseNames = contexts.stream()
                    .map(DatabaseReplicationContext::databaseName)
                    .toList();

            long schemaCount = ingestionMetricService.execute(
                    runId,
                    IngestionMetricJob.SCHEMA_REPLICATION,
                    counter -> {
                        if (databaseSnapshotCommitted) {
                            cleanupStaleSchemas(
                                    serviceName,
                                    currentDatabaseNames,
                                    DatabaseType.SAPASE,
                                    counter
                            );
                        } else {
                            log.warn(
                                    "Skipping stale SCHEMA cleanup because "
                                            + "DATABASE snapshot was not committed. "
                                            + "serviceName={}",
                                    serviceName
                            );
                        }

                        return replicateSchemas(
                                source,
                                sqlSchemas,
                                serviceName,
                                contexts,
                                counter,
                                exclusionRules
                        );
                    }
            );

            long tableCount = ingestionMetricService.execute(
                    runId,
                    IngestionMetricJob.TABLE_REPLICATION,
                    counter -> {
                        if (databaseSnapshotCommitted) {
                            cleanupStaleTables(
                                    serviceName,
                                    currentDatabaseNames,
                                    DatabaseType.SAPASE,
                                    counter
                            );
                        } else {
                            log.warn(
                                    "Skipping stale TABLE cleanup because "
                                            + "DATABASE snapshot was not committed. "
                                            + "serviceName={}",
                                    serviceName
                            );
                        }

                        return replicateTables(
                                source,
                                sqlObjects,
                                sqlColumns,
                                sqlConstraints,
                                sqlViews,
                                serviceName,
                                contexts,
                                counter,
                                exclusionRules
                        );
                    }
            );

            ReplicationStats stats = new ReplicationStats(
                    databaseCount,
                    schemaCount,
                    tableCount
            );

            log.info(
                    "SAP ASE metadata replication completed. "
                            + "serviceName={}, databases={}, schemas={}, "
                            + "tablesAndViews={}, elapsedMs={}",
                    serviceName,
                    stats.databases(),
                    stats.schemas(),
                    stats.tables(),
                    elapsedMs(started)
            );

        } catch (MetadataReplicationException e) {
            log.error(
                    "SAP ASE metadata replication failed. serviceName={}",
                    serviceName,
                    e
            );
            throw e;

        } catch (Exception e) {
            log.error(
                    "SAP ASE metadata replication failed. serviceName={}",
                    serviceName,
                    e
            );
            throw new MetadataReplicationException(
                    "SAP ASE metadata replication failed: " + serviceName,
                    e
            );
        }
    }

    private long replicateDatabases(
            List<DatabaseEntry> databases,
            String serviceName,
            MetricCounter counter) {

        try {
            long count = inTargetTransaction(
                    targetConnection -> {
                        metadataRepository.deleteDatabaseMetadata(
                                serviceName,
                                DatabaseType.SAPASE
                        );

                        long copied = copyStreamer.copyDatabases(
                                targetConnection,
                                databases,
                                serviceName
                        );

                        if (copied != databases.size()) {
                            throw new MetadataReplicationException(
                                    "SAP ASE database count mismatch. expected="
                                            + databases.size()
                                            + ", copied=" + copied
                            );
                        }

                        return copied;
                    }
            );

            counter.success(count);

            log.info(
                    "SAP ASE DATABASE snapshot committed. "
                            + "serviceName={}, count={}",
                    serviceName,
                    count
            );

            return count;

        } catch (Exception e) {
            counter.error();
            log.error(
                    "SAP ASE replication error. "
                            + "entityType=DATABASE, serviceName={}",
                    serviceName,
                    e
            );
            return 0;
        }
    }

    private long replicateSchemas(
            SourceConnection source,
            String sqlSchemas,
            String serviceName,
            List<DatabaseReplicationContext> databases,
            MetricCounter counter,
            MetadataExclusionRules exclusionRules) {

        long total = 0;

        for (DatabaseReplicationContext database : databases) {
            String databaseName = database.databaseName();

            try {
                List<SchemaEntry> schemas;

                try (
                        Connection sourceConnection =
                                openDatabaseConnection(source, databaseName)
                ) {
                    schemas = copyStreamer.loadSchemas(
                            sourceConnection,
                            sqlSchemas,
                            exclusionRules
                    );
                }

                if (schemas.isEmpty()) {
                    throw new MetadataReplicationException(
                            "SAP ASE schema snapshot is empty. "
                                    + "Refusing to replace existing SCHEMA snapshot. "
                                    + "serviceName=" + serviceName
                                    + ", database=" + databaseName
                    );
                }

                long count = inTargetTransaction(
                        targetConnection -> {
                            metadataRepository.deleteSchemaMetadata(
                                    serviceName,
                                    databaseName,
                                    DatabaseType.SAPASE
                            );

                            long copied = copyStreamer.copySchemas(
                                    targetConnection,
                                    schemas,
                                    serviceName,
                                    database
                            );

                            if (copied != schemas.size()) {
                                throw new MetadataReplicationException(
                                        "SAP ASE schema count mismatch. database="
                                                + databaseName
                                                + ", expected=" + schemas.size()
                                                + ", copied=" + copied
                                );
                            }

                            return copied;
                        }
                );

                counter.success(count);
                total += count;

                log.info(
                        "SAP ASE SCHEMA snapshot committed. "
                                + "serviceName={}, database={}, count={}",
                        serviceName,
                        databaseName,
                        count
                );

            } catch (Exception e) {
                counter.error();
                log.error(
                        "SAP ASE replication error. "
                                + "entityType=SCHEMA, entityName={}.*, "
                                + "database={}, serviceName={}",
                        databaseName,
                        databaseName,
                        serviceName,
                        e
                );
            }
        }

        return total;
    }

    private long replicateTables(
            SourceConnection source,
            String sqlObjects,
            String sqlColumns,
            String sqlConstraints,
            String sqlViews,
            String serviceName,
            List<DatabaseReplicationContext> databases,
            MetricCounter counter,
            MetadataExclusionRules exclusionRules) {

        long total = 0;

        for (DatabaseReplicationContext database : databases) {
            String databaseName = database.databaseName();
            long started = System.nanoTime();

            try {
                Snapshot snapshot;

                try (
                        Connection sourceConnection =
                                openDatabaseConnection(source, databaseName)
                ) {
                    snapshot = copyStreamer.loadObjects(
                            sourceConnection,
                            sqlObjects,
                            exclusionRules
                    );
                }

                if (snapshot.size() == 0 && !allowEmptyObjectSnapshot) {
                    throw new MetadataReplicationException(
                            "SAP ASE object snapshot is empty. "
                                    + "Refusing to replace existing TABLE snapshot. "
                                    + "serviceName=" + serviceName
                                    + ", database=" + databaseName
                    );
                }

                runDetailWorkers(
                        source,
                        database,
                        snapshot,
                        sqlColumns,
                        sqlConstraints,
                        sqlViews,
                        serviceName
                );

                SnapshotSummary summary = snapshot.summary();

                log.info(
                        "SAP ASE extraction completed. "
                                + "serviceName={}, database={}, total={}, tables={}, "
                                + "views={}, viewsWithoutDefinition={}, elapsedMs={}",
                        serviceName,
                        databaseName,
                        summary.total(),
                        summary.tables(),
                        summary.views(),
                        summary.viewsWithoutDefinition(),
                        elapsedMs(started)
                );

                long count = inTargetTransaction(
                        targetConnection -> {
                            metadataRepository.deleteTableMetadata(
                                    serviceName,
                                    databaseName,
                                    DatabaseType.SAPASE
                            );

                            long copied = copyStreamer.copyTables(
                                    targetConnection,
                                    snapshot,
                                    serviceName,
                                    database
                            );

                            if (copied != snapshot.size()) {
                                throw new MetadataReplicationException(
                                        "SAP ASE table count mismatch. database="
                                                + databaseName
                                                + ", expected=" + snapshot.size()
                                                + ", copied=" + copied
                                );
                            }

                            return copied;
                        }
                );

                counter.success(count);
                total += count;

                log.info(
                        "SAP ASE TABLE snapshot committed. "
                                + "serviceName={}, database={}, count={}, elapsedMs={}",
                        serviceName,
                        databaseName,
                        count,
                        elapsedMs(started)
                );

            } catch (Exception e) {
                counter.error();
                log.error(
                        "SAP ASE replication error. "
                                + "entityType=TABLE, entityName={}.*, "
                                + "database={}, serviceName={}",
                        databaseName,
                        databaseName,
                        serviceName,
                        e
                );
            }
        }

        return total;
    }

    private void runDetailWorkers(
            SourceConnection source,
            DatabaseReplicationContext database,
            Snapshot snapshot,
            String sqlColumns,
            String sqlConstraints,
            String sqlViews,
            String serviceName) {

        if (getPipeline() == ReplicationPipeline.STANDARD) {
            try {
                withDatabaseConnection(
                        source,
                        database.databaseName(),
                        connection -> {
                            copyStreamer.loadColumns(
                                    connection, sqlColumns, snapshot,
                                    serviceName, database, 0, 1
                            );
                            copyStreamer.loadConstraints(connection, sqlConstraints, snapshot);
                            return copyStreamer.loadViews(connection, sqlViews, snapshot);
                        }
                );
                return;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new MetadataReplicationException(
                        "SAP ASE STANDARD details failed. database=" + database.databaseName(),
                        e
                );
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(
                parallelism,
                sapaseThreadFactory(database.databaseName())
        );

        List<Callable<LoadResult>> tasks = List.of(
                () -> withDatabaseConnection(
                        source,
                        database.databaseName(),
                        connection -> copyStreamer.loadColumns(
                                connection,
                                sqlColumns,
                                snapshot,
                                serviceName,
                                database,
                                0,
                                COLUMN_SHARDS
                        )
                ),
                () -> withDatabaseConnection(
                        source,
                        database.databaseName(),
                        connection -> copyStreamer.loadColumns(
                                connection,
                                sqlColumns,
                                snapshot,
                                serviceName,
                                database,
                                1,
                                COLUMN_SHARDS
                        )
                ),
                () -> withDatabaseConnection(
                        source,
                        database.databaseName(),
                        connection -> copyStreamer.loadConstraints(
                                connection,
                                sqlConstraints,
                                snapshot
                        )
                ),
                () -> withDatabaseConnection(
                        source,
                        database.databaseName(),
                        connection -> copyStreamer.loadViews(
                                connection,
                                sqlViews,
                                snapshot
                        )
                )
        );

        try {
            List<Future<LoadResult>> futures = executor.invokeAll(tasks);

            for (Future<LoadResult> future : futures) {
                try {
                    LoadResult result = future.get();
                    log.debug(
                            "SAP ASE worker finished. "
                                    + "database={}, stage={}, rows={}, applied={}, skipped={}",
                            database.databaseName(),
                            result.stage(),
                            result.rows(),
                            result.applied(),
                            result.skipped()
                    );

                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException runtimeException) {
                        throw runtimeException;
                    }
                    throw new MetadataReplicationException(
                            "SAP ASE detail worker failed. database="
                                    + database.databaseName(),
                            cause
                    );
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MetadataReplicationException(
                    "SAP ASE detail workers interrupted. database="
                            + database.databaseName(),
                    e
            );

        } finally {
            executor.shutdownNow();
        }
    }

    private LoadResult withDatabaseConnection(
            SourceConnection source,
            String databaseName,
            SourceLoadWork work)
            throws Exception {

        try (
                Connection connection =
                        openDatabaseConnection(source, databaseName)
        ) {
            return work.execute(connection);
        }
    }

    private Connection openDatabaseConnection(
            SourceConnection source,
            String databaseName) {

        try {
            Class.forName(driverClassName);
        } catch (ClassNotFoundException e) {
            throw new MetadataReplicationException(
                    "SAP ASE JDBC driver is not on the runtime classpath: "
                            + driverClassName,
                    e
            );
        }

        Connection connection = sourceConnectionFactory.open(source);
        if (databaseName == null) {
            return connection;
        }

        try {
            // jConnect switches the database through JDBC; no SQL identifier interpolation.
            connection.setCatalog(databaseName);
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT db_name()")) {
                if (!rs.next() || !databaseName.equals(rs.getString(1))) {
                    throw new MetadataReplicationException(
                            "SAP ASE driver did not switch to database: " + databaseName
                    );
                }
            }
            return connection;
        } catch (SQLException | RuntimeException e) {
            try {
                connection.close();
            } catch (SQLException closeException) {
                e.addSuppressed(closeException);
            }
            throw new MetadataReplicationException(
                    "Unable to select SAP ASE database: " + databaseName,
                    e
            );
        }
    }

    private List<DatabaseReplicationContext> toDatabaseContexts(
            List<DatabaseEntry> databases,
            String serviceName) {

        List<DatabaseReplicationContext> result = new ArrayList<>(databases.size());
        Set<String> names = new LinkedHashSet<>();

        for (DatabaseEntry database : databases) {
            if (!names.add(database.name())) {
                throw new MetadataReplicationException(
                        "Duplicate SAP ASE database: " + database.name()
                );
            }

            result.add(
                    new DatabaseReplicationContext(
                            database.name(),
                            MetadataFqn.database(
                                    serviceName,
                                    database.name()
                            )
                    )
            );
        }

        return List.copyOf(result);
    }

    private void validateSource(SourceConnection source) {
        if (source == null) {
            throw new MetadataReplicationException(
                    "Source connection is null"
            );
        }

        if (source.getServiceName() == null
                || source.getServiceName().isBlank()) {
            throw new MetadataReplicationException(
                    "Source serviceName is empty"
            );
        }

        if (source.getDbType() == null
                || !DatabaseType.SAPASE.name().equalsIgnoreCase(
                        source.getDbType().trim()
                )) {
            throw new MetadataReplicationException(
                    "Expected SAP ASE connection, actual="
                            + source.getDbType()
            );
        }
    }

    private ThreadFactory sapaseThreadFactory(String databaseName) {
        AtomicInteger counter = new AtomicInteger();

        String safeDatabase = databaseName.replaceAll(
                "[^A-Za-z0-9._-]",
                "_"
        );

        return runnable -> {
            Thread thread = new Thread(
                    runnable,
                    "sapase-" + safeDatabase + "-" + counter.incrementAndGet()
            );
            thread.setDaemon(false);
            return thread;
        };
    }

    private long elapsedMs(long started) {
        return (System.nanoTime() - started) / 1_000_000;
    }

    @FunctionalInterface
    private interface SourceLoadWork {
        LoadResult execute(Connection connection) throws Exception;
    }
}
