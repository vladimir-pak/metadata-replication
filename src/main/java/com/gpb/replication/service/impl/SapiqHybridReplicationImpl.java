package com.gpb.replication.service.impl;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import com.gpb.replication.connection.SourceJdbcConnectionFactory;
import com.gpb.replication.dto.DatabaseReplicationContext;
import com.gpb.replication.dto.ReplicationStats;
import com.gpb.replication.dto.SourceConnection;
import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.enums.MetadataType;
import com.gpb.replication.enums.ReplicationPipeline;
import com.gpb.replication.exceptions.MetadataReplicationException;
import com.gpb.replication.metrics.MetricCounter;
import com.gpb.replication.metrics.enums.IngestionMetricJob;
import com.gpb.replication.repository.MetadataRepository;
import com.gpb.replication.service.IngestionMetricService;
import com.gpb.replication.service.ReplicationService;
import com.gpb.replication.stream.sapiq.AbstractSapiqMetadataCopyStreamer.DatabaseEntry;
import com.gpb.replication.stream.sapiq.AbstractSapiqMetadataCopyStreamer.LoadResult;
import com.gpb.replication.stream.sapiq.AbstractSapiqMetadataCopyStreamer.SchemaEntry;
import com.gpb.replication.stream.sapiq.AbstractSapiqMetadataCopyStreamer.Snapshot;
import com.gpb.replication.stream.sapiq.AbstractSapiqMetadataCopyStreamer.SnapshotSummary;
import com.gpb.replication.stream.sapiq.SapiqHybridMetadataCopyStreamer;
import com.gpb.replication.utils.MetadataFqn;
import com.gpb.replication.utils.SqlQueryProvider;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class SapiqHybridReplicationImpl
        extends ReplicationService {

    private static final int DETAIL_WORKERS = 4;
    private static final int COLUMN_SHARDS = 2;

    private final SqlQueryProvider sqlQueryProvider;
    private final SapiqHybridMetadataCopyStreamer copyStreamer;
    private final int parallelism;
    private final boolean allowEmptyObjectSnapshot;

    public SapiqHybridReplicationImpl(
            SqlQueryProvider sqlQueryProvider,
            @Qualifier("jdbcTemplate")
            JdbcTemplate jdbcTemplate,
            @Qualifier("transactionManager")
            PlatformTransactionManager transactionManager,
            MetadataRepository metadataRepository,
            SourceJdbcConnectionFactory sourceConnectionFactory,
            IngestionMetricService ingestionMetricService,
            SapiqHybridMetadataCopyStreamer copyStreamer,
            @Value("${replication.sapiq.hybrid.parallelism:4}")
            int parallelism,
            @Value("${replication.sapiq.hybrid.allow-empty-object-snapshot:false}")
            boolean allowEmptyObjectSnapshot) {

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
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.SAPIQ;
    }

    @Override
    public ReplicationPipeline getPipeline() {
        return ReplicationPipeline.HYBRID;
    }

    @Override
    protected void execute(
            SourceConnection source,
            String runId) {

        long started = System.nanoTime();
        validateSource(source);

        String serviceName = source.getServiceName();

        String sqlDatabase = sqlQueryProvider.getQuery(
                DatabaseType.SAPIQ,
                MetadataType.DATABASE
        );
        String sqlSchema = sqlQueryProvider.getQuery(
                DatabaseType.SAPIQ,
                MetadataType.SCHEMA
        );
        String sqlObjects = sqlQueryProvider.getQuery(
                DatabaseType.SAPIQ,
                "hybrid_objects"
        );
        String sqlColumns = sqlQueryProvider.getQuery(
                DatabaseType.SAPIQ,
                "hybrid_columns"
        );
        String sqlConstraints = sqlQueryProvider.getQuery(
                DatabaseType.SAPIQ,
                "hybrid_constraints"
        );
        String sqlViews = sqlQueryProvider.getQuery(
                DatabaseType.SAPIQ,
                "hybrid_views"
        );

        log.info(
                "Starting SAP IQ HYBRID metadata replication. "
                        + "serviceName={}, detailParallelism={}, columnShards={}",
                serviceName,
                parallelism,
                COLUMN_SHARDS
        );

        try {
            DatabaseEntry databaseEntry;

            try (
                    Connection connection = sourceConnectionFactory.open(source)
            ) {
                databaseEntry = copyStreamer.loadDatabase(
                        connection,
                        sqlDatabase
                );
            }

            DatabaseReplicationContext database = new DatabaseReplicationContext(
                    databaseEntry.name(),
                    MetadataFqn.database(serviceName, databaseEntry.name())
            );

            long databaseCount = ingestionMetricService.execute(
                    runId,
                    IngestionMetricJob.DATABASE_REPLICATION,
                    counter -> replicateDatabase(
                            databaseEntry,
                            serviceName,
                            counter
                    )
            );

            boolean databaseSnapshotCommitted = databaseCount == 1;
            List<String> currentDatabaseNames = List.of(database.databaseName());

            long schemaCount = ingestionMetricService.execute(
                    runId,
                    IngestionMetricJob.SCHEMA_REPLICATION,
                    counter -> {
                        if (databaseSnapshotCommitted) {
                            cleanupStaleSchemas(
                                    serviceName,
                                    currentDatabaseNames,
                                    DatabaseType.SAPIQ,
                                    counter
                            );
                        } else {
                            log.warn(
                                    "Skipping stale SCHEMA cleanup because DATABASE snapshot "
                                            + "was not committed. serviceName={}",
                                    serviceName
                            );
                        }

                        return replicateSchemas(
                                source,
                                sqlSchema,
                                serviceName,
                                database,
                                counter
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
                                    DatabaseType.SAPIQ,
                                    counter
                            );
                        } else {
                            log.warn(
                                    "Skipping stale TABLE cleanup because DATABASE snapshot "
                                            + "was not committed. serviceName={}",
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
                                database,
                                counter
                        );
                    }
            );

            ReplicationStats stats = new ReplicationStats(
                    databaseCount,
                    schemaCount,
                    tableCount
            );

            log.info(
                    "SAP IQ HYBRID metadata replication completed. "
                            + "serviceName={}, database={}, databases={}, schemas={}, "
                            + "tablesAndViews={}, elapsedMs={}",
                    serviceName,
                    database.databaseName(),
                    stats.databases(),
                    stats.schemas(),
                    stats.tables(),
                    elapsedMs(started)
            );

        } catch (MetadataReplicationException e) {
            log.error(
                    "SAP IQ HYBRID metadata replication failed. serviceName={}",
                    serviceName,
                    e
            );
            throw e;

        } catch (Exception e) {
            log.error(
                    "SAP IQ HYBRID metadata replication failed. serviceName={}",
                    serviceName,
                    e
            );
            throw new MetadataReplicationException(
                    "SAP IQ HYBRID metadata replication failed: " + serviceName,
                    e
            );
        }
    }

    private long replicateDatabase(
            DatabaseEntry database,
            String serviceName,
            MetricCounter counter) {

        try {
            long count = inTargetTransaction(
                    targetConnection -> {
                        metadataRepository.deleteDatabaseMetadata(
                                serviceName,
                                DatabaseType.SAPIQ
                        );

                        long copied = copyStreamer.copyDatabase(
                                targetConnection,
                                database,
                                serviceName
                        );

                        if (copied != 1) {
                            throw new MetadataReplicationException(
                                    "Expected exactly one SAP IQ database, copied=" + copied
                            );
                        }

                        return copied;
                    }
            );

            counter.success(count);
            return count;

        } catch (Exception e) {
            counter.error();
            log.error(
                    "SAP IQ HYBRID replication error. entityType=DATABASE, serviceName={}",
                    serviceName,
                    e
            );
            return 0;
        }
    }

    private long replicateSchemas(
            SourceConnection source,
            String sql,
            String serviceName,
            DatabaseReplicationContext database,
            MetricCounter counter) {

        try {
            List<SchemaEntry> schemas;

            try (
                    Connection connection = sourceConnectionFactory.open(source)
            ) {
                schemas = copyStreamer.loadSchemas(connection, sql);
            }

            if (schemas.isEmpty()) {
                throw new MetadataReplicationException(
                        "SAP IQ schema snapshot is empty. serviceName=" + serviceName
                                + ", database=" + database.databaseName()
                );
            }

            long count = inTargetTransaction(
                    targetConnection -> {
                        metadataRepository.deleteSchemaMetadata(
                                serviceName,
                                database.databaseName(),
                                DatabaseType.SAPIQ
                        );

                        long copied = copyStreamer.copySchemas(
                                targetConnection,
                                schemas,
                                serviceName,
                                database
                        );

                        if (copied != schemas.size()) {
                            throw new MetadataReplicationException(
                                    "SAP IQ schema count mismatch. expected="
                                            + schemas.size() + ", copied=" + copied
                            );
                        }

                        return copied;
                    }
            );

            counter.success(count);
            return count;

        } catch (Exception e) {
            counter.error();
            log.error(
                    "SAP IQ HYBRID replication error. entityType=SCHEMA, database={}, serviceName={}",
                    database.databaseName(),
                    serviceName,
                    e
            );
            return 0;
        }
    }

    private long replicateTables(
            SourceConnection source,
            String sqlObjects,
            String sqlColumns,
            String sqlConstraints,
            String sqlViews,
            String serviceName,
            DatabaseReplicationContext database,
            MetricCounter counter) {

        long started = System.nanoTime();

        try {
            Snapshot snapshot;

            try (
                    Connection connection = sourceConnectionFactory.open(source)
            ) {
                snapshot = copyStreamer.loadObjects(
                        connection,
                        sqlObjects
                );
            }

            if (snapshot.size() == 0 && !allowEmptyObjectSnapshot) {
                throw new MetadataReplicationException(
                        "SAP IQ HYBRID object snapshot is empty. Refusing to replace "
                                + "existing TABLE snapshot. serviceName=" + serviceName
                                + ", database=" + database.databaseName()
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
                    "SAP IQ HYBRID extraction completed. serviceName={}, database={}, "
                            + "total={}, tables={}, views={}, materializedViews={}, "
                            + "viewsWithoutDefinition={}, elapsedMs={}",
                    serviceName,
                    database.databaseName(),
                    summary.total(),
                    summary.tables(),
                    summary.views(),
                    summary.materializedViews(),
                    summary.viewsWithoutDefinition(),
                    elapsedMs(started)
            );

            long count = inTargetTransaction(
                    targetConnection -> {
                        metadataRepository.deleteTableMetadata(
                                serviceName,
                                database.databaseName(),
                                DatabaseType.SAPIQ
                        );

                        long copied = copyStreamer.copyTables(
                                targetConnection,
                                snapshot,
                                serviceName,
                                database
                        );

                        if (copied != snapshot.size()) {
                            throw new MetadataReplicationException(
                                    "SAP IQ table count mismatch. expected="
                                            + snapshot.size() + ", copied=" + copied
                            );
                        }

                        return copied;
                    }
            );

            counter.success(count);
            return count;

        } catch (Exception e) {
            counter.error();
            log.error(
                    "SAP IQ HYBRID replication error. entityType=TABLE, database={}, serviceName={}",
                    database.databaseName(),
                    serviceName,
                    e
            );
            return 0;
        }
    }

    private void runDetailWorkers(
            SourceConnection source,
            DatabaseReplicationContext database,
            Snapshot snapshot,
            String sqlColumns,
            String sqlConstraints,
            String sqlViews,
            String serviceName) {

        ExecutorService executor = Executors.newFixedThreadPool(
                parallelism,
                sapiqHybridThreadFactory(database.databaseName())
        );

        List<Callable<LoadResult>> tasks = List.of(
                () -> withSourceConnection(
                        source,
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
                () -> withSourceConnection(
                        source,
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
                () -> withSourceConnection(
                        source,
                        connection -> copyStreamer.loadConstraints(
                                connection,
                                sqlConstraints,
                                snapshot
                        )
                ),
                () -> withSourceConnection(
                        source,
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
                            "SAP IQ HYBRID worker finished. database={}, stage={}, "
                                    + "rows={}, applied={}, skipped={}",
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
                            "SAP IQ HYBRID detail worker failed. database="
                                    + database.databaseName(),
                            cause
                    );
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MetadataReplicationException(
                    "SAP IQ HYBRID detail workers interrupted. database="
                            + database.databaseName(),
                    e
            );

        } finally {
            executor.shutdownNow();
        }
    }

    private LoadResult withSourceConnection(
            SourceConnection source,
            SourceLoadWork work)
            throws Exception {

        try (
                Connection connection = sourceConnectionFactory.open(source)
        ) {
            return work.execute(connection);
        }
    }

    private void validateSource(SourceConnection source) {
        if (source == null) {
            throw new MetadataReplicationException("Source connection is null");
        }

        if (source.getServiceName() == null || source.getServiceName().isBlank()) {
            throw new MetadataReplicationException("Source serviceName is empty");
        }

        if (source.getDbType() == null
                || !DatabaseType.SAPIQ.name().equalsIgnoreCase(source.getDbType())) {
            throw new MetadataReplicationException(
                    "Expected SAPIQ connection, actual=" + source.getDbType()
            );
        }
    }

    private ThreadFactory sapiqHybridThreadFactory(String databaseName) {
        AtomicInteger counter = new AtomicInteger();

        String safeDatabase = databaseName.replaceAll(
                "[^A-Za-z0-9._-]",
                "_"
        );

        return runnable -> {
            Thread thread = new Thread(
                    runnable,
                    "sapiq-hybrid-" + safeDatabase + "-" + counter.incrementAndGet()
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
