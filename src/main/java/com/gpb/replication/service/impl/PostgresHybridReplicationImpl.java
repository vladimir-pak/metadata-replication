package com.gpb.replication.service.impl;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
import com.gpb.replication.enums.ReplicationPipeline;
import com.gpb.replication.exceptions.MetadataReplicationException;
import com.gpb.replication.exclusion.MetadataExclusionProvider;
import com.gpb.replication.exclusion.MetadataExclusionRules;
import com.gpb.replication.metrics.MetricCounter;
import com.gpb.replication.metrics.enums.IngestionMetricJob;
import com.gpb.replication.repository.MetadataRepository;
import com.gpb.replication.service.IngestionMetricService;
import com.gpb.replication.service.ReplicationService;
import com.gpb.replication.stream.postgres.PostgresHybridMetadataCopyStreamer;
import com.gpb.replication.stream.postgres.PostgresHybridMetadataCopyStreamer.DatabaseEntry;
import com.gpb.replication.stream.postgres.PostgresHybridMetadataCopyStreamer.LoadResult;
import com.gpb.replication.stream.postgres.PostgresHybridMetadataCopyStreamer.SchemaEntry;
import com.gpb.replication.stream.postgres.PostgresHybridMetadataCopyStreamer.Snapshot;
import com.gpb.replication.stream.postgres.PostgresHybridMetadataCopyStreamer.SnapshotSummary;
import com.gpb.replication.stream.postgres.PostgresJdbcUrl;
import com.gpb.replication.utils.MetadataFqn;
import com.gpb.replication.utils.SqlQueryProvider;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class PostgresHybridReplicationImpl
        extends ReplicationService {

    private static final int DETAIL_WORKERS = 4;
    private static final int COLUMN_SHARDS = 2;

    private final SqlQueryProvider sqlQueryProvider;
    private final PostgresHybridMetadataCopyStreamer hybridCopyStreamer;
    private final int parallelism;
    private final boolean allowEmptyObjectSnapshot;
    private final MetadataExclusionProvider metadataExclusionProvider;

    public PostgresHybridReplicationImpl(
            SqlQueryProvider sqlQueryProvider,
            @Qualifier("jdbcTemplate")
            JdbcTemplate jdbcTemplate,
            @Qualifier("transactionManager")
            PlatformTransactionManager transactionManager,
            MetadataRepository metadataRepository,
            SourceJdbcConnectionFactory sourceConnectionFactory,
            IngestionMetricService ingestionMetricService,
            PostgresHybridMetadataCopyStreamer hybridCopyStreamer,
            @Value("${replication.postgres.hybrid.parallelism:4}")
            int parallelism,
            @Value("${replication.postgres.hybrid.allow-empty-object-snapshot:false}")
            boolean allowEmptyObjectSnapshot,
            MetadataExclusionProvider metadataExclusionProvider) {

        super(
                jdbcTemplate,
                transactionManager,
                metadataRepository,
                sourceConnectionFactory,
                ingestionMetricService
        );

        this.sqlQueryProvider = sqlQueryProvider;
        this.hybridCopyStreamer = hybridCopyStreamer;
        this.parallelism = Math.max(
                1,
                Math.min(parallelism, DETAIL_WORKERS)
        );
        this.allowEmptyObjectSnapshot = allowEmptyObjectSnapshot;
        this.metadataExclusionProvider = metadataExclusionProvider;
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.POSTGRES;
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

        MetadataExclusionRules exclusionRules =
                metadataExclusionProvider.load(
                        DatabaseType.POSTGRES
                );

        String serviceName = source.getServiceName();

        String sqlDatabases = sqlQueryProvider.getQuery(
                DatabaseType.POSTGRES,
                "hybrid_databases"
        );
        String sqlSchemas = sqlQueryProvider.getQuery(
                DatabaseType.POSTGRES,
                "hybrid_schemas"
        );
        String sqlObjects = sqlQueryProvider.getQuery(
                DatabaseType.POSTGRES,
                "hybrid_objects"
        );
        String sqlColumns = sqlQueryProvider.getQuery(
                DatabaseType.POSTGRES,
                "hybrid_columns"
        );
        String sqlConstraints = sqlQueryProvider.getQuery(
                DatabaseType.POSTGRES,
                "hybrid_constraints"
        );
        String sqlViews = sqlQueryProvider.getQuery(
                DatabaseType.POSTGRES,
                "hybrid_views"
        );

        log.info(
                "Starting PostgreSQL HYBRID metadata replication. "
                        + "serviceName={}, detailParallelism={}, columnShards={}",
                serviceName,
                parallelism,
                COLUMN_SHARDS
        );

        try {
            List<DatabaseEntry> databases;

            /*
             * pg_database является cluster-wide catalog,
             * поэтому discovery выполняется один раз на исходном URL.
             */
            try (
                    Connection discoveryConnection =
                            sourceConnectionFactory.open(source)
            ) {
                databases = hybridCopyStreamer.loadDatabases(
                        discoveryConnection,
                        sqlDatabases
                );
            }

            if (databases.isEmpty()) {
                throw new MetadataReplicationException(
                        "No PostgreSQL databases found. serviceName="
                                + serviceName
                );
            }

            List<DatabaseReplicationContext> contexts =
                    toDatabaseContexts(
                            databases,
                            serviceName
                    );

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
                                    DatabaseType.POSTGRES,
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
                                    DatabaseType.POSTGRES,
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
                    "PostgreSQL HYBRID metadata replication completed. "
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
                    "PostgreSQL HYBRID metadata replication failed. "
                            + "serviceName={}",
                    serviceName,
                    e
            );
            throw e;

        } catch (Exception e) {
            log.error(
                    "PostgreSQL HYBRID metadata replication failed. "
                            + "serviceName={}",
                    serviceName,
                    e
            );
            throw new MetadataReplicationException(
                    "PostgreSQL HYBRID metadata replication failed: "
                            + serviceName,
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
                                DatabaseType.POSTGRES
                        );

                        long copied = hybridCopyStreamer.copyDatabases(
                                targetConnection,
                                databases,
                                serviceName
                        );

                        if (copied != databases.size()) {
                            throw new MetadataReplicationException(
                                    "PostgreSQL database count mismatch. expected="
                                            + databases.size()
                                            + ", copied=" + copied
                            );
                        }

                        return copied;
                    }
            );

            counter.success(count);

            log.info(
                    "PostgreSQL HYBRID DATABASE snapshot committed. "
                            + "serviceName={}, count={}",
                    serviceName,
                    count
            );

            return count;

        } catch (Exception e) {
            counter.error();

            log.error(
                    "PostgreSQL HYBRID replication error. "
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
                                openDatabaseConnection(
                                        source,
                                        databaseName
                                )
                ) {
                    schemas = hybridCopyStreamer.loadSchemas(
                            sourceConnection,
                            sqlSchemas,
                            exclusionRules
                    );
                }

                if (schemas.isEmpty()) {
                    throw new MetadataReplicationException(
                            "PostgreSQL schema snapshot is empty. "
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
                                    DatabaseType.POSTGRES
                            );

                            long copied = hybridCopyStreamer.copySchemas(
                                    targetConnection,
                                    schemas,
                                    serviceName,
                                    database
                            );

                            if (copied != schemas.size()) {
                                throw new MetadataReplicationException(
                                        "PostgreSQL schema count mismatch. database="
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
                        "PostgreSQL HYBRID SCHEMA snapshot committed. "
                                + "serviceName={}, database={}, count={}",
                        serviceName,
                        databaseName,
                        count
                );

            } catch (Exception e) {
                counter.error();

                log.error(
                        "PostgreSQL HYBRID replication error. "
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

                /*
                 * Object catalog читается первым и закрывается ДО запуска
                 * detail workers. Target transaction здесь ещё отсутствует.
                 */
                try (
                        Connection sourceConnection =
                                openDatabaseConnection(
                                        source,
                                        databaseName
                                )
                ) {
                    snapshot = hybridCopyStreamer.loadObjects(
                            sourceConnection,
                            sqlObjects,
                            exclusionRules
                    );
                }

                if (snapshot.size() == 0 && !allowEmptyObjectSnapshot) {
                    throw new MetadataReplicationException(
                            "PostgreSQL HYBRID object snapshot is empty. "
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
                        sqlViews
                );

                SnapshotSummary summary = snapshot.summary();

                log.info(
                        "PostgreSQL HYBRID extraction completed. "
                                + "serviceName={}, database={}, total={}, tables={}, "
                                + "views={}, materializedViews={}, "
                                + "viewsWithoutDefinition={}, elapsedMs={}",
                        serviceName,
                        databaseName,
                        summary.total(),
                        summary.tables(),
                        summary.views(),
                        summary.materializedViews(),
                        summary.viewsWithoutDefinition(),
                        elapsedMs(started)
                );

                /*
                 * Только после успешного source extraction открываем
                 * target transaction: DELETE + COPY + COMMIT.
                 */
                long count = inTargetTransaction(
                        targetConnection -> {
                            metadataRepository.deleteTableMetadata(
                                    serviceName,
                                    databaseName,
                                    DatabaseType.POSTGRES
                            );

                            long copied = hybridCopyStreamer.copyTables(
                                    targetConnection,
                                    snapshot,
                                    serviceName,
                                    database
                            );

                            if (copied != snapshot.size()) {
                                throw new MetadataReplicationException(
                                        "PostgreSQL HYBRID table count mismatch. database="
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
                        "PostgreSQL HYBRID TABLE snapshot committed. "
                                + "serviceName={}, database={}, count={}, elapsedMs={}",
                        serviceName,
                        databaseName,
                        count,
                        elapsedMs(started)
                );

            } catch (Exception e) {
                counter.error();

                log.error(
                        "PostgreSQL HYBRID replication error. "
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
            String sqlViews) {

        ExecutorService executor = Executors.newFixedThreadPool(
                parallelism,
                postgresHybridThreadFactory(
                        database.databaseName()
                )
        );

        List<Callable<LoadResult>> tasks = List.of(
                () -> withDatabaseConnection(
                        source,
                        database.databaseName(),
                        connection -> hybridCopyStreamer.loadColumns(
                                connection,
                                sqlColumns,
                                snapshot,
                                0,
                                COLUMN_SHARDS
                        )
                ),
                () -> withDatabaseConnection(
                        source,
                        database.databaseName(),
                        connection -> hybridCopyStreamer.loadColumns(
                                connection,
                                sqlColumns,
                                snapshot,
                                1,
                                COLUMN_SHARDS
                        )
                ),
                () -> withDatabaseConnection(
                        source,
                        database.databaseName(),
                        connection -> hybridCopyStreamer.loadConstraints(
                                connection,
                                sqlConstraints,
                                snapshot
                        )
                ),
                () -> withDatabaseConnection(
                        source,
                        database.databaseName(),
                        connection -> hybridCopyStreamer.loadViews(
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
                            "PostgreSQL HYBRID worker finished. "
                                    + "database={}, stage={}, rows={}, "
                                    + "applied={}, skipped={}",
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
                            "PostgreSQL HYBRID detail worker failed. database="
                                    + database.databaseName(),
                            cause
                    );
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new MetadataReplicationException(
                    "PostgreSQL HYBRID detail workers interrupted. database="
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
                        openDatabaseConnection(
                                source,
                                databaseName
                        )
        ) {
            return work.execute(connection);
        }
    }

    private Connection openDatabaseConnection(
            SourceConnection source,
            String databaseName) {

        return sourceConnectionFactory.open(
                source,
                url -> PostgresJdbcUrl.withDatabase(
                        url,
                        databaseName
                )
        );
    }

    private List<DatabaseReplicationContext> toDatabaseContexts(
            List<DatabaseEntry> databases,
            String serviceName) {

        List<DatabaseReplicationContext> result =
                new ArrayList<>(databases.size());

        Set<String> names = new LinkedHashSet<>();

        for (DatabaseEntry database : databases) {
            if (!names.add(database.name())) {
                throw new MetadataReplicationException(
                        "Duplicate PostgreSQL database: "
                                + database.name()
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

    private void validateSource(
            SourceConnection source) {

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
                || !DatabaseType.POSTGRES.name().equalsIgnoreCase(
                        source.getDbType()
                )) {

            throw new MetadataReplicationException(
                    "Expected POSTGRES connection, actual="
                            + source.getDbType()
            );
        }
    }

    private ThreadFactory postgresHybridThreadFactory(
            String databaseName) {

        AtomicInteger counter = new AtomicInteger();

        String safeDatabase = databaseName.replaceAll(
                "[^A-Za-z0-9._-]",
                "_"
        );

        return runnable -> {
            Thread thread = new Thread(
                    runnable,
                    "postgres-hybrid-"
                            + safeDatabase
                            + "-"
                            + counter.incrementAndGet()
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
