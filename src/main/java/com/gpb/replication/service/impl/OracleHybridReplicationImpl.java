package com.gpb.replication.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import com.gpb.replication.exclusion.MetadataExclusionProvider;
import com.gpb.replication.exclusion.MetadataExclusionRules;
import com.gpb.replication.metrics.MetricCounter;
import com.gpb.replication.metrics.enums.IngestionMetricJob;
import com.gpb.replication.repository.MetadataRepository;
import com.gpb.replication.service.IngestionMetricService;
import com.gpb.replication.service.ReplicationService;
import com.gpb.replication.stream.oracle.OracleCapabilities;
import com.gpb.replication.stream.oracle.OracleCompatibilityService;
import com.gpb.replication.stream.oracle.OracleHybridMetadataCopyStreamer;
import com.gpb.replication.stream.oracle.OracleHybridMetadataCopyStreamer.LoadResult;
import com.gpb.replication.stream.oracle.OracleHybridMetadataCopyStreamer.Snapshot;
import com.gpb.replication.stream.oracle.OracleHybridMetadataCopyStreamer.SnapshotSummary;
import com.gpb.replication.stream.oracle.OracleMetadataCopyStreamer;
import com.gpb.replication.utils.MetadataFqn;
import com.gpb.replication.utils.SqlQueryProvider;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class OracleHybridReplicationImpl
        extends ReplicationService {

    private static final int DETAIL_WORKERS = 4;

    private final SqlQueryProvider sqlQueryProvider;

    private final OracleMetadataCopyStreamer standardCopyStreamer;

    private final OracleHybridMetadataCopyStreamer hybridCopyStreamer;

    private final OracleCompatibilityService compatibilityService;

    private final MetadataExclusionProvider metadataExclusionProvider;

    private final int parallelism;

    private final boolean allowEmptyObjectSnapshot;


    public OracleHybridReplicationImpl(
            SqlQueryProvider sqlQueryProvider,

            @Qualifier("jdbcTemplate")
            JdbcTemplate jdbcTemplate,

            @Qualifier("transactionManager")
            PlatformTransactionManager transactionManager,

            MetadataRepository metadataRepository,

            SourceJdbcConnectionFactory sourceConnectionFactory,

            IngestionMetricService ingestionMetricService,

            OracleMetadataCopyStreamer standardCopyStreamer,

            OracleHybridMetadataCopyStreamer hybridCopyStreamer,

            OracleCompatibilityService compatibilityService,

            @Value("${replication.oracle.hybrid.parallelism:4}")
            int parallelism,

            @Value("${replication.oracle.hybrid.allow-empty-object-snapshot:false}")
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

        this.standardCopyStreamer = standardCopyStreamer;

        this.hybridCopyStreamer = hybridCopyStreamer;

        this.compatibilityService = compatibilityService;

        this.metadataExclusionProvider = metadataExclusionProvider;

        this.parallelism =
                Math.max(
                        1,
                        Math.min(
                                parallelism,
                                DETAIL_WORKERS
                        )
                );

        this.allowEmptyObjectSnapshot =
                allowEmptyObjectSnapshot;
    }


    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.ORACLE;
    }


    @Override
    public ReplicationPipeline getPipeline() {
        return ReplicationPipeline.HYBRID;
    }


    @Override
    protected void execute(
            SourceConnection source,
            String runId) {

        long started =
                System.nanoTime();

        validateSource(source);

        String serviceName =
                source.getServiceName();

        MetadataExclusionRules exclusionRules =
                metadataExclusionProvider.load(
                        DatabaseType.ORACLE
                );

        /*
         * Загружаем SQL только один раз.
         */
        OracleHybridSql rawSql =
                loadSql();

        log.info(
                "Starting Oracle HYBRID metadata replication. "
                        + "serviceName={}, detailParallelism={}",
                serviceName,
                parallelism
        );

        try {

            DatabaseReplicationContext database;

            OracleCapabilities capabilities;

            long databaseCount;

            /*
             * Используем первый connection:
             *
             * 1. database metadata
             * 2. Oracle capabilities
             * 3. database replication
             */
            try (
                    Connection sourceConnection =
                            sourceConnectionFactory.open(
                                    source
                            )
            ) {
                database =
                        resolveDatabase(
                                sourceConnection,
                                rawSql.database(),
                                serviceName
                        );

                capabilities =
                        compatibilityService.detect(
                                sourceConnection
                        );

                databaseCount =
                        ingestionMetricService.execute(
                                runId,
                                IngestionMetricJob.DATABASE_REPLICATION,
                                counter ->
                                        replicateDatabase(
                                                sourceConnection,
                                                rawSql.database(),
                                                serviceName,
                                                counter
                                        )
                        );
            }

            /*
             * После capability detection готовим SQL,
             * который гарантированно подходит
             * конкретной версии Oracle.
             */
            OracleHybridSql sql =
                    prepareSql(
                            rawSql,
                            capabilities
                    );

            log.info(
                    "Oracle HYBRID effective mode. "
                            + "serviceName={}, "
                            + "oracleMaintainedSupported={}",
                    serviceName,
                    capabilities.oracleMaintainedSupported()
            );

            boolean databaseSnapshotCommitted =
                    databaseCount == 1;

            List<String> currentDatabaseNames =
                    List.of(
                            database.databaseName()
                    );

            /*
             * SCHEMAS
             */
            long schemaCount =
                    ingestionMetricService.execute(
                            runId,
                            IngestionMetricJob.SCHEMA_REPLICATION,
                            counter -> {

                                if (databaseSnapshotCommitted) {

                                    cleanupStaleSchemas(
                                            serviceName,
                                            currentDatabaseNames,
                                            DatabaseType.ORACLE,
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
                                        sql.schema(),
                                        serviceName,
                                        database,
                                        counter,
                                        exclusionRules
                                );
                            }
                    );

            /*
             * TABLES / VIEWS
             */
            long tableCount =
                    ingestionMetricService.execute(
                            runId,
                            IngestionMetricJob.TABLE_REPLICATION,
                            counter -> {

                                if (databaseSnapshotCommitted) {

                                    cleanupStaleTables(
                                            serviceName,
                                            currentDatabaseNames,
                                            DatabaseType.ORACLE,
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

                                return replicateTablesHybrid(
                                        source,
                                        sql,
                                        serviceName,
                                        database,
                                        counter,
                                        exclusionRules
                                );
                            }
                    );

            ReplicationStats stats =
                    new ReplicationStats(
                            databaseCount,
                            schemaCount,
                            tableCount
                    );

            log.info(
                    "Oracle HYBRID metadata replication completed. "
                            + "serviceName={}, database={}, "
                            + "databases={}, schemas={}, "
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
                    "Oracle HYBRID metadata replication failed. "
                            + "serviceName={}",
                    serviceName,
                    e
            );

            throw e;

        } catch (Exception e) {

            log.error(
                    "Oracle HYBRID metadata replication failed. "
                            + "serviceName={}",
                    serviceName,
                    e
            );

            throw new MetadataReplicationException(
                    "Oracle HYBRID metadata replication failed: "
                            + serviceName,
                    e
            );
        }
    }

    /*
     * ---------------------------------------------------------------------
     * SQL
     * ---------------------------------------------------------------------
     */
    private OracleHybridSql loadSql() {

        return new OracleHybridSql(
                sqlQueryProvider.getQuery(
                        DatabaseType.ORACLE,
                        MetadataType.DATABASE
                ),

                sqlQueryProvider.getQuery(
                        DatabaseType.ORACLE,
                        MetadataType.SCHEMA
                ),

                sqlQueryProvider.getQuery(
                        DatabaseType.ORACLE,
                        "hybrid_objects"
                ),

                sqlQueryProvider.getQuery(
                        DatabaseType.ORACLE,
                        "hybrid_columns"
                ),

                sqlQueryProvider.getQuery(
                        DatabaseType.ORACLE,
                        "hybrid_constraints"
                ),

                sqlQueryProvider.getQuery(
                        DatabaseType.ORACLE,
                        "hybrid_view"
                ),

                sqlQueryProvider.getQuery(
                        DatabaseType.ORACLE,
                        "hybrid_mview"
                )
        );
    }

    private OracleHybridSql prepareSql(
            OracleHybridSql raw,
            OracleCapabilities capabilities) {

        return new OracleHybridSql(
                raw.database(),

                compatibilityService.applySchemaCompatibility(
                        raw.schema(),
                        capabilities,
                        "SCHEMAS"
                ),

                compatibilityService.applySchemaCompatibility(
                        raw.objects(),
                        capabilities,
                        "OBJECTS"
                ),

                compatibilityService.applySchemaCompatibility(
                        raw.columns(),
                        capabilities,
                        "COLUMNS"
                ),

                compatibilityService.applySchemaCompatibility(
                        raw.constraints(),
                        capabilities,
                        "CONSTRAINTS"
                ),

                compatibilityService.applySchemaCompatibility(
                        raw.views(),
                        capabilities,
                        "VIEWS"
                ),

                compatibilityService.applySchemaCompatibility(
                        raw.materializedViews(),
                        capabilities,
                        "MVIEW"
                )
        );
    }

    /*
     * ---------------------------------------------------------------------
     * DATABASE
     * ---------------------------------------------------------------------
     */
    private long replicateDatabase(
            Connection sourceConnection,
            String sql,
            String serviceName,
            MetricCounter counter) {

        try {
            long count =
                    inTargetTransaction(
                            targetConnection -> {

                                metadataRepository
                                        .deleteDatabaseMetadata(
                                                serviceName,
                                                DatabaseType.ORACLE
                                        );

                                long copied =
                                        standardCopyStreamer
                                                .streamDatabases(
                                                        sourceConnection,
                                                        targetConnection,
                                                        sql,
                                                        serviceName
                                                );

                                if (copied != 1) {

                                    throw new MetadataReplicationException(
                                            "Expected exactly one Oracle database, "
                                                    + "but received "
                                                    + copied
                                    );
                                }

                                return copied;
                            }
                    );

            counter.success(
                    count
            );

            return count;

        } catch (Exception e) {

            counter.error();

            log.error(
                    "Oracle HYBRID replication error. "
                            + "entityType=DATABASE, serviceName={}",
                    serviceName,
                    e
            );

            return 0;
        }
    }

    /*
     * ---------------------------------------------------------------------
     * SCHEMAS
     * ---------------------------------------------------------------------
     */
    private long replicateSchemas(
            SourceConnection source,
            String sql,
            String serviceName,
            DatabaseReplicationContext database,
            MetricCounter counter,
            MetadataExclusionRules exclusionRules) {

        String databaseName =
                database.databaseName();

        try (
                Connection sourceConnection =
                        sourceConnectionFactory.open(
                                source
                        )
        ) {

            long count =
                    inTargetTransaction(
                            targetConnection -> {

                                metadataRepository
                                        .deleteSchemaMetadata(
                                                serviceName,
                                                databaseName,
                                                DatabaseType.ORACLE
                                        );

                                return standardCopyStreamer
                                        .streamSchemas(
                                                sourceConnection,
                                                targetConnection,
                                                sql,
                                                serviceName,
                                                database,
                                                exclusionRules
                                        );
                            }
                    );

            counter.success(
                    count
            );

            return count;

        } catch (Exception e) {

            counter.error();

            log.error(
                    "Oracle HYBRID replication error. "
                            + "entityType=SCHEMA, database={}, "
                            + "serviceName={}",
                    databaseName,
                    serviceName,
                    e
            );

            return 0;
        }
    }

    /*
     * ---------------------------------------------------------------------
     * TABLES / VIEWS
     * ---------------------------------------------------------------------
     */

    private long replicateTablesHybrid(
            SourceConnection source,
            OracleHybridSql sql,
            String serviceName,
            DatabaseReplicationContext database,
            MetricCounter counter,
            MetadataExclusionRules exclusionRules) {

        long started =
                System.nanoTime();

        try {

            /*
             * Phase 1.
             *
             * Полный catalog объектов.
             * Target PostgreSQL transaction
             * ещё не открыта.
             */
            Snapshot snapshot;

            try (
                    Connection sourceConnection =
                            sourceConnectionFactory.open(
                                    source
                            )
            ) {

                snapshot =
                        hybridCopyStreamer.loadObjects(
                                sourceConnection,
                                sql.objects(),
                                exclusionRules
                        );
            }

            if (snapshot.size() == 0
                    && !allowEmptyObjectSnapshot) {

                throw new MetadataReplicationException(
                        "Oracle HYBRID object snapshot is empty. "
                                + "Refusing to replace existing TABLE snapshot. "
                                + "serviceName="
                                + serviceName
                );
            }

            /*
             * Phase 2.
             *
             * COLUMNS
             * CONSTRAINTS
             * VIEW_FAST
             * VIEW_LONG
             * MVIEW
             */
            runDetailWorkers(
                    source,
                    snapshot,
                    sql
            );

            SnapshotSummary summary =
                    snapshot.summary();

            log.info(
                    "Oracle HYBRID extraction completed. "
                            + "serviceName={}, database={}, "
                            + "total={}, tables={}, views={}, "
                            + "mviews={}, viewsWithoutDefinition={}, "
                            + "elapsedMs={}",
                    serviceName,
                    database.databaseName(),
                    summary.total(),
                    summary.tables(),
                    summary.views(),
                    summary.materializedViews(),
                    summary.viewsWithoutDefinition(),
                    elapsedMs(started)
            );

            /*
             * Phase 3.
             *
             * Только после успешного extraction
             * заменяем snapshot в PostgreSQL.
             */
            long count =
                    inTargetTransaction(
                            targetConnection -> {

                                metadataRepository
                                        .deleteTableMetadata(
                                                serviceName,
                                                database.databaseName(),
                                                DatabaseType.ORACLE
                                        );

                                long copied =
                                        hybridCopyStreamer
                                                .copyTables(
                                                        targetConnection,
                                                        snapshot,
                                                        serviceName,
                                                        database
                                                );

                                if (copied != snapshot.size()) {

                                    throw new MetadataReplicationException(
                                            "Oracle HYBRID table count mismatch. "
                                                    + "expected="
                                                    + snapshot.size()
                                                    + ", copied="
                                                    + copied
                                    );
                                }

                                return copied;
                            }
                    );

            counter.success(
                    count
            );

            log.info(
                    "Oracle HYBRID TABLE snapshot committed. "
                            + "serviceName={}, database={}, count={}, "
                            + "totalElapsedMs={}",
                    serviceName,
                    database.databaseName(),
                    count,
                    elapsedMs(started)
            );

            return count;

        } catch (Exception e) {

            counter.error();

            log.error(
                    "Oracle HYBRID replication error. "
                            + "entityType=TABLE, database={}, "
                            + "serviceName={}",
                    database.databaseName(),
                    serviceName,
                    e
            );

            return 0;
        }
    }

    /*
     * ---------------------------------------------------------------------
     * DETAIL WORKERS
     * ---------------------------------------------------------------------
     */

    private void runDetailWorkers(
            SourceConnection source,
            Snapshot snapshot,
            OracleHybridSql sql) {

        ExecutorService executor =
                Executors.newFixedThreadPool(
                        parallelism,
                        oracleHybridThreadFactory()
                );

        List<Callable<LoadResult>> tasks =
        List.of(

                () -> withSourceConnection(
                        source,
                        connection ->
                                hybridCopyStreamer
                                        .loadColumns(
                                                connection,
                                                sql.columns(),
                                                snapshot
                                        )
                ),

                () -> withSourceConnection(
                        source,
                        connection ->
                                hybridCopyStreamer
                                        .loadConstraints(
                                                connection,
                                                sql.constraints(),
                                                snapshot
                                        )
                ),

                () -> withSourceConnection(
                        source,
                        connection ->
                                hybridCopyStreamer
                                        .loadViews(
                                                connection,
                                                sql.views(),
                                                snapshot
                                        )
                ),

                () -> withSourceConnection(
                        source,
                        connection ->
                                hybridCopyStreamer
                                        .loadMaterializedViews(
                                                connection,
                                                sql.materializedViews(),
                                                snapshot
                                        )
                )
        );

        executeWorkers(
                executor,
                tasks
        );
    }

    private void executeWorkers(
            ExecutorService executor,
            List<Callable<LoadResult>> tasks) {

        try {

            List<Future<LoadResult>> futures =
                    executor.invokeAll(
                            tasks
                    );

            for (Future<LoadResult> future : futures) {

                LoadResult result =
                        getWorkerResult(
                                future
                        );

                log.debug(
                        "Oracle HYBRID worker finished. "
                                + "stage={}, rows={}, applied={}, skipped={}",
                        result.stage(),
                        result.rows(),
                        result.applied(),
                        result.skipped()
                );
            }

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            throw new MetadataReplicationException(
                    "Oracle HYBRID detail workers interrupted",
                    e
            );

        } finally {

            executor.shutdownNow();
        }
    }

    private LoadResult getWorkerResult(
            Future<LoadResult> future) {

        try {

            return future.get();

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            throw new MetadataReplicationException(
                    "Oracle HYBRID worker interrupted",
                    e
            );

        } catch (ExecutionException e) {

            Throwable cause =
                    e.getCause();

            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }

            throw new MetadataReplicationException(
                    "Oracle HYBRID detail worker failed",
                    cause
            );
        }
    }

    private LoadResult withSourceConnection(
            SourceConnection source,
            SourceLoadWork work)
            throws Exception {

        try (
                Connection connection =
                        sourceConnectionFactory.open(
                                source
                        )
        ) {

            return work.execute(
                    connection
            );
        }
    }

    /*
     * ---------------------------------------------------------------------
     * DATABASE CONTEXT
     * ---------------------------------------------------------------------
     */

    private DatabaseReplicationContext resolveDatabase(
            Connection sourceConnection,
            String sqlDatabase,
            String serviceName) {

        try (
                PreparedStatement statement =
                        sourceConnection.prepareStatement(
                                sqlDatabase,
                                ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_READ_ONLY
                        )
        ) {

            statement.setFetchSize(
                    1
            );

            try (
                    ResultSet rs =
                            statement.executeQuery()
            ) {

                if (!rs.next()) {

                    throw new MetadataReplicationException(
                            "Oracle database metadata not found"
                    );
                }

                String databaseName =
                        rs.getString(
                                "DB_NAME"
                        );

                if (databaseName == null
                        || databaseName.isBlank()) {

                    throw new MetadataReplicationException(
                            "Oracle DB_NAME is empty"
                    );
                }

                if (rs.next()) {

                    throw new MetadataReplicationException(
                            "More than one database returned "
                                    + "for Oracle connection"
                    );
                }

                return new DatabaseReplicationContext(
                        databaseName,
                        MetadataFqn.database(
                                serviceName,
                                databaseName
                        )
                );
            }

        } catch (Exception e) {

            if (e instanceof MetadataReplicationException mre) {
                throw mre;
            }

            throw new MetadataReplicationException(
                    "Failed to resolve Oracle database",
                    e
            );
        }
    }

    /*
     * ---------------------------------------------------------------------
     * VALIDATION
     * ---------------------------------------------------------------------
     */

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
                || !DatabaseType.ORACLE
                        .name()
                        .equalsIgnoreCase(
                                source.getDbType()
                        )) {

            throw new MetadataReplicationException(
                    "Expected ORACLE connection, actual="
                            + source.getDbType()
            );
        }
    }

    /*
     * ---------------------------------------------------------------------
     * UTILS
     * ---------------------------------------------------------------------
     */

    private ThreadFactory oracleHybridThreadFactory() {

        AtomicInteger counter =
                new AtomicInteger();

        return runnable -> {

            Thread thread =
                    new Thread(
                            runnable,
                            "oracle-hybrid-detail-"
                                    + counter.incrementAndGet()
                    );

            thread.setDaemon(false);

            return thread;
        };
    }


    private long elapsedMs(
            long started) {

        return (System.nanoTime() - started) / 1_000_000;
    }


    /*
     * ---------------------------------------------------------------------
     * INTERNAL TYPES
     * ---------------------------------------------------------------------
     */

    private record OracleHybridSql(
            String database,
            String schema,
            String objects,
            String columns,
            String constraints,
            String views,
            String materializedViews) {
    }

    @FunctionalInterface
    private interface SourceLoadWork {

        LoadResult execute(
                Connection connection)
                throws Exception;
    }
}