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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private final int parallelism;
    private final boolean allowEmptyObjectSnapshot;
    private final MetadataExclusionProvider metadataExclusionProvider;

    private static final Pattern ORACLE_MAINTAINED_PREDICATE =
            Pattern.compile(
                    "(?i)\\b([A-Za-z_][A-Za-z0-9_$#]*)"
                            + "\\s*\\.\\s*oracle_maintained"
                            + "\\s*=\\s*'N'"
            );

    private static final Pattern ORACLE_MAINTAINED_TOKEN =
            Pattern.compile(
                    "(?i)\\boracle_maintained\\b"
            );

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
        this.parallelism = Math.max(1, Math.min(parallelism, DETAIL_WORKERS));
        this.allowEmptyObjectSnapshot = allowEmptyObjectSnapshot;
        this.metadataExclusionProvider = metadataExclusionProvider;
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

        long started = System.nanoTime();
        validateSource(source);

        MetadataExclusionRules exclusionRules =
                metadataExclusionProvider.load(
                        DatabaseType.ORACLE
                );

        String serviceName = source.getServiceName();

        String sqlDatabase = sqlQueryProvider.getQuery(
                DatabaseType.ORACLE,
                MetadataType.DATABASE
        );
        String sqlSchema = sqlQueryProvider.getQuery(
                DatabaseType.ORACLE,
                MetadataType.SCHEMA
        );

        String sqlObjects = sqlQueryProvider.getQuery(
                DatabaseType.ORACLE,
                "hybrid_objects"
        );
        String sqlColumns = sqlQueryProvider.getQuery(
                DatabaseType.ORACLE,
                "hybrid_columns"
        );
        String sqlConstraints = sqlQueryProvider.getQuery(
                DatabaseType.ORACLE,
                "hybrid_constraints"
        );
        String sqlFastViews = sqlQueryProvider.getQuery(
                DatabaseType.ORACLE,
                "hybrid_view"
        );
        String sqlLongViews = sqlQueryProvider.getQuery(
                DatabaseType.ORACLE,
                "hybrid_view_long"
        );
        String sqlMaterializedViews = sqlQueryProvider.getQuery(
                DatabaseType.ORACLE,
                "hybrid_mview"
        );

        log.info(
                "Starting Oracle HYBRID metadata replication. "
                        + "serviceName={}, detailParallelism={}",
                serviceName,
                parallelism
        );

        try {
            DatabaseReplicationContext database;
            long databaseCount;

            try (
                    Connection sourceConnection =
                            sourceConnectionFactory.open(source)
            ) {
                database = resolveDatabase(
                        sourceConnection,
                        sqlDatabase,
                        serviceName
                );

                databaseCount = ingestionMetricService.execute(
                        runId,
                        IngestionMetricJob.DATABASE_REPLICATION,
                        counter -> replicateDatabase(
                                sourceConnection,
                                sqlDatabase,
                                serviceName,
                                counter
                        )
                );
            }

            boolean databaseSnapshotCommitted = databaseCount == 1;
            List<String> currentDatabaseNames =
                    List.of(database.databaseName());

            long schemaCount = ingestionMetricService.execute(
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
                                sqlSchema,
                                serviceName,
                                database,
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
                                sqlObjects,
                                sqlColumns,
                                sqlConstraints,
                                sqlFastViews,
                                sqlLongViews,
                                sqlMaterializedViews,
                                serviceName,
                                database,
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
                    "Oracle HYBRID metadata replication completed. "
                            + "serviceName={}, database={}, "
                            + "databases={}, schemas={}, tablesAndViews={}, elapsedMs={}",
                    serviceName,
                    database.databaseName(),
                    stats.databases(),
                    stats.schemas(),
                    stats.tables(),
                    elapsedMs(started)
            );

        } catch (MetadataReplicationException e) {
            log.error(
                    "Oracle HYBRID metadata replication failed. serviceName={}",
                    serviceName,
                    e
            );
            throw e;

        } catch (Exception e) {
            log.error(
                    "Oracle HYBRID metadata replication failed. serviceName={}",
                    serviceName,
                    e
            );
            throw new MetadataReplicationException(
                    "Oracle HYBRID metadata replication failed: " + serviceName,
                    e
            );
        }
    }

    private long replicateDatabase(
            Connection sourceConnection,
            String sql,
            String serviceName,
            MetricCounter counter) {

        try {
            long count = inTargetTransaction(
                    targetConnection -> {
                        metadataRepository.deleteDatabaseMetadata(
                                serviceName,
                                DatabaseType.ORACLE
                        );

                        long copied = standardCopyStreamer.streamDatabases(
                                sourceConnection,
                                targetConnection,
                                sql,
                                serviceName
                        );

                        if (copied != 1) {
                            throw new MetadataReplicationException(
                                    "Expected exactly one Oracle database, but received "
                                            + copied
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
                    "Oracle HYBRID replication error. "
                            + "entityType=DATABASE, serviceName={}",
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
            MetricCounter counter,
            MetadataExclusionRules exclusionRules) {

        String databaseName =
                database.databaseName();

        try {

            long count =
                    executeWithOracleLegacyFallback(
                            source,
                            sql,
                            "SCHEMAS",
                            (sourceConnection, effectiveSql) ->
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
                                                                effectiveSql,
                                                                serviceName,
                                                                database,
                                                                exclusionRules
                                                        );
                                            }
                                    )
                    );

            counter.success(count);

            return count;

        } catch (Exception e) {

            counter.error();

            log.error(
                    "Oracle HYBRID replication error. "
                            + "entityType=SCHEMA, entityName={}.*, "
                            + "database={}, serviceName={}",
                    databaseName,
                    databaseName,
                    serviceName,
                    e
            );

            return 0;
        }
    }

    private long replicateTablesHybrid(
            SourceConnection source,
            String sqlObjects,
            String sqlColumns,
            String sqlConstraints,
            String sqlFastViews,
            String sqlLongViews,
            String sqlMaterializedViews,
            String serviceName,
            DatabaseReplicationContext database,
            MetricCounter counter,
            MetadataExclusionRules exclusionRules) {

        long started = System.nanoTime();

        try {
            Snapshot snapshot;

            /*
             * Phase 1: дешёвый catalog scan.
             * Target transaction ещё не начата.
             */
            try (
                    Connection sourceConnection =
                            sourceConnectionFactory.open(source)
            ) {
                snapshot = loadObjectsWithFallback(
                        source,
                        sqlObjects,
                        exclusionRules,
                        serviceName
                );
            }

            if (snapshot.size() == 0 && !allowEmptyObjectSnapshot) {
                throw new MetadataReplicationException(
                        "Oracle HYBRID object snapshot is empty. "
                                + "Refusing to replace existing TABLE snapshot. "
                                + "serviceName=" + serviceName
                );
            }

            /*
             * Phase 2: четыре независимых source worker-а.
             */
            runDetailWorkers(
                    source,
                    snapshot,
                    sqlColumns,
                    sqlConstraints,
                    sqlFastViews,
                    sqlLongViews,
                    sqlMaterializedViews
            );

            SnapshotSummary summary = snapshot.summary();

            log.info(
                    "Oracle HYBRID extraction completed. "
                            + "serviceName={}, database={}, total={}, tables={}, "
                            + "views={}, mviews={}, viewsWithoutDefinition={}, elapsedMs={}",
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
             * Phase 3: только теперь открываем короткую target transaction.
             * Если extraction выше упал, старый snapshot не затрагивается.
             */
            long count = inTargetTransaction(
                    targetConnection -> {
                        metadataRepository.deleteTableMetadata(
                                serviceName,
                                database.databaseName(),
                                DatabaseType.ORACLE
                        );

                        long copied = hybridCopyStreamer.copyTables(
                                targetConnection,
                                snapshot,
                                serviceName,
                                database
                        );

                        if (copied != snapshot.size()) {
                            throw new MetadataReplicationException(
                                    "Oracle HYBRID table count mismatch. expected="
                                            + snapshot.size()
                                            + ", copied=" + copied
                            );
                        }

                        return copied;
                    }
            );

            counter.success(count);

            log.info(
                    "Oracle HYBRID TABLE snapshot committed. "
                            + "serviceName={}, database={}, count={}, totalElapsedMs={}",
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
                            + "entityType=TABLE, entityName={}.*, "
                            + "database={}, serviceName={}",
                    database.databaseName(),
                    database.databaseName(),
                    serviceName,
                    e
            );
            return 0;
        }
    }

    private void runDetailWorkers(
            SourceConnection source,
            Snapshot snapshot,
            String sqlColumns,
            String sqlConstraints,
            String sqlFastViews,
            String sqlLongViews,
            String sqlMaterializedViews) {

        ExecutorService executor =
                Executors.newFixedThreadPool(
                        parallelism,
                        oracleHybridThreadFactory()
                );

        List<Callable<LoadResult>> tasks =
                List.of(

                        /*
                        * COLUMNS
                        *
                        * ORACLE_MAINTAINED здесь нет,
                        * поэтому fallback не нужен.
                        */
                        () -> executeWithOracleLegacyFallback(
                                source,
                                sqlColumns,
                                "COLUMNS",
                                (connection, effectiveSql) ->
                                        hybridCopyStreamer.loadColumns(
                                                connection,
                                                effectiveSql,
                                                snapshot
                                        )
                        ),
                        /*
                        * CONSTRAINTS
                        */
                        () -> executeWithOracleLegacyFallback(
                                source,
                                sqlConstraints,
                                "CONSTRAINTS",
                                (connection, effectiveSql) ->
                                        hybridCopyStreamer.loadConstraints(
                                                connection,
                                                effectiveSql,
                                                snapshot
                                        )
                        ),
                        /*
                        * FAST VIEW
                        */
                        () -> executeWithOracleLegacyFallback(
                                source,
                                sqlFastViews,
                                "VIEW_FAST",
                                (connection, effectiveSql) ->
                                        hybridCopyStreamer.loadFastViews(
                                                connection,
                                                effectiveSql,
                                                snapshot
                                        )
                        ),
                        /*
                        * LONG VIEW + MATERIALIZED VIEW
                        * Они остаются в одном worker-е,
                        * чтобы сохранить текущий лимит
                        * DETAIL_WORKERS = 4.
                        */
                        () -> {
                            LoadResult longViews =
                                    executeWithOracleLegacyFallback(
                                            source,
                                            sqlLongViews,
                                            "VIEW_LONG",
                                            (connection, effectiveSql) ->
                                                    hybridCopyStreamer
                                                            .loadLongViews(
                                                                    connection,
                                                                    effectiveSql,
                                                                    snapshot
                                                            )
                                    );
                            LoadResult mviews =
                                    executeWithOracleLegacyFallback(
                                            source,
                                            sqlMaterializedViews,
                                            "MVIEW",
                                            (connection, effectiveSql) ->
                                                    hybridCopyStreamer
                                                            .loadMaterializedViews(
                                                                    connection,
                                                                    effectiveSql,
                                                                    snapshot
                                                            )
                                    );
                            return LoadResult.combine(
                                    "VIEW_LONG+MVIEW",
                                    longViews,
                                    mviews
                            );
                        }
                );

        try {

            List<Future<LoadResult>> futures =
                    executor.invokeAll(tasks);

            for (Future<LoadResult> future : futures) {

                try {

                    LoadResult result =
                            future.get();

                    log.debug(
                            "Oracle HYBRID worker finished. "
                                    + "stage={}, rows={}, applied={}, skipped={}",
                            result.stage(),
                            result.rows(),
                            result.applied(),
                            result.skipped()
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
            statement.setFetchSize(1);

            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    throw new MetadataReplicationException(
                            "Oracle database metadata not found"
                    );
                }

                String databaseName = rs.getString("DB_NAME");
                if (databaseName == null || databaseName.isBlank()) {
                    throw new MetadataReplicationException(
                            "Oracle DB_NAME is empty"
                    );
                }

                if (rs.next()) {
                    throw new MetadataReplicationException(
                            "More than one database returned for Oracle connection"
                    );
                }

                return new DatabaseReplicationContext(
                        databaseName,
                        MetadataFqn.database(serviceName, databaseName)
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

    private Snapshot loadObjectsWithFallback(
            SourceConnection source,
            String sql,
            MetadataExclusionRules exclusionRules,
            String serviceName) {

        try {

            return executeWithOracleLegacyFallback(
                    source,
                    sql,
                    "OBJECTS",
                    (connection, effectiveSql) ->
                            hybridCopyStreamer.loadObjects(
                                    connection,
                                    effectiveSql,
                                    exclusionRules
                            )
            );

        } catch (MetadataReplicationException e) {
            throw e;

        } catch (Exception e) {

            throw new MetadataReplicationException(
                    "Oracle HYBRID object catalog load failed. "
                            + "serviceName=" + serviceName,
                    e
            );
        }
    }

    private boolean isOracleMaintainedUnsupported(
            Throwable throwable) {

        Throwable current = throwable;

        while (current != null) {

            if (current instanceof java.sql.SQLException sqlException) {

                /*
                * ORA-00904 = errorCode 904.
                */
                if (sqlException.getErrorCode() == 904) {

                    String message =
                            sqlException.getMessage();

                    return message != null
                            && message
                                .toUpperCase(java.util.Locale.ROOT)
                                .contains("ORACLE_MAINTAINED");
                }
            }

            current = current.getCause();
        }

        return false;
    }

    @FunctionalInterface
    private interface OracleSqlWork<T> {
        T execute(
                Connection connection,
                String sql)
                throws Exception;
    }

    private <T> T executeWithOracleLegacyFallback(
            SourceConnection source,
            String sql,
            String stage,
            OracleSqlWork<T> work)
            throws Exception {

        /*
        * PRIMARY
        */
        try (
                Connection connection =
                        sourceConnectionFactory.open(source)
        ) {

            return work.execute(
                    connection,
                    sql
            );

        } catch (Exception e) {

            if (!isOracleMaintainedUnsupported(e)) {
                throw e;
            }

            log.warn(
                    "Oracle ORACLE_MAINTAINED is not supported. "
                            + "Retrying with legacy schema filtering. "
                            + "stage={}, serviceName={}",
                    stage,
                    source.getServiceName()
            );
        }

        String fallbackSql =
                buildOracleLegacySql(
                        sql,
                        stage
                );

        /*
        * Дополнительная страховка непосредственно
        * перед выполнением.
        */
        if (ORACLE_MAINTAINED_TOKEN
                .matcher(fallbackSql)
                .find()) {

            throw new MetadataReplicationException(
                    "Refusing to execute Oracle legacy SQL because "
                            + "ORACLE_MAINTAINED is still present. "
                            + "stage=" + stage
            );
        }

        /*
        * FALLBACK — обязательно новое connection.
        */
        try (
                Connection connection =
                        sourceConnectionFactory.open(source)
        ) {

            log.debug(
                    "Executing Oracle legacy SQL. "
                            + "stage={}, serviceName={}",
                    stage,
                    source.getServiceName()
            );

            T result =
                    work.execute(
                            connection,
                            fallbackSql
                    );

            log.info(
                    "Oracle legacy schema fallback succeeded. "
                            + "stage={}, serviceName={}",
                    stage,
                    source.getServiceName()
            );

            return result;

        } catch (Exception fallbackException) {

            throw new MetadataReplicationException(
                    "Oracle legacy schema fallback failed. "
                            + "stage=" + stage
                            + ", serviceName="
                            + source.getServiceName(),
                    fallbackException
            );
        }
    }

    private String buildOracleLegacySql(
            String sql,
            String stage) {

        if (sql == null || sql.isBlank()) {
            throw new MetadataReplicationException(
                    "Oracle legacy fallback cannot be applied: "
                            + "SQL is empty. stage=" + stage
            );
        }

        Matcher matcher =
                ORACLE_MAINTAINED_PREDICATE.matcher(sql);

        StringBuffer result =
                new StringBuffer(sql.length() + 512);

        int replacements = 0;

        while (matcher.find()) {

            String alias =
                    matcher.group(1);

            String replacement =
                    buildLegacySchemaPredicate(alias);

            matcher.appendReplacement(
                    result,
                    Matcher.quoteReplacement(replacement)
            );

            replacements++;
        }

        matcher.appendTail(result);

        String fallbackSql =
                result.toString();

        boolean changed =
                !fallbackSql.equals(sql);

        boolean oracleMaintainedRemaining =
                ORACLE_MAINTAINED_TOKEN
                        .matcher(fallbackSql)
                        .find();

        log.warn(
                "Oracle legacy SQL rewrite. "
                        + "stage={}, replacements={}, changed={}, "
                        + "oracleMaintainedRemaining={}",
                stage,
                replacements,
                changed,
                oracleMaintainedRemaining
        );

        /*
        * На DEBUG можно увидеть фактический SQL,
        * который уйдёт в Oracle.
        */
        log.debug(
                "Oracle legacy SQL after rewrite. stage={}\n{}",
                stage,
                fallbackSql
        );

        if (replacements == 0) {

            throw new MetadataReplicationException(
                    "Oracle legacy fallback cannot be applied: "
                            + "ORACLE_MAINTAINED predicate was not replaced. "
                            + "stage=" + stage
            );
        }

        /*
        * Очень важная защита:
        * fallback SQL вообще не должен содержать
        * ORACLE_MAINTAINED.
        */
        if (oracleMaintainedRemaining) {

            throw new MetadataReplicationException(
                    "Oracle legacy fallback SQL still contains "
                            + "ORACLE_MAINTAINED. stage=" + stage
            );
        }

        return fallbackSql;
    }

    private String buildLegacySchemaPredicate(
            String alias) {

        return """
                %s.username NOT IN (
                    'SYS',
                    'SYSTEM',
                    'OUTLN',
                    'DBSNMP',
                    'SYSMAN',
                    'MDSYS',
                    'ORDSYS',
                    'ORDDATA',
                    'CTXSYS',
                    'XDB',
                    'WMSYS',
                    'OLAPSYS',
                    'OWBSYS',
                    'OWBSYS_AUDIT',
                    'APPQOSSYS',
                    'AUDSYS',
                    'GSMADMIN_INTERNAL',
                    'OJVMSYS',
                    'DVF',
                    'DVSYS'
                )
                """.formatted(alias);
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
                || !DatabaseType.ORACLE.name().equalsIgnoreCase(
                        source.getDbType()
                )) {
            throw new MetadataReplicationException(
                    "Expected ORACLE connection, actual="
                            + source.getDbType()
            );
        }
    }

    private ThreadFactory oracleHybridThreadFactory() {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(
                    runnable,
                    "oracle-hybrid-detail-" + counter.incrementAndGet()
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
