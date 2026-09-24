package com.gpb.replication.service.impl;

import java.sql.Connection;
import java.util.List;

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
import com.gpb.replication.stream.sapiq.AbstractSapiqMetadataCopyStreamer.DatabaseEntry;
import com.gpb.replication.stream.sapiq.AbstractSapiqMetadataCopyStreamer.Snapshot;
import com.gpb.replication.stream.sapiq.AbstractSapiqMetadataCopyStreamer.SnapshotSummary;
import com.gpb.replication.stream.sapiq.SapiqMetadataCopyStreamer;
import com.gpb.replication.utils.MetadataFqn;
import com.gpb.replication.utils.SqlQueryProvider;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class SapiqReplicationImpl
        extends ReplicationService {

    private final SqlQueryProvider sqlQueryProvider;
    private final SapiqMetadataCopyStreamer copyStreamer;
    private final boolean allowEmptyObjectSnapshot;
    private final MetadataExclusionProvider metadataExclusionProvider;

    public SapiqReplicationImpl(
            SqlQueryProvider sqlQueryProvider,
            @Qualifier("jdbcTemplate")
            JdbcTemplate jdbcTemplate,
            @Qualifier("transactionManager")
            PlatformTransactionManager transactionManager,
            MetadataRepository metadataRepository,
            SourceJdbcConnectionFactory sourceConnectionFactory,
            IngestionMetricService ingestionMetricService,
            SapiqMetadataCopyStreamer copyStreamer,
            @Value("${replication.sapiq.allow-empty-object-snapshot:false}")
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
        this.copyStreamer = copyStreamer;
        this.allowEmptyObjectSnapshot = allowEmptyObjectSnapshot;
        this.metadataExclusionProvider = metadataExclusionProvider;
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.SAPIQ;
    }

    @Override
    public ReplicationPipeline getPipeline() {
        return ReplicationPipeline.STANDARD;
    }

    @Override
    protected void execute(
            SourceConnection source,
            String runId) {

        long started = System.nanoTime();
        validateSource(source);

        MetadataExclusionRules exclusionRules =
                metadataExclusionProvider.load(
                        DatabaseType.SAPIQ
                );

        String serviceName = source.getServiceName();

        String sqlDatabase = sqlQueryProvider.getQuery(
                DatabaseType.SAPIQ,
                MetadataType.DATABASE
        );
        String sqlSchema = sqlQueryProvider.getQuery(
                DatabaseType.SAPIQ,
                MetadataType.SCHEMA
        );
        String sqlTable = sqlQueryProvider.getQuery(
                DatabaseType.SAPIQ,
                MetadataType.TABLE
        );
        String sqlColumn = sqlQueryProvider.getQuery(
                DatabaseType.SAPIQ,
                "column"
        );
        String sqlConstraint = sqlQueryProvider.getQuery(
                DatabaseType.SAPIQ,
                "constraint"
        );

        log.info(
                "Starting SAP IQ STANDARD metadata replication. serviceName={}",
                serviceName
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
                                sqlTable,
                                sqlColumn,
                                sqlConstraint,
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
                    "SAP IQ STANDARD metadata replication completed. "
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
                    "SAP IQ STANDARD metadata replication failed. serviceName={}",
                    serviceName,
                    e
            );
            throw e;

        } catch (Exception e) {
            log.error(
                    "SAP IQ STANDARD metadata replication failed. serviceName={}",
                    serviceName,
                    e
            );
            throw new MetadataReplicationException(
                    "SAP IQ STANDARD metadata replication failed: " + serviceName,
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
                    "SAP IQ STANDARD replication error. entityType=DATABASE, serviceName={}",
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

        try {
            List<com.gpb.replication.stream.sapiq.AbstractSapiqMetadataCopyStreamer.SchemaEntry> schemas;

            try (
                    Connection connection = sourceConnectionFactory.open(source)
            ) {
                schemas = copyStreamer.loadSchemas(
                        connection, 
                        sql, 
                        exclusionRules
                );
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
                    "SAP IQ STANDARD replication error. entityType=SCHEMA, database={}, serviceName={}",
                    database.databaseName(),
                    serviceName,
                    e
            );
            return 0;
        }
    }

    private long replicateTables(
            SourceConnection source,
            String sqlTable,
            String sqlColumn,
            String sqlConstraint,
            String serviceName,
            DatabaseReplicationContext database,
            MetricCounter counter,
            MetadataExclusionRules exclusionRules) {

        long started = System.nanoTime();

        try {
            Snapshot snapshot;

            try (
                    Connection connection = sourceConnectionFactory.open(source)
            ) {
                snapshot = copyStreamer.loadStandardObjects(
                        connection,
                        sqlTable,
                        exclusionRules
                );
            }

            if (snapshot.size() == 0 && !allowEmptyObjectSnapshot) {
                throw new MetadataReplicationException(
                        "SAP IQ STANDARD object snapshot is empty. Refusing to replace "
                                + "existing TABLE snapshot. serviceName=" + serviceName
                                + ", database=" + database.databaseName()
                );
            }

            try (
                    Connection connection = sourceConnectionFactory.open(source)
            ) {
                copyStreamer.loadColumns(
                        connection,
                        sqlColumn,
                        snapshot,
                        serviceName,
                        database
                );
            }

            try (
                    Connection connection = sourceConnectionFactory.open(source)
            ) {
                copyStreamer.loadConstraints(
                        connection,
                        sqlConstraint,
                        snapshot
                );
            }

            SnapshotSummary summary = snapshot.summary();

            log.info(
                    "SAP IQ STANDARD extraction completed. serviceName={}, database={}, "
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
                    "SAP IQ STANDARD replication error. entityType=TABLE, database={}, serviceName={}",
                    database.databaseName(),
                    serviceName,
                    e
            );
            return 0;
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

    private long elapsedMs(long started) {
        return (System.nanoTime() - started) / 1_000_000;
    }
}
