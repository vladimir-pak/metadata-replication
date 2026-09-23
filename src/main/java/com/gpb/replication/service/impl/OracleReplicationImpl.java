package com.gpb.replication.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import com.gpb.replication.connection.SourceJdbcConnectionFactory;
import com.gpb.replication.dto.DatabaseReplicationContext;
import com.gpb.replication.dto.ReplicationStats;
import com.gpb.replication.dto.SourceConnection;
import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.enums.MetadataType;
import com.gpb.replication.exceptions.MetadataReplicationException;
import com.gpb.replication.metrics.MetricCounter;
import com.gpb.replication.metrics.enums.IngestionMetricJob;
import com.gpb.replication.repository.MetadataRepository;
import com.gpb.replication.service.IngestionMetricService;
import com.gpb.replication.service.ReplicationService;
import com.gpb.replication.stream.oracle.OracleMetadataCopyStreamer;
import com.gpb.replication.utils.MetadataFqn;
import com.gpb.replication.utils.SqlQueryProvider;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class OracleReplicationImpl
        extends ReplicationService {

    private final SqlQueryProvider sqlQueryProvider;

    private final OracleMetadataCopyStreamer copyStreamer;

    public OracleReplicationImpl(
            SqlQueryProvider sqlQueryProvider,
            @Qualifier("jdbcTemplate")
            JdbcTemplate jdbcTemplate,
            @Qualifier("transactionManager")
            PlatformTransactionManager transactionManager,
            MetadataRepository metadataRepository,
            SourceJdbcConnectionFactory sourceConnectionFactory,
            IngestionMetricService ingestionMetricService,
            OracleMetadataCopyStreamer copyStreamer) {

        super(
            jdbcTemplate,
            transactionManager,
            metadataRepository,
            sourceConnectionFactory,
            ingestionMetricService
        );

        this.sqlQueryProvider = sqlQueryProvider;
        this.copyStreamer = copyStreamer;
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.ORACLE;
    }
    
    @Override
    protected void execute(
            SourceConnection source,
            String runId) {

        long started = System.nanoTime();

        validateSource(source);

        String serviceName = source.getServiceName();

        String sqlDatabase =
                sqlQueryProvider.getQuery(
                    DatabaseType.ORACLE,
                    MetadataType.DATABASE
                );

        String sqlSchema =
                sqlQueryProvider.getQuery(
                    DatabaseType.ORACLE,
                    MetadataType.SCHEMA
                );

        String sqlTable =
                sqlQueryProvider.getQuery(
                    DatabaseType.ORACLE,
                    MetadataType.TABLE
                );

        log.info(
            "Starting Oracle metadata replication. "
            + "serviceName={}",
            serviceName
        );

        try {

            DatabaseReplicationContext database;

            long databaseCount;

            /*
            * Первый connection:
            * resolve database + DATABASE replication.
            */
            try (
                Connection sourceConnection =
                        sourceConnectionFactory.open(source)
            ) {
                database =
                        resolveDatabase(
                            sourceConnection,
                            sqlDatabase,
                            serviceName
                        );

                databaseCount =
                        ingestionMetricService.execute(
                                runId,
                                IngestionMetricJob.DATABASE_REPLICATION,
                                counter ->
                                        replicateDatabase(
                                            sourceConnection,
                                            sqlDatabase,
                                            serviceName,
                                            counter
                                        )
                        );
            }

            boolean databaseSnapshotCommitted = databaseCount == 1;

            List<String> currentDatabaseNames =
                    List.of(
                        database.databaseName()
                    );

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
                                    sqlSchema,
                                    serviceName,
                                    database,
                                    counter
                                );
                            }
                    );

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

                                return replicateTables(
                                    source,
                                    sqlTable,
                                    serviceName,
                                    database,
                                    counter
                                );
                            }
                    );

            ReplicationStats stats =
                    new ReplicationStats(
                        databaseCount,
                        schemaCount,
                        tableCount
                    );

            long elapsedMs = (System.nanoTime() - started) / 1_000_000;

            log.info(
                "Oracle metadata replication completed. "
                + "serviceName={}, database={}, "
                + "databases={}, schemas={}, tables={}, "
                + "elapsedMs={}",
                serviceName,
                database.databaseName(),
                stats.databases(),
                stats.schemas(),
                stats.tables(),
                elapsedMs
            );

        } catch (MetadataReplicationException e) {

            log.error(
                "Oracle metadata replication failed. "
                + "serviceName={}",
                serviceName,
                e
            );

            throw e;

        } catch (Exception e) {

            log.error(
                "Oracle metadata replication failed. "
                + "serviceName={}",
                serviceName,
                e
            );

            throw new MetadataReplicationException(
                    "Oracle metadata replication failed: "
                    + serviceName,
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

            long count =
                    inTargetTransaction(
                            targetConnection -> {
                                metadataRepository
                                        .deleteDatabaseMetadata(
                                            serviceName,
                                            DatabaseType.ORACLE
                                        );

                                long copied =
                                        copyStreamer
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

            counter.success(count);

            log.info(
                "Oracle DATABASE metadata committed. "
                + "serviceName={}, count={}",
                serviceName,
                count
            );

            return count;

        } catch (Exception e) {

            counter.error();

            log.error(
                "Oracle metadata replication error. "
                + "entityType=DATABASE, "
                + "entityName={}, "
                + "serviceName={}",
                serviceName,
                serviceName,
                e
            );

            /*
            * Не прерываем pipeline.
            */
            return 0;
        }
    }

    private long replicateSchemas(
            SourceConnection source,
            String sql,
            String serviceName,
            DatabaseReplicationContext database,
            MetricCounter counter) {

        String databaseName = database.databaseName();

        try (
                Connection sourceConnection =
                        sourceConnectionFactory.open(source)
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

                                return copyStreamer
                                        .streamSchemas(
                                            sourceConnection,
                                            targetConnection,
                                            sql,
                                            serviceName,
                                            database
                                        );
                            }
                    );

            counter.success(count);

            log.info(
                "Oracle SCHEMA metadata committed. "
                + "serviceName={}, database={}, count={}",
                serviceName,
                databaseName,
                count
            );

            return count;

        } catch (Exception e) {

            counter.error();

            log.error(
                "Oracle metadata replication error. "
                + "entityType=SCHEMA, "
                + "entityName={}.*, "
                + "database={}, "
                + "serviceName={}",
                databaseName,
                databaseName,
                serviceName,
                e
            );

            return 0;
        }
    }

    private long replicateTables(
            SourceConnection source,
            String sql,
            String serviceName,
            DatabaseReplicationContext database,
            MetricCounter counter) {

        String databaseName = database.databaseName();

        try (
                Connection sourceConnection =
                        sourceConnectionFactory.open(source)
        ) {

            long count =
                    inTargetTransaction(
                            targetConnection -> {

                                metadataRepository
                                        .deleteTableMetadata(
                                            serviceName,
                                            databaseName,
                                            DatabaseType.ORACLE
                                        );

                                return copyStreamer
                                        .streamTables(
                                            sourceConnection,
                                            targetConnection,
                                            sql,
                                            serviceName,
                                            database
                                        );
                            }
                    );

            counter.success(count);

            log.info(
                "Oracle TABLE metadata committed. "
                + "serviceName={}, database={}, count={}",
                serviceName,
                databaseName,
                count
            );

            return count;

        } catch (Exception e) {

            counter.error();

            log.error(
                "Oracle metadata replication error. "
                + "entityType=TABLE, "
                + "entityName={}.*, "
                + "database={}, "
                + "serviceName={}",
                databaseName,
                databaseName,
                serviceName,
                e
            );

            return 0;
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

            try (
                ResultSet rs = statement.executeQuery()
            ) {
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
}