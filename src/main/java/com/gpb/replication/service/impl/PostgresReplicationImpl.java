package com.gpb.replication.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
import com.gpb.replication.stream.postgres.PostgresMetadataCopyStreamer;
import com.gpb.replication.utils.MetadataFqn;
import com.gpb.replication.stream.postgres.PostgresJdbcUrl;
import com.gpb.replication.utils.SqlQueryProvider;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class PostgresReplicationImpl extends ReplicationService {

    private final SqlQueryProvider sqlQueryProvider;

    private final PostgresMetadataCopyStreamer copyStreamer;

    public PostgresReplicationImpl(
            SqlQueryProvider sqlQueryProvider,
            @Qualifier("jdbcTemplate")
            JdbcTemplate jdbcTemplate,
            @Qualifier("transactionManager")
            PlatformTransactionManager transactionManager,
            MetadataRepository metadataRepository,
            SourceJdbcConnectionFactory sourceConnectionFactory,
            IngestionMetricService ingestionMetricService,
            PostgresMetadataCopyStreamer copyStreamer) {

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
        return DatabaseType.POSTGRES;
    }

    @Override
    protected void execute(
            SourceConnection source,
            String runId) {

        validateSource(source);

        String serviceName = source.getServiceName();

        String sqlDatabase =
                sqlQueryProvider.getQuery(
                    DatabaseType.POSTGRES,
                    MetadataType.DATABASE
                );

        String sqlSchema =
                sqlQueryProvider.getQuery(
                    DatabaseType.POSTGRES,
                    MetadataType.SCHEMA
                );

        String sqlTable =
                sqlQueryProvider.getQuery(
                    DatabaseType.POSTGRES,
                    MetadataType.TABLE
                );

        long started = System.nanoTime();

        log.info(
            "Starting PostgreSQL metadata replication: "
            + "serviceName={}",
            serviceName
        );

        /*
         * Initial connection:
         * используется только для discovery pg_database
         * и COPY database metadata.
         *
         * SourceJdbcConnectionFactory сам применяет
         * failover по source.url.
         */
        List<DatabaseReplicationContext> databases;
        long databaseCount;

        try (
                Connection discoveryConnection =
                        sourceConnectionFactory.open(source)
        ) {

            databases =
                    discoverDatabases(
                            discoveryConnection,
                            sqlDatabase,
                            serviceName
                    );

            if (databases.isEmpty()) {
                throw new MetadataReplicationException(
                        "No PostgreSQL databases found. serviceName="
                                + serviceName
                );
            }

            log.info(
                    "PostgreSQL databases discovered: "
                            + "serviceName={}, count={}",
                    serviceName,
                    databases.size()
            );

            databaseCount =
                    ingestionMetricService.execute(
                            runId,
                            IngestionMetricJob.DATABASE_REPLICATION,
                            counter ->
                                    replicateDatabases(
                                            discoveryConnection,
                                            sqlDatabase,
                                            serviceName,
                                            databases,
                                            counter
                                    )
                    );
        } catch (MetadataReplicationException e) {
            log.error(
                "PostgreSQL metadata replication failed: "
                + "serviceName={}",
                serviceName,
                e
            );

            throw e;

        } catch (Exception e) {
            log.error(
                "PostgreSQL metadata replication failed: "
                + "serviceName={}",
                serviceName,
                e
            );

            throw new MetadataReplicationException(
                "PostgreSQL metadata replication failed: "
                + serviceName,
                e
            );
        }
            
        boolean databaseSnapshotCommitted =
                databaseCount == databases.size();

        List<String> currentDatabaseNames =
                databases.stream()
                        .map(DatabaseReplicationContext::databaseName)
                        .toList();

        long schemaCount =
                ingestionMetricService.execute(
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
                                sqlSchema,
                                serviceName,
                                databases,
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
                                sqlTable,
                                serviceName,
                                databases,
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
            "PostgreSQL metadata replication completed: "
            + "serviceName={}, "
            + "databases={}, schemas={}, tables={}, "
            + "elapsedMs={}",
            serviceName,
            stats.databases(),
            stats.schemas(),
            stats.tables(),
            elapsedMs
        );
    }

    private long replicateDatabases(
            Connection discoveryConnection,
            String sql,
            String serviceName,
            List<DatabaseReplicationContext> databases,
            MetricCounter counter) {
        try {
            long count =
                    inTargetTransaction(
                            targetConnection -> {
                                metadataRepository
                                        .deleteDatabaseMetadata(
                                            serviceName,
                                            DatabaseType.POSTGRES
                                        );

                                long copied =
                                        copyStreamer.streamDatabases(
                                            discoveryConnection,
                                            targetConnection,
                                            sql,
                                            serviceName
                                        );

                                if (copied != databases.size()) {

                                    throw new MetadataReplicationException(
                                            "PostgreSQL database count mismatch. "
                                            + "discovered="
                                            + databases.size()
                                            + ", copied="
                                            + copied
                                    );
                                }

                                return copied;
                            }
                    );

            counter.success(count);

            log.info(
                "PostgreSQL DATABASE metadata committed. "
                + "serviceName={}, count={}",
                serviceName,
                count
            );

            return count;

        } catch (Exception e) {
            counter.error();

            log.error(
                "PostgreSQL metadata replication error. "
                + "entityType=DATABASE, "
                + "entityName={}, "
                + "serviceName={}",
                serviceName,
                serviceName,
                e
            );

            /*
            * Не пробрасываем exception.
            * SCHEMA_REPLICATION должен продолжиться.
            */
            return 0;
        }
    }

    private long replicateSchemas(
            SourceConnection source,
            String sql,
            String serviceName,
            List<DatabaseReplicationContext> databases,
            MetricCounter counter) {

        long total = 0;

        for (DatabaseReplicationContext database
                : databases) {

            String databaseName =
                    database.databaseName();

            log.info(
                "Starting PostgreSQL SCHEMA replication. "
                + "serviceName={}, database={}",
                serviceName,
                databaseName
            );

            /*
            * Source connection открываем ДО target transaction.
            *
            * Если connection к конкретной DB не установился,
            * target DB вообще не затрагивается.
            */
            try (
                Connection sourceConnection =
                        openDatabaseConnection(
                            source,
                            databaseName
                        )
            ) {

                long count =
                        inTargetTransaction(
                                targetConnection -> {
                                    /*
                                    * Удаляем snapshot schemas
                                    * только конкретной DB.
                                    */
                                    metadataRepository
                                            .deleteSchemaMetadata(
                                                serviceName,
                                                databaseName,
                                                DatabaseType.POSTGRES
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

                /*
                * До этой строки дошли =
                * Spring transaction уже COMMIT.
                */
                counter.success(count);

                total += count;

                log.info(
                    "PostgreSQL SCHEMA metadata committed. "
                    + "serviceName={}, database={}, "
                    + "count={}",
                    serviceName,
                    databaseName,
                    count
                );

            } catch (Exception e) {

                /*
                * Ошибка одной DB не останавливает остальные.
                */
                counter.error();

                log.error(
                    "PostgreSQL metadata replication error. "
                    + "entityType=SCHEMA, "
                    + "entityName={}.*, "
                    + "database={}, "
                    + "serviceName={}",
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
            String sql,
            String serviceName,
            List<DatabaseReplicationContext> databases,
            MetricCounter counter) {

        long total = 0;

        for (DatabaseReplicationContext database : databases) {

            String databaseName = database.databaseName();

            log.info(
                "Starting PostgreSQL TABLE replication. "
                + "serviceName={}, database={}",
                serviceName,
                databaseName
            );

            try (
                Connection sourceConnection =
                        openDatabaseConnection(
                            source,
                            databaseName
                        )
            ) {
                long count =
                        inTargetTransaction(
                                targetConnection -> {
                                    metadataRepository
                                            .deleteTableMetadata(
                                                serviceName,
                                                databaseName,
                                                DatabaseType.POSTGRES
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

                total += count;

                log.info(
                    "PostgreSQL TABLE metadata committed. "
                    + "serviceName={}, database={}, "
                    + "count={}",
                    serviceName,
                    databaseName,
                    count
                );

            } catch (Exception e) {

                counter.error();

                log.error(
                    "PostgreSQL metadata replication error. "
                    + "entityType=TABLE, "
                    + "entityName={}.*, "
                    + "database={}, "
                    + "serviceName={}",
                    databaseName,
                    databaseName,
                    serviceName,
                    e
                );
            }
        }

        return total;
    }

    private List<DatabaseReplicationContext> discoverDatabases(
            Connection connection,
            String sql,
            String serviceName) {

        List<DatabaseReplicationContext> result = new ArrayList<>();

        Set<String> names = new LinkedHashSet<>();

        try (
            PreparedStatement statement =
                    connection.prepareStatement(
                        sql,
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY
                    );

            ResultSet rs = statement.executeQuery()
        ) {
            while (rs.next()) {
                String databaseName = rs.getString("DB_NAME");

                if (databaseName == null || databaseName.isBlank()) {
                    throw new MetadataReplicationException(
                            "PostgreSQL DB_NAME is empty"
                    );
                }

                if (!names.add(databaseName)) {
                    throw new MetadataReplicationException(
                            "Duplicate PostgreSQL database: "
                            + databaseName
                    );
                }

                result.add(
                    new DatabaseReplicationContext(
                        databaseName,
                        MetadataFqn.database(
                            serviceName,
                            databaseName
                        )
                    )
                );
            }

            return result;

        } catch (MetadataReplicationException e) {
            throw e;

        } catch (Exception e) {
            throw new MetadataReplicationException(
                    "Failed to discover PostgreSQL databases",
                    e
            );
        }
    }

    private Connection openDatabaseConnection(
            SourceConnection source,
            String databaseName) {

        return sourceConnectionFactory.open(
                source,
                url ->
                        PostgresJdbcUrl.withDatabase(
                                url,
                                databaseName
                        )
        );
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
                || !DatabaseType.POSTGRES
                        .name()
                        .equalsIgnoreCase(
                            source.getDbType()
                        )) {

            throw new MetadataReplicationException(
                    "Expected POSTGRES connection, actual="
                    + source.getDbType()
            );
        }
    }
}