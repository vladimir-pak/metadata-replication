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
public class PostgresReplicationImpl
        extends ReplicationService {

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
        try (
            Connection discoveryConnection =
                    sourceConnectionFactory.open(source)
        ) {
            List<DatabaseReplicationContext> databases =
                    discoverDatabases(
                        discoveryConnection,
                        sqlDatabase,
                        serviceName
                    );

            if (databases.isEmpty()) {
                throw new MetadataReplicationException(
                        "No PostgreSQL databases found. "
                        + "serviceName="
                        + serviceName
                );
            }

            log.info(
                "PostgreSQL databases discovered: "
                + "serviceName={}, count={}",
                serviceName,
                databases.size()
            );

            ReplicationStats stats =
                    inTargetTransaction(
                            targetConnection -> {

                                long databaseCount =
                                        ingestionMetricService.execute(
                                                runId,
                                                IngestionMetricJob.DATABASE_REPLICATION,
                                                counter -> {
                                                    /*
                                                     * Полная замена snapshot
                                                     * данного service.
                                                     *
                                                     * Вызывать только один раз.
                                                     */
                                                    deleteServiceSnapshot(
                                                        serviceName,
                                                        DatabaseType.POSTGRES
                                                    );

                                                    long count =
                                                            copyStreamer
                                                                    .streamDatabases(
                                                                        discoveryConnection,
                                                                        targetConnection,
                                                                        sqlDatabase,
                                                                        serviceName
                                                                    );

                                                    if (count != databases.size()) {
                                                        throw new MetadataReplicationException(
                                                                "PostgreSQL database count mismatch. "
                                                                + "discovered="
                                                                + databases.size()
                                                                + ", copied="
                                                                + count
                                                        );
                                                    }

                                                    counter.success(count);

                                                    return count;
                                                }
                                        );

                                long schemaCount =
                                        ingestionMetricService.execute(
                                                runId,
                                                IngestionMetricJob.SCHEMA_REPLICATION,
                                                counter -> {
                                                    long count =
                                                            streamSchemas(
                                                                source,
                                                                targetConnection,
                                                                sqlSchema,
                                                                serviceName,
                                                                databases
                                                            );

                                                    counter.success(count);

                                                    return count;
                                                }
                                        );

                                long tableCount =
                                        ingestionMetricService.execute(
                                                runId,
                                                IngestionMetricJob.TABLE_REPLICATION,
                                                counter -> {
                                                    long count =
                                                            streamTables(
                                                                source,
                                                                targetConnection,
                                                                sqlTable,
                                                                serviceName,
                                                                databases
                                                            );

                                                    counter.success(count);

                                                    return count;
                                                }
                                        );

                                return new ReplicationStats(
                                    databaseCount,
                                    schemaCount,
                                    tableCount
                                );
                            }
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

    private long streamSchemas(
            SourceConnection source,
            Connection targetConnection,
            String sql,
            String serviceName,
            List<DatabaseReplicationContext> databases) {

        long total = 0;

        for (DatabaseReplicationContext database : databases) {
            log.info(
                "Streaming PostgreSQL schemas: "
                + "serviceName={}, database={}",
                serviceName,
                database.databaseName()
            );

            try (
                Connection sourceConnection =
                        openDatabaseConnection(
                            source,
                            database.databaseName()
                        )
            ) {
                total +=
                        copyStreamer.streamSchemas(
                            sourceConnection,
                            targetConnection,
                            sql,
                            serviceName,
                            database
                        );
            } catch (Exception e) {
                throw new MetadataReplicationException(
                        "Failed to replicate PostgreSQL schemas. "
                        + "serviceName="
                        + serviceName
                        + ", database="
                        + database.databaseName(),
                        e
                );
            }
        }

        return total;
    }

    private long streamTables(
            SourceConnection source,
            Connection targetConnection,
            String sql,
            String serviceName,
            List<DatabaseReplicationContext> databases) {

        long total = 0;

        for (DatabaseReplicationContext database : databases) {
            log.info(
                    "Streaming PostgreSQL tables: "
                    + "serviceName={}, database={}",
                    serviceName,
                    database.databaseName()
            );

            try (
                Connection sourceConnection =
                        openDatabaseConnection(
                            source,
                            database.databaseName()
                        )
            ) {

                total += copyStreamer.streamTables(
                            sourceConnection,
                            targetConnection,
                            sql,
                            serviceName,
                            database
                        );

            } catch (Exception e) {

                throw new MetadataReplicationException(
                        "Failed to replicate PostgreSQL tables. "
                        + "serviceName="
                        + serviceName
                        + ", database="
                        + database.databaseName(),
                        e
                );
            }
        }

        return total;
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