package com.gpb.replication.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

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

        String serviceName = source.getServiceName();

        validateSource(source);

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
                "Starting Oracle metadata replication: serviceName={}",
                serviceName
        );

        try (
            Connection sourceConnection =
                    sourceConnectionFactory.open(source)
        ) {
            /*
             * Oracle:
             * один database context в рамках connection.
             */
            DatabaseReplicationContext database =
                    resolveDatabase(
                        sourceConnection,
                        sqlDatabase,
                        serviceName
                    );

            ReplicationStats stats =
                    inTargetTransaction(
                            targetConnection -> {
                                
                                long databases =
                                    ingestionMetricService.execute(
                                        runId,
                                        IngestionMetricJob.DATABASE_REPLICATION,
                                        counter -> {
                                            /*
                                            * DELETE включаем в первый job,
                                            * чтобы ошибка очистки тоже
                                            * сделала DATABASE_REPLICATION FAILED.
                                            */
                                            deleteServiceSnapshot(
                                                serviceName,
                                                DatabaseType.ORACLE
                                            );

                                            long count =
                                                    streamDatabase(
                                                        sourceConnection,
                                                        targetConnection,
                                                        sqlDatabase,
                                                        serviceName
                                                    );

                                            if (count != 1) {
                                                throw new MetadataReplicationException(
                                                        "Expected exactly one "
                                                        + "Oracle database, but received "
                                                        + count
                                                );
                                            }

                                            counter.success(count);

                                            return count;
                                        }
                                    );

                            long schemas =
                                    ingestionMetricService.execute(
                                        runId,
                                        IngestionMetricJob.SCHEMA_REPLICATION,
                                        counter -> {
                                            long count =
                                                    streamSchema(
                                                        sourceConnection,
                                                        targetConnection,
                                                        sqlSchema,
                                                        serviceName,
                                                        database
                                                    );

                                            counter.success(count);

                                            return count;
                                        }
                                    );

                            long tables =
                                    ingestionMetricService.execute(
                                        runId,
                                        IngestionMetricJob.TABLE_REPLICATION,
                                        counter -> {
                                            long count =
                                                    streamTable(
                                                        sourceConnection,
                                                        targetConnection,
                                                        sqlTable,
                                                        serviceName,
                                                        database
                                                    );

                                            counter.success(count);

                                            return count;
                                        }
                                    );

                            return new ReplicationStats(
                                databases,
                                schemas,
                                tables
                            );
                        }
                    );

            long elapsedMs = (System.nanoTime() - started) / 1_000_000;

            log.info(
                    "Oracle metadata replication completed: "
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
                    "Oracle metadata replication failed: serviceName={}",
                    serviceName,
                    e
            );
            throw e;
        } catch (Exception e) {
            log.error(
                    "Oracle metadata replication failed: serviceName={}",
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

    private long streamDatabase(
            Connection sourceConnection,
            Connection targetConnection,
            String sql,
            String serviceName) {

        return copyStreamer.streamDatabases(
            sourceConnection,
            targetConnection,
            sql,
            serviceName
        );
    }

    private long streamSchema(
            Connection sourceConnection,
            Connection targetConnection,
            String sql,
            String serviceName,
            DatabaseReplicationContext database) {

        return copyStreamer.streamSchemas(
            sourceConnection,
            targetConnection,
            sql,
            serviceName,
            database
        );
    }

    private long streamTable(
            Connection sourceConnection,
            Connection targetConnection,
            String sql,
            String serviceName,
            DatabaseReplicationContext database) {

        return copyStreamer.streamTables(
            sourceConnection,
            targetConnection,
            sql,
            serviceName,
            database
        );
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
                ResultSet rs =
                        statement.executeQuery()
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