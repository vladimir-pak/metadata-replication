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
import com.gpb.replication.exclusion.MetadataExclusionProvider;
import com.gpb.replication.exclusion.MetadataExclusionRules;
import com.gpb.replication.metrics.MetricCounter;
import com.gpb.replication.metrics.enums.IngestionMetricJob;
import com.gpb.replication.repository.MetadataRepository;
import com.gpb.replication.service.IngestionMetricService;
import com.gpb.replication.service.ReplicationService;
import com.gpb.replication.stream.mssql.MssqlJdbcUrl;
import com.gpb.replication.stream.mssql.MssqlMetadataCopyStreamer;
import com.gpb.replication.utils.MetadataFqn;
import com.gpb.replication.utils.SqlQueryProvider;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class MssqlReplicationImpl
        extends ReplicationService {

    private final SqlQueryProvider sqlQueryProvider;
    private final MssqlMetadataCopyStreamer copyStreamer;
    private final MetadataExclusionProvider metadataExclusionProvider;

    public MssqlReplicationImpl(
            SqlQueryProvider sqlQueryProvider,
            @Qualifier("jdbcTemplate")
            JdbcTemplate jdbcTemplate,
            @Qualifier("transactionManager")
            PlatformTransactionManager transactionManager,
            MetadataRepository metadataRepository,
            SourceJdbcConnectionFactory sourceConnectionFactory,
            IngestionMetricService ingestionMetricService,
            MssqlMetadataCopyStreamer copyStreamer,
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
        this.metadataExclusionProvider = metadataExclusionProvider;
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.MSSQL;
    }

    @Override
    protected void execute(
            SourceConnection source,
            String runId) {

        long started = System.nanoTime();
        validateSource(source);

        MetadataExclusionRules exclusionRules =
                metadataExclusionProvider.load(
                        DatabaseType.MSSQL
                );

        String serviceName = source.getServiceName();

        String sqlDatabase = sqlQueryProvider.getQuery(
                DatabaseType.MSSQL,
                MetadataType.DATABASE
        );
        String sqlSchema = sqlQueryProvider.getQuery(
                DatabaseType.MSSQL,
                MetadataType.SCHEMA
        );
        String sqlTable = sqlQueryProvider.getQuery(
                DatabaseType.MSSQL,
                MetadataType.TABLE
        );

        log.info(
                "Starting MSSQL STANDARD metadata replication. serviceName={}",
                serviceName
        );

        List<DatabaseReplicationContext> databases;
        long databaseCount;

        try (
                Connection discoveryConnection =
                        sourceConnectionFactory.open(source)
        ) {
            databases = discoverDatabases(
                    discoveryConnection,
                    sqlDatabase,
                    serviceName
            );

            if (databases.isEmpty()) {
                throw new MetadataReplicationException(
                        "No MSSQL databases found. serviceName=" + serviceName
                );
            }

            databaseCount = ingestionMetricService.execute(
                    runId,
                    IngestionMetricJob.DATABASE_REPLICATION,
                    counter -> replicateDatabases(
                            discoveryConnection,
                            sqlDatabase,
                            serviceName,
                            databases,
                            counter
                    )
            );

        } catch (MetadataReplicationException e) {
            throw e;

        } catch (Exception e) {
            throw new MetadataReplicationException(
                    "MSSQL STANDARD database discovery failed: " + serviceName,
                    e
            );
        }

        boolean databaseSnapshotCommitted =
                databaseCount == databases.size();

        List<String> currentDatabaseNames = databases.stream()
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
                                DatabaseType.MSSQL,
                                counter
                        );
                    }

                    return replicateSchemas(
                            source,
                            sqlSchema,
                            serviceName,
                            databases,
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
                                DatabaseType.MSSQL,
                                counter
                        );
                    }

                    return replicateTables(
                            source,
                            sqlTable,
                            serviceName,
                            databases,
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
                "MSSQL STANDARD metadata replication completed. "
                        + "serviceName={}, databases={}, schemas={}, tablesAndViews={}, elapsedMs={}",
                serviceName,
                stats.databases(),
                stats.schemas(),
                stats.tables(),
                elapsedMs(started)
        );
    }

    private long replicateDatabases(
            Connection discoveryConnection,
            String sql,
            String serviceName,
            List<DatabaseReplicationContext> databases,
            MetricCounter counter) {

        try {
            long count = inTargetTransaction(
                    targetConnection -> {
                        metadataRepository.deleteDatabaseMetadata(
                                serviceName,
                                DatabaseType.MSSQL
                        );

                        long copied = copyStreamer.streamDatabases(
                                discoveryConnection,
                                targetConnection,
                                sql,
                                serviceName
                        );

                        if (copied != databases.size()) {
                            throw new MetadataReplicationException(
                                    "MSSQL database count mismatch. expected="
                                            + databases.size()
                                            + ", copied=" + copied
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
                    "MSSQL STANDARD replication error. "
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
            List<DatabaseReplicationContext> databases,
            MetricCounter counter,
            MetadataExclusionRules exclusionRules) {

        long total = 0;

        for (DatabaseReplicationContext database : databases) {
            String databaseName = database.databaseName();

            try (
                    Connection sourceConnection =
                            openDatabaseConnection(source, databaseName)
            ) {
                long count = inTargetTransaction(
                        targetConnection -> {
                            metadataRepository.deleteSchemaMetadata(
                                    serviceName,
                                    databaseName,
                                    DatabaseType.MSSQL
                            );

                            return copyStreamer.streamSchemas(
                                    sourceConnection,
                                    targetConnection,
                                    sql,
                                    serviceName,
                                    database,
                                    exclusionRules
                            );
                        }
                );

                counter.success(count);
                total += count;

            } catch (Exception e) {
                counter.error();
                log.error(
                        "MSSQL STANDARD replication error. "
                                + "entityType=SCHEMA, entityName={}.*, database={}, serviceName={}",
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
            MetricCounter counter,
            MetadataExclusionRules exclusionRules) {

        long total = 0;

        for (DatabaseReplicationContext database : databases) {
            String databaseName = database.databaseName();

            try (
                    Connection sourceConnection =
                            openDatabaseConnection(source, databaseName)
            ) {
                long count = inTargetTransaction(
                        targetConnection -> {
                            metadataRepository.deleteTableMetadata(
                                    serviceName,
                                    databaseName,
                                    DatabaseType.MSSQL
                            );

                            return copyStreamer.streamTables(
                                    sourceConnection,
                                    targetConnection,
                                    sql,
                                    serviceName,
                                    database,
                                    exclusionRules
                            );
                        }
                );

                counter.success(count);
                total += count;

            } catch (Exception e) {
                counter.error();
                log.error(
                        "MSSQL STANDARD replication error. "
                                + "entityType=TABLE, entityName={}.*, database={}, serviceName={}",
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
                PreparedStatement statement = connection.prepareStatement(
                        sql,
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY
                );
                ResultSet rs = statement.executeQuery()
        ) {
            while (rs.next()) {
                String databaseName = rs.getString("DATNAME");

                if (databaseName == null || databaseName.isBlank()) {
                    throw new MetadataReplicationException(
                            "MSSQL DATNAME is empty"
                    );
                }

                if (!names.add(databaseName)) {
                    throw new MetadataReplicationException(
                            "Duplicate MSSQL database: " + databaseName
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
                    "Failed to discover MSSQL databases",
                    e
            );
        }
    }

    private Connection openDatabaseConnection(
            SourceConnection source,
            String databaseName) {

        return sourceConnectionFactory.open(
                source,
                url -> MssqlJdbcUrl.withDatabase(
                        url,
                        databaseName
                )
        );
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
                || !DatabaseType.MSSQL.name().equalsIgnoreCase(
                        source.getDbType()
                )) {
            throw new MetadataReplicationException(
                    "Expected MSSQL connection, actual="
                            + source.getDbType()
            );
        }
    }

    private long elapsedMs(long started) {
        return (System.nanoTime() - started) / 1_000_000;
    }
}
