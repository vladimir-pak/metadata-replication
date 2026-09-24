package com.gpb.replication.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.gpb.replication.connection.SourceJdbcConnectionFactory;
import com.gpb.replication.dto.SourceConnection;
import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.enums.ReplicationPipeline;
import com.gpb.replication.exceptions.MetadataReplicationException;
import com.gpb.replication.metrics.MetricCounter;
import com.gpb.replication.repository.MetadataRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j 
public abstract class ReplicationService {

    protected final JdbcTemplate mainJdbcTemplate;

    protected final MetadataRepository metadataRepository;

    protected final SourceJdbcConnectionFactory sourceConnectionFactory;

    private final TransactionTemplate targetTransactionTemplate;

    protected final IngestionMetricService ingestionMetricService;

    public abstract DatabaseType getDatabaseType();

    public ReplicationPipeline getPipeline() {
        return ReplicationPipeline.STANDARD;
    }

    protected ReplicationService(
            JdbcTemplate mainJdbcTemplate,
            PlatformTransactionManager transactionManager,
            MetadataRepository metadataRepository,
            SourceJdbcConnectionFactory sourceConnectionFactory,
            IngestionMetricService ingestionMetricService) {

        this.mainJdbcTemplate = mainJdbcTemplate;

        this.metadataRepository = metadataRepository;

        this.sourceConnectionFactory = sourceConnectionFactory;

        this.targetTransactionTemplate = new TransactionTemplate(transactionManager);

        this.ingestionMetricService = ingestionMetricService;
    }

    protected final <T> T inTargetTransaction(TargetTransactionWork<T> work) {
        T result =
                targetTransactionTemplate.execute(
                        status ->
                                mainJdbcTemplate.execute(
                                        (ConnectionCallback<T>) connection -> {
                                            try {
                                                return work.execute(
                                                        connection
                                                );
                                            } catch (SQLException e) {
                                                throw e;
                                            } catch (RuntimeException e) {
                                                throw e;
                                            } catch (Exception e) {
                                                throw new MetadataReplicationException(
                                                        "Target transaction failed",
                                                        e
                                                );
                                            }
                                        }
                                )
                );

        return Objects.requireNonNull(
            result,
            "Target transaction returned null"
        );
    }

    @Async()
    public void startAsync(
            SourceConnection source,
            String runId) {

        start(source, runId);
    }

    public final void start(
            SourceConnection source,
            String runId) {

        try {

            execute(source, runId);

        } catch (RuntimeException e) {

            try {
                ingestionMetricService.skipRemaining(runId);
            } catch (RuntimeException metricException) {

                e.addSuppressed(metricException);

                log.error(
                        "Failed to mark remaining jobs as SKIPPED. runId={}",
                        runId,
                        metricException
                );
            }

            log.error(
                    "Metadata ingestion failed. runId={}, serviceName={}",
                    runId,
                    source != null
                            ? source.getServiceName()
                            : null,
                    e
            );

            throw e;
        }
    }

    protected abstract void execute(
            SourceConnection source,
            String runId);

    protected final void deleteServiceSnapshot(
            String serviceName,
            DatabaseType databaseType) {
        /**
         * DELETE из MetadataRepository и COPY работают в одной Spring-транзакции. 
         * JdbcTemplate использует transaction-bound connection, если транзакция уже активна.
         */

        metadataRepository.deleteTableMetadata(
                serviceName,
                databaseType
        );

        metadataRepository.deleteSchemaMetadata(
                serviceName,
                databaseType
        );

        metadataRepository.deleteDatabaseMetadata(
                serviceName,
                databaseType
        );
    }

    protected final void cleanupStaleSchemas(
            String serviceName,
            List<String> databaseNames,
            DatabaseType databaseType,
            MetricCounter counter) {
        try {
            int deleted =
                    inTargetTransaction(
                            connection ->
                                    metadataRepository
                                            .deleteSchemaMetadataNotInDatabases(
                                                serviceName,
                                                databaseNames,
                                                databaseType
                                            )
                    );

            log.info(
                "Stale schema metadata cleanup committed. "
                + "serviceName={}, databaseType={}, deleted={}",
                serviceName,
                databaseType,
                deleted
            );

        } catch (Exception e) {
            counter.error();

            log.error(
                "Metadata cleanup failed. "
                + "entityType=SCHEMA, "
                + "entityName=STALE_DATABASES, "
                + "serviceName={}, databaseType={}",
                serviceName,
                databaseType,
                e
            );
        }
    }

    protected final void cleanupStaleTables(
            String serviceName,
            List<String> databaseNames,
            DatabaseType databaseType,
            MetricCounter counter) {

        try {
            int deleted =
                    inTargetTransaction(
                            connection ->
                                    metadataRepository
                                            .deleteTableMetadataNotInDatabases(
                                                serviceName,
                                                databaseNames,
                                                databaseType
                                            )
                    );

            log.info(
                "Stale table metadata cleanup committed. "
                + "serviceName={}, databaseType={}, deleted={}",
                serviceName,
                databaseType,
                deleted
            );

        } catch (Exception e) {
            counter.error();

            log.error(
                "Metadata cleanup failed. "
                + "entityType=TABLE, "
                + "entityName=STALE_DATABASES, "
                + "serviceName={}, databaseType={}",
                serviceName,
                databaseType,
                e
            );
        }
    }

    @FunctionalInterface
    protected interface TargetTransactionWork<T> {
        T execute(
                Connection targetConnection)
                throws Exception;
    }
}