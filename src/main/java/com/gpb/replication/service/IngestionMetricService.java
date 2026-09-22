package com.gpb.replication.service;

import java.util.UUID;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.gpb.replication.exceptions.IngestionAlreadyRunningException;
import com.gpb.replication.metrics.MetricCounter;
import com.gpb.replication.metrics.enums.IngestionMetricJob;
import com.gpb.replication.repository.MetadataIngestionMetricRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class IngestionMetricService {

    private final MetadataIngestionMetricRepository repository;

    private final TransactionTemplate requiresNewTransaction;

    @Value("${spring.application.name:metadata-ingestion}")
    private String appName;

    public IngestionMetricService(
            MetadataIngestionMetricRepository repository,
            @Qualifier ("transactionManager")
            PlatformTransactionManager transactionManager) {

        this.repository = repository;

        this.requiresNewTransaction =
                new TransactionTemplate(transactionManager);

        this.requiresNewTransaction.setPropagationBehavior(
                TransactionDefinition.PROPAGATION_REQUIRES_NEW
        );
    }

    @Transactional
    public String createRunIfNotExecuting(
            String serviceName) {

        repository.lockService(serviceName);

        if (repository.isExecuting(
                serviceName,
                appName)) {

            throw new IngestionAlreadyRunningException(
                    serviceName
            );
        }

        String runId = UUID.randomUUID().toString();

        repository.createQueuedJobs(
                runId,
                serviceName,
                appName
        );

        log.info(
                "Created ingestion run. runId={}, serviceName={}",
                runId,
                serviceName
        );

        return runId;
    }

    public <T> T execute(
            String runId,
            IngestionMetricJob job,
            Function<MetricCounter, T> action) {

        MetricCounter counter =
                new MetricCounter();

        requiresNew(() ->
                repository.markRunning(
                        runId,
                        job
                )
        );

        log.info(
                "Ingestion job started. runId={}, job={}",
                runId,
                job
        );

        try {

            T result =
                    action.apply(counter);

            requiresNew(() ->
                    repository.markDone(
                            runId,
                            job,
                            counter
                    )
            );

            log.info(
                    "Ingestion job completed. "
                            + "runId={}, job={}, "
                            + "successCount={}, errorCount={}",
                    runId,
                    job,
                    counter.getSuccessCount(),
                    counter.getErrorCount()
            );

            return result;

        } catch (RuntimeException e) {

            if (counter.getErrorCount() == 0) {
                counter.error();
            }

            try {

                requiresNew(() ->
                        repository.markFailed(
                                runId,
                                job,
                                counter
                        )
                );

            } catch (RuntimeException metricException) {

                e.addSuppressed(metricException);

                log.error(
                        "Failed to update job to FAILED. "
                                + "runId={}, job={}",
                        runId,
                        job,
                        metricException
                );
            }

            throw e;
        }
    }

    public void skipRemaining(
            String runId) {

        requiresNew(() -> {

            int skipped =
                    repository.markQueuedAsSkipped(
                            runId
                    );

            log.info(
                    "Skipped remaining ingestion jobs. "
                            + "runId={}, count={}",
                    runId,
                    skipped
            );
        });
    }

    private void requiresNew(
            Runnable action) {

        requiresNewTransaction.executeWithoutResult(
                status -> action.run()
        );
    }
}
