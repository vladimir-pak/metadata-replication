package com.gpb.replication.repository;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.replication.metrics.MetricCounter;
import com.gpb.replication.metrics.enums.IngestionMetricJob;
import com.gpb.replication.metrics.enums.IngestionStatus;

@Repository
public class MetadataIngestionMetricRepository {

    private final JdbcTemplate jdbcTemplate;

    public MetadataIngestionMetricRepository(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbcTemplate) {

        this.jdbcTemplate = jdbcTemplate;
    }

    private static final String INSERT_JOB = """
            INSERT INTO public.metadata_ingestion (
                run_id,
                job_name,
                appname,
                status,
                service_name,
                success_count,
                error_count
            )
            VALUES (?, ?, ?, ?, ?, 0, 0)
            """;

    /**
     * Берём transaction-level advisory lock для serviceName.
     * Lock существует только до конца текущей транзакции.
     * Для одного serviceName одновременно только одна транзакция
     * сможет пройти дальше.
     */
    public void lockService(String serviceName) {

        jdbcTemplate.query(
                """
                SELECT pg_advisory_xact_lock(
                    hashtext('metadata-ingestion'),
                    hashtext(?)
                )
                """,
                ps -> ps.setString(
                        1,
                        serviceName
                ),
                rs -> null
        );
    }

    /**
     * Проверяем наличие активного основного ingestion.
     *
     * VIEW_PARSING здесь намеренно исключён:
     * он выполняется другим сервисом и не должен блокировать
     * следующий metadata ingestion.
     */
    public boolean isExecuting(
            String serviceName,
            String appName) {

        Boolean exists = jdbcTemplate.queryForObject(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM public.metadata_ingestion
                    WHERE service_name = ?
                      AND appname = ?
                      AND status IN (?, ?)
                )
                """,
                Boolean.class,
                serviceName,
                appName,
                IngestionStatus.QUEUE.name(),
                IngestionStatus.RUNNING.name()
        );

        return Boolean.TRUE.equals(exists);
    }

    /**
     * Создаём все основные процессы конкретного ingestion
     * сразу со статусом QUEUE.
     * VIEW_PARSING создаётся отдельно после завершения ingestion.
     */
    public void createQueuedJobs(
            String runId,
            String serviceName,
            String appName) {

        List<Object[]> batchArgs =
                Arrays.stream(IngestionMetricJob.values())
                        .map(
                                job -> new Object[]{
                                        runId,
                                        job.name(),
                                        appName,
                                        IngestionStatus.QUEUE.name(),
                                        serviceName
                                }
                        )
                        .toList();

        jdbcTemplate.batchUpdate(
                INSERT_JOB,
                batchArgs
        );
    }

    /**
     * QUEUE -> RUNNING
     */
    public void markRunning(
            String runId,
            IngestionMetricJob job) {

        int updated = jdbcTemplate.update(
                """
                UPDATE public.metadata_ingestion
                   SET status = ?,
                       start_dttm = now()
                 WHERE run_id = ?
                   AND job_name = ?
                   AND status = ?
                """,
                IngestionStatus.RUNNING.name(),
                runId,
                job.name(),
                IngestionStatus.QUEUE.name()
        );

        checkSingleUpdate(
                updated,
                runId,
                job,
                IngestionStatus.RUNNING
        );
    }

    /**
     * RUNNING -> DONE
     */
    public void markDone(
            String runId,
            IngestionMetricJob job,
            MetricCounter counter) {

        int updated = jdbcTemplate.update(
                """
                UPDATE public.metadata_ingestion
                   SET status = ?,
                       success_count = ?,
                       error_count = ?,
                       end_dttm = now()
                 WHERE run_id = ?
                   AND job_name = ?
                   AND status = ?
                """,
                IngestionStatus.DONE.name(),
                counter.getSuccessCount(),
                counter.getErrorCount(),
                runId,
                job.name(),
                IngestionStatus.RUNNING.name()
        );

        checkSingleUpdate(
                updated,
                runId,
                job,
                IngestionStatus.DONE
        );
    }

    /**
     * RUNNING -> FAILED
     */
    public void markFailed(
            String runId,
            IngestionMetricJob job,
            MetricCounter counter) {

        int updated = jdbcTemplate.update(
                """
                UPDATE public.metadata_ingestion
                   SET status = ?,
                       success_count = ?,
                       error_count = ?,
                       end_dttm = now()
                 WHERE run_id = ?
                   AND job_name = ?
                   AND status = ?
                """,
                IngestionStatus.FAILED.name(),
                counter.getSuccessCount(),
                counter.getErrorCount(),
                runId,
                job.name(),
                IngestionStatus.RUNNING.name()
        );

        checkSingleUpdate(
                updated,
                runId,
                job,
                IngestionStatus.FAILED
        );
    }

    /**
     * Все jobs, которые ещё не стартовали:
     *
     * QUEUE -> SKIPPED
     */
    public int markQueuedAsSkipped(
            String runId) {

        return jdbcTemplate.update(
                """
                UPDATE public.metadata_ingestion
                   SET status = ?,
                       end_dttm = now()
                 WHERE run_id = ?
                   AND status = ?
                """,
                IngestionStatus.SKIPPED.name(),
                runId,
                IngestionStatus.QUEUE.name()
        );
    }

    private void checkSingleUpdate(
            int updated,
            String runId,
            IngestionMetricJob job,
            IngestionStatus targetStatus) {

        if (updated != 1) {

            throw new IllegalStateException(
                    String.format(
                            "Cannot change ingestion status. " +
                            "runId=%s, job=%s, " +
                            "targetStatus=%s, updated=%d",
                            runId,
                            job,
                            targetStatus,
                            updated
                    )
            );
        }
    }

    /**
     * Создание месячной партиции.
     */
    public void createMetricPartition() {

        String table =
                "public.metadata_ingestion";

        YearMonth currentMonth =
                YearMonth.now();

        String partitionName =
                String.format(
                        "public.metadata_ingestion_%s",
                        currentMonth.format(
                                DateTimeFormatter.ofPattern(
                                        "yyyy_MM"
                                )
                        )
                );

        LocalDate from =
                currentMonth.atDay(1);

        LocalDate to =
                currentMonth
                        .plusMonths(1)
                        .atDay(1);

        String sql =
                String.format(
                        """
                        CREATE TABLE IF NOT EXISTS %s
                        PARTITION OF %s
                        FOR VALUES FROM ('%s 00:00:00')
                                 TO ('%s 00:00:00')
                        """,
                        partitionName,
                        table,
                        from,
                        to
                );

        jdbcTemplate.execute(sql);
    }
}
