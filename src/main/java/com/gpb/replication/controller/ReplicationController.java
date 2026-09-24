package com.gpb.replication.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.replication.cef.SvoiApiLog;
import com.gpb.replication.dto.ReplicationRequestDto;
import com.gpb.replication.dto.SourceConnection;
import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.enums.ReplicationPipeline;
import com.gpb.replication.service.ConnectionService;
import com.gpb.replication.service.IngestionMetricService;
import com.gpb.replication.service.ReplicationService;
import com.gpb.replication.service.ReplicationServiceRegistry;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Tag(
    name = "Replication",
    description = "API запуска репликации"
)
@Slf4j
public class ReplicationController {

    private final ConnectionService connectionService;
    private final ReplicationServiceRegistry replicationServiceRegistry;
    private final IngestionMetricService ingestionMetricService;

    @Value("${replication.oracle.pipeline:STANDARD}")
    private String defaultOraclePipeline;

    @PostMapping("/replication/start")
    @Operation(summary = "Запуск репликации по наименованию сервиса")
    @SvoiApiLog(functionName = "StartReplication")
    public ResponseEntity<String> startReplication(
            @RequestBody ReplicationRequestDto dto) {

        try {
            SourceConnection source = connectionService.getConn(
                    dto.getServiceName()
            );

            if (source.getDbType() == null
                    || source.getDbType().isBlank()) {

                throw new IllegalArgumentException(
                        "Database type is not configured for service: "
                                + dto.getServiceName()
                );
            }

            DatabaseType databaseType = DatabaseType.valueOf(
                    source.getDbType()
                            .trim()
                            .toUpperCase(Locale.ROOT)
            );

            ReplicationPipeline pipeline = resolvePipeline(
                    databaseType,
                    dto.getPipeline()
            );

            ReplicationService service = replicationServiceRegistry.get(
                    databaseType,
                    pipeline
            );

            return startInternal(
                    service,
                    source,
                    databaseType,
                    pipeline,
                    dto.isAsync()
            );

        } catch (IllegalArgumentException e) {
            return ResponseEntity
                    .badRequest()
                    .body(e.getMessage());

        } catch (Exception e) {
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(
                            "Failed to start replication: "
                                    + e.getMessage()
                    );
        }
    }

    private ReplicationPipeline resolvePipeline(
            DatabaseType databaseType,
            ReplicationPipeline requested) {

        if (requested != null) {
            return requested;
        }

        if (databaseType == DatabaseType.ORACLE) {
            ReplicationPipeline configured =
                    ReplicationPipeline.from(defaultOraclePipeline);

            return configured != null
                    ? configured
                    : ReplicationPipeline.STANDARD;
        }

        return ReplicationPipeline.STANDARD;
    }

    private ResponseEntity<String> startInternal(
            ReplicationService service,
            SourceConnection source,
            DatabaseType databaseType,
            ReplicationPipeline pipeline,
            boolean async) {

        String runId = null;

        try {
            runId = ingestionMetricService
                    .createRunIfNotExecuting(
                            source.getServiceName()
                    );

            if (async) {
                service.startAsync(
                        source,
                        runId
                );
            } else {
                service.start(
                        source,
                        runId
                );
            }

            return ResponseEntity.ok(
                    String.format(
                            "Replication %s. DBType=%s, Pipeline=%s, ServiceName=%s",
                            async ? "queued" : "finished",
                            databaseType.name(),
                            pipeline.name(),
                            source.getServiceName()
                    )
            );

        } catch (IllegalArgumentException e) {
            if (runId != null) {
                skipSafely(runId);
            }
            return ResponseEntity
                    .badRequest()
                    .body(e.getMessage());

        } catch (Exception e) {
            if (runId != null) {
                skipSafely(runId);
            }
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(
                            "Failed to start replication: "
                                    + e.getMessage()
                    );
        }
    }

    private void skipSafely(String runId) {
        try {
            ingestionMetricService.skipRemaining(runId);
        } catch (Exception e) {
            log.error(
                    "Failed to mark ingestion jobs as SKIPPED. runId={}",
                    runId,
                    e
            );
        }
    }
}
