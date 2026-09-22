package com.gpb.replication.cef.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.gpb.replication.cef.SvoiLogger;
import com.gpb.replication.cef.enums.SvoiSeverityEnum;
import com.gpb.replication.cef.repository.LogPartitionRepository;
import com.gpb.replication.cef.service.LogFileService;

/**
 * Планировщик для чистки логов и создания партиций в базе
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LogScheduler {
    private final SvoiLogger svoiCustomLogger;
    private final LogPartitionRepository logPartitionRepository;
    private final LogFileService cefLogger;

    @Scheduled(cron = "${logs-database.task-create-partition}")
    public void createPartition() {
        logPartitionRepository.createTodayPartition();
    }

    @Scheduled(cron = "${clean-database-logs.task-cleaner-schedule}")
    public void cleanPartition() {
        logPartitionRepository.dropOldPartitions();
    }

    @Scheduled(cron = "${clean-database-logs.task-cleaner-schedule}")
    public void cleanupOldLogs() {
        cefLogger.rotateLogFile();
        cefLogger.cleanupOldLogs();
        svoiCustomLogger.sendInternal(
                "cleanLogs",
                "Clean Log Files",
                "Cleaned old log files",
                SvoiSeverityEnum.ONE
        );
    }
}
