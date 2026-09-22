package com.gpb.replication.cef.service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.gpb.replication.cef.properties.CefLoggingProperties;

import lombok.RequiredArgsConstructor;

/**
 * Класс для работы с файлом логов в формате cef
 */
@Service
@RequiredArgsConstructor
public class LogFileService {

    private static final Logger log = LoggerFactory.getLogger(LogFileService.class);
    private final CefLoggingProperties properties;

    private static final ZoneId ZONE = ZoneId.systemDefault();
    private final Object lock = new Object();

    /**
     * Запись строки в оперативный лог
     */
    public void writeToFile(LocalDateTime created, String cefLog) {
        Path path = properties.getPath().resolve(properties.getFileName());
        synchronized (lock) {
            try {
                Files.createDirectories(path.getParent());
                String line = cefLog + System.lineSeparator();
                Files.writeString(path, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                log.error("Ошибка при записи CEF-лога в файл {}", path, e);
            }
        }
    }

    /**
     * Очистка старых архивов по retentionDays из application.yaml
     */
    public void cleanupOldLogs() {
        Path dir = properties.getPath().getParent();
        if (dir == null || !Files.exists(dir)) return;

        LocalDate threshold = LocalDate.now(ZONE).minusDays(properties.getRetentionDays());
        Pattern pattern = buildPattern();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, properties.getFileName() + ".*.tar.gz")) {
            for (Path p : stream) {
                try {
                    Matcher m = pattern.matcher(p.getFileName().toString());
                    if (m.matches()) {
                        LocalDate fileDate = LocalDate.parse(m.group(1));
                        if (fileDate.isBefore(threshold)) {
                            Files.deleteIfExists(p);
                            log.info("Удалён старый лог: {}", p.getFileName());
                        }
                    }
                } catch (Exception ignored) {
                    log.warn("Не удалось удалить файл: {}", p.getFileName());
                }
            }
        } catch (IOException e) {
            log.error("Ошибка при очистке старых логов", e);
        }
    }

    /**
     * Ротация текущего лог-файла в архив .tar.gz
     */
    public void rotateLogFile() {
        synchronized (lock) {
            Path logFile = properties.getPath().resolve(properties.getFileName());
            if (!Files.exists(logFile)) {
                log.warn("Файл {} отсутствует, пропуск ротации", logFile);
                return;
            }

            LocalDate date = LocalDate.now(ZONE).minusDays(1);
            Path rotated = logFile.resolveSibling(properties.getFileName() + "." + date);

            // 1. Переименовать текущий лог
            try {
                Files.move(logFile, rotated, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                log.error("Не удалось переименовать лог перед архивированием", e);
                return;
            }

            // 2. Архивировать переименованный файл
            Path archivePath = rotated.resolveSibling(rotated.getFileName() + ".tar.gz");
            try (OutputStream fOut = Files.newOutputStream(archivePath);
                 GZIPOutputStream gzOut = new GZIPOutputStream(fOut);
                 TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {

                TarArchiveEntry entry = new TarArchiveEntry(rotated.toFile());
                entry.setName(rotated.getFileName().toString());
                tOut.putArchiveEntry(entry);
                Files.copy(rotated, tOut);
                tOut.closeArchiveEntry();
                tOut.finish();

            } catch (Exception e) {
                log.error("Ошибка при архивации лога {}", e);
                return;
            }

            // 3. Удалить переименованный файл после успешного архива
            try {
                Files.deleteIfExists(rotated);
            } catch (IOException e) {
                log.warn("Не удалось удалить временный лог {}", rotated);
            }
        }
    }

    /**
     * Построение регулярного шаблона для поиска старых архивов
     */
    private Pattern buildPattern() {
        return Pattern.compile("^" + Pattern.quote(properties.getFileName()) +
                "\\.(\\d{4}-\\d{2}-\\d{2})\\.tar\\.gz$");
    }
}
