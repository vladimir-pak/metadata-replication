package com.gpb.replication.postgres.utils;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.stereotype.Component;

import com.gpb.replication.postgres.config.DatabaseConfig;
import com.gpb.replication.postgres.dto.SourceDbConnections;

import lombok.RequiredArgsConstructor;

import java.io.*;
import java.sql.*;
import java.util.Iterator;
import java.util.function.Function;

@Component
@RequiredArgsConstructor
public final class PostgresCopyStreamer {

    private final DatabaseConfig databaseConfig;

    private static final Logger logger = LoggerFactory.getLogger(PostgresCopyStreamer.class);

    /**
     * Универсальный COPY из внешнего источника Postgres в целевую Postgres БД.
     *
     * @param sourceQuery SQL-запрос к мастер-системе
     * @param serializer функция сериализации ResultSet → COPY text row
     * @param copySql SQL COPY ... FROM STDIN
     */
    public long streamCopy(
            SourceDbConnections source,
            String sourceQuery,
            Function<ResultSet, String> serializer,
            String copySql
    ) throws SQLException, IOException {

        Statement stmt = null;
        ResultSet rs = null;
        InputStream inputStream = null;
        Connection targetConnection = null;

        try (Connection connection = source.getConnection()) {
            // --- 1. Streaming курсор из исходной БД ---
            stmt = connection.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY
            );
            stmt.setFetchSize(10_000);

            logger.info("[STREAM] Executing source query...");
            rs = stmt.executeQuery(sourceQuery);

            Iterator<ResultSet> rsIterator = new ResultSetIterator(rs);

            Iterator<String> lineIterator = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return rsIterator.hasNext();
                }

                @Override
                public String next() {
                    return serializer.apply(rsIterator.next());
                }
            };

            inputStream = new IteratorStringInputStream(lineIterator);

            // --- 2. Target Connection ---
            DataSourceProperties targetDbProperties = databaseConfig.getDbProperties();

            targetConnection = DriverManager.getConnection(
                targetDbProperties.getUrl(), 
                targetDbProperties.getUsername(), 
                targetDbProperties.getPassword()
            );

            PGConnection pgTarget = targetConnection.unwrap(PGConnection.class);
            CopyManager copyManager = pgTarget.getCopyAPI();

            logger.info("[STREAM] Starting COPY to target DB...");

            long rows = copyManager.copyIn(copySql, inputStream);

            logger.info("[STREAM] COPY finished. Inserted rows={}", rows);
            return rows;

        } finally {
            closeQuietly(inputStream);
            closeQuietly(rs);
            closeQuietly(stmt);
            closeQuietly(targetConnection);
        }
    }

    // --- Утилиты закрытия ---
    private static void closeQuietly(AutoCloseable c) {
        if (c != null) {
            try { c.close(); } catch (Exception ignore) {}
        }
    }

    // ========= Вспомогательный итератор ResultSet ========= //

    private static class ResultSetIterator implements Iterator<ResultSet> {
    private final ResultSet rs;
    private boolean hasNextChecked = false;

    public ResultSetIterator(ResultSet rs) {
        this.rs = rs;
    }

    @Override
    public boolean hasNext() {
        if (!hasNextChecked) {
            try {
                hasNextChecked = rs.next();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return hasNextChecked;
    }

    @Override
    public ResultSet next() {
        if (!hasNextChecked && !hasNext()) {
            throw new IllegalStateException("No more elements");
        }
        hasNextChecked = false; // сбрасываем для следующего вызова
        return rs;
    }
}

    // ========= Поток, читающий строки из итератора ========= //

    private static class IteratorStringInputStream extends InputStream {
        private final Iterator<String> iterator;
        private byte[] buffer = null;
        private int index = 0;

        public IteratorStringInputStream(Iterator<String> iterator) {
            this.iterator = iterator;
        }

        @Override
        public int read() {
            try {
                if (buffer == null || index >= buffer.length) {
                    if (!iterator.hasNext()) return -1;
                    buffer = iterator.next().getBytes("UTF-8");
                    index = 0;
                }
                return buffer[index++] & 0xFF;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

