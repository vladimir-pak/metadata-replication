package com.gpb.replication.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.exclusion.ExcludeEntityType;
import com.gpb.replication.exclusion.ExcludePatternRow;

@Repository
public class ExcludePatternsRepository {

    private static final String FIND_BY_DATABASE_TYPE_SQL = """
            SELECT
                entity_type,
                pattern_expr
            FROM metadata_replication.exclude_patterns
            WHERE database_type = ?
            ORDER BY entity_type, id
            """;

    private static final String FIND_BY_DATABASE_TYPE_AND_ENTITY_TYPE_SQL = """
            SELECT
                entity_type,
                pattern_expr
            FROM metadata_replication.exclude_patterns
            WHERE database_type = ?
              AND entity_type = ?
            ORDER BY id
            """;

    private final JdbcTemplate jdbcTemplate;

    public ExcludePatternsRepository(
            @Qualifier("jdbcTemplate")
            JdbcTemplate jdbcTemplate) {

        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Основной метод для replication pipeline.
     *
     * Получает ВСЕ правила СУБД одним запросом.
     * Это быстрее двух отдельных round-trip для SCHEMA и TABLE.
     */
    public List<ExcludePatternRow> findByDatabaseType(
            DatabaseType databaseType) {

        return jdbcTemplate.query(
                FIND_BY_DATABASE_TYPE_SQL,
                this::mapRow,
                databaseType.name()
        );
    }

    /**
     * Удобен для административного API/тестов/точечного чтения.
     * В основном replication path предпочтительнее findByDatabaseType().
     */
    public List<ExcludePatternRow> findByDatabaseTypeAndEntityType(
            DatabaseType databaseType,
            ExcludeEntityType entityType) {

        return jdbcTemplate.query(
                FIND_BY_DATABASE_TYPE_AND_ENTITY_TYPE_SQL,
                this::mapRow,
                databaseType.name(),
                entityType.name()
        );
    }

    private ExcludePatternRow mapRow(
            ResultSet rs,
            int rowNum) throws SQLException {

        String rawEntityType =
                rs.getString("entity_type");

        String patternExpression =
                rs.getString("pattern_expr");

        if (rawEntityType == null
                || rawEntityType.isBlank()) {

            throw new DataIntegrityViolationException(
                    "exclude_patterns.entity_type is null/blank"
            );
        }

        if (patternExpression == null
                || patternExpression.isBlank()) {

            throw new DataIntegrityViolationException(
                    "exclude_patterns.pattern_expr is null/blank"
            );
        }

        ExcludeEntityType entityType;

        try {
            entityType =
                    ExcludeEntityType.valueOf(
                            rawEntityType
                                    .trim()
                                    .toUpperCase(Locale.ROOT)
                    );
        } catch (IllegalArgumentException e) {
            throw new DataIntegrityViolationException(
                    "Unsupported exclude_patterns.entity_type: "
                            + rawEntityType,
                    e
            );
        }

        return new ExcludePatternRow(
                entityType,
                patternExpression
        );
    }
}
