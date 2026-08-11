package com.gpb.replication.postgres.repository;

import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ExcludePatternsRepository {

    @Qualifier("dataSource")
    private final JdbcTemplate jdbcTemplate;

    public ExcludePatternsRepository(@Qualifier("dataSource") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<String> getTablePatterns() {
        String sql = """
            SELECT pattern_expr
            FROM postgres_metadata.exclude_patterns
            WHERE entity_type = 'TABLE'
        """;
        try {
            return jdbcTemplate.queryForList(sql, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null; // или throw new EntityNotFoundException("Schema not found with oid: " + oid);
        }
    }

    public List<String> getSchemaPatterns() {
        String sql = """
            SELECT pattern_expr
            FROM postgres_metadata.exclude_patterns
            WHERE entity_type = 'SCHEMA'
        """;
        try {
            return jdbcTemplate.queryForList(sql, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null; // или throw new EntityNotFoundException("Schema not found with oid: " + oid);
        }
    }
}
