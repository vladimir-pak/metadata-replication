package com.gpb.replication.exclusion;

/**
 * Сырая запись exclusion rule из metadata_replication.exclude_patterns.
 */
public record ExcludePatternRow(
        ExcludeEntityType entityType,
        String patternExpression) {
}
