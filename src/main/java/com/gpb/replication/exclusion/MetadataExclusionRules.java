package com.gpb.replication.exclusion;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.exceptions.MetadataReplicationException;

/**
 * Immutable compiled snapshot exclusion-правил на ОДИН replication run.
 *
 * В hot path:
 * - нет JDBC;
 * - нет Pattern.compile();
 * - нет Stream API;
 * - только EnumMap lookup + цикл по уже скомпилированным Pattern.
 *
 * Семантика соответствует старому String.matches(pattern):
 * regex должен совпасть со всей строкой (Matcher.matches()).
 */
public final class MetadataExclusionRules {

    private final DatabaseType databaseType;

    private final EnumMap<ExcludeEntityType, Pattern[]> patternsByType;

    private MetadataExclusionRules(
            DatabaseType databaseType,
            EnumMap<ExcludeEntityType, Pattern[]> patternsByType) {

        this.databaseType = databaseType;
        this.patternsByType = patternsByType;
    }

    public static MetadataExclusionRules empty(
            DatabaseType databaseType) {

        return new MetadataExclusionRules(
                databaseType,
                new EnumMap<>(ExcludeEntityType.class)
        );
    }

    public static MetadataExclusionRules compile(
            DatabaseType databaseType,
            List<ExcludePatternRow> rows) {

        if (rows == null || rows.isEmpty()) {
            return empty(databaseType);
        }

        EnumMap<ExcludeEntityType, List<Pattern>> mutable =
                new EnumMap<>(ExcludeEntityType.class);

        for (ExcludePatternRow row : rows) {

            if (row == null) {
                continue;
            }

            String expression =
                    row.patternExpression();

            if (expression == null
                    || expression.isBlank()) {

                continue;
            }

            Pattern pattern;

            try {
                pattern = Pattern.compile(expression);
            } catch (PatternSyntaxException e) {
                throw new MetadataReplicationException(
                        "Invalid metadata exclusion regex. "
                                + "databaseType="
                                + databaseType
                                + ", entityType="
                                + row.entityType()
                                + ", pattern="
                                + expression,
                        e
                );
            }

            mutable.computeIfAbsent(
                    row.entityType(),
                    ignored -> new ArrayList<>()
            ).add(pattern);
        }

        EnumMap<ExcludeEntityType, Pattern[]> compiled =
                new EnumMap<>(ExcludeEntityType.class);

        for (Map.Entry<ExcludeEntityType, List<Pattern>> entry
                : mutable.entrySet()) {

            compiled.put(
                    entry.getKey(),
                    entry.getValue()
                            .toArray(Pattern[]::new)
            );
        }

        return new MetadataExclusionRules(
                databaseType,
                compiled
        );
    }

    public boolean isExcluded(
            ExcludeEntityType entityType,
            String value) {

        if (value == null) {
            return false;
        }

        Pattern[] patterns =
                patternsByType.get(entityType);

        if (patterns == null
                || patterns.length == 0) {

            return false;
        }

        for (Pattern pattern : patterns) {

            if (pattern.matcher(value).matches()) {
                return true;
            }
        }

        return false;
    }

    public boolean isSchemaExcluded(
            String schemaName) {

        return isExcluded(
                ExcludeEntityType.SCHEMA,
                schemaName
        );
    }

    public boolean isTableExcluded(
            String tableName) {

        return isExcluded(
                ExcludeEntityType.TABLE,
                tableName
        );
    }

    public int count(
            ExcludeEntityType entityType) {

        Pattern[] patterns =
                patternsByType.get(entityType);

        return patterns == null
                ? 0
                : patterns.length;
    }

    public int totalCount() {

        int count = 0;

        for (Pattern[] patterns
                : patternsByType.values()) {

            count += patterns.length;
        }

        return count;
    }

    public DatabaseType databaseType() {
        return databaseType;
    }
}
