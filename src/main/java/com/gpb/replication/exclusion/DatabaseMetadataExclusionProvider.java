package com.gpb.replication.exclusion;

import org.springframework.stereotype.Service;

import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.repository.ExcludePatternsRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class DatabaseMetadataExclusionProvider
        implements MetadataExclusionProvider {

    private final ExcludePatternsRepository repository;

    @Override
    public MetadataExclusionRules load(
            DatabaseType databaseType) {

        long started = System.nanoTime();

        MetadataExclusionRules rules =
                MetadataExclusionRules.compile(
                        databaseType,
                        repository.findByDatabaseType(
                                databaseType
                        )
                );

        log.info(
                "Metadata exclusion rules loaded. "
                        + "databaseType={}, total={}, "
                        + "schemaPatterns={}, tablePatterns={}, "
                        + "elapsedMs={}",
                databaseType,
                rules.totalCount(),
                rules.count(
                        ExcludeEntityType.SCHEMA
                ),
                rules.count(
                        ExcludeEntityType.TABLE
                ),
                (System.nanoTime() - started)
                        / 1_000_000
        );

        return rules;
    }
}
