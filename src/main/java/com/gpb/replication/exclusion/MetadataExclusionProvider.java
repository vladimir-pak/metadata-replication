package com.gpb.replication.exclusion;

import com.gpb.replication.enums.DatabaseType;

/**
 * Extension point для источника exclusion rules.
 *
 * Сейчас реализация читает PostgreSQL metadata DB.
 * В дальнейшем можно без изменения pipeline добавить:
 * - cached provider;
 * - Vault/config provider;
 * - remote provider.
 */
public interface MetadataExclusionProvider {

    MetadataExclusionRules load(
            DatabaseType databaseType);
}
