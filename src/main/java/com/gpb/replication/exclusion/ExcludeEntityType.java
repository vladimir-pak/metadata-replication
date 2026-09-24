package com.gpb.replication.exclusion;

/**
 * Тип metadata-сущности, к которой применяется exclusion regex.
 *
 * Для добавления нового типа достаточно:
 * 1. добавить enum value;
 * 2. разрешить значение в БД;
 * 3. применить rules.isExcluded(...) в нужном месте pipeline.
 */
public enum ExcludeEntityType {
    SCHEMA,
    TABLE
}
