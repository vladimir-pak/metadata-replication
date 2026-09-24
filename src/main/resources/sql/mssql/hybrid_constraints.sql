SELECT
    x.SCHEMA_NAME,
    x.TABLE_NAME,
    x.CONSTRAINT_NAME,
    x.CONSTRAINT_TYPE,
    x.COLUMN_NAME,
    x.ORDINAL_POSITION
FROM (
    /* PRIMARY KEY + UNIQUE constraints */
    SELECT
        s.name AS SCHEMA_NAME,
        t.name AS TABLE_NAME,
        kc.name AS CONSTRAINT_NAME,
        CASE kc.type
            WHEN 'PK' THEN 'PRIMARY_KEY'
            WHEN 'UQ' THEN 'UNIQUE'
        END AS CONSTRAINT_TYPE,
        c.name AS COLUMN_NAME,
        ic.key_ordinal AS ORDINAL_POSITION
    FROM sys.key_constraints kc
    INNER JOIN sys.tables t
        ON t.object_id = kc.parent_object_id
    INNER JOIN sys.schemas s
        ON s.schema_id = t.schema_id
    INNER JOIN sys.index_columns ic
        ON ic.object_id = kc.parent_object_id
       AND ic.index_id = kc.unique_index_id
       AND ic.key_ordinal > 0
    INNER JOIN sys.columns c
        ON c.object_id = ic.object_id
       AND c.column_id = ic.column_id
    WHERE t.is_ms_shipped = 0
      AND s.name NOT IN ('information_schema', 'sys')

    UNION ALL

    /* FOREIGN KEY constraints */
    SELECT
        s.name AS SCHEMA_NAME,
        t.name AS TABLE_NAME,
        fk.name AS CONSTRAINT_NAME,
        'FOREIGN_KEY' AS CONSTRAINT_TYPE,
        c.name AS COLUMN_NAME,
        fkc.constraint_column_id AS ORDINAL_POSITION
    FROM sys.foreign_keys fk
    INNER JOIN sys.tables t
        ON t.object_id = fk.parent_object_id
    INNER JOIN sys.schemas s
        ON s.schema_id = t.schema_id
    INNER JOIN sys.foreign_key_columns fkc
        ON fkc.constraint_object_id = fk.object_id
    INNER JOIN sys.columns c
        ON c.object_id = fkc.parent_object_id
       AND c.column_id = fkc.parent_column_id
    WHERE t.is_ms_shipped = 0
      AND s.name NOT IN ('information_schema', 'sys')

    UNION ALL

    /* CHECK constraints. COLUMN_NAME is NULL for table-level checks. */
    SELECT
        s.name AS SCHEMA_NAME,
        t.name AS TABLE_NAME,
        cc.name AS CONSTRAINT_NAME,
        'CHECK' AS CONSTRAINT_TYPE,
        c.name AS COLUMN_NAME,
        CAST(1 AS INT) AS ORDINAL_POSITION
    FROM sys.check_constraints cc
    INNER JOIN sys.tables t
        ON t.object_id = cc.parent_object_id
    INNER JOIN sys.schemas s
        ON s.schema_id = t.schema_id
    LEFT JOIN sys.columns c
        ON c.object_id = cc.parent_object_id
       AND c.column_id = cc.parent_column_id
       AND cc.parent_column_id > 0
    WHERE t.is_ms_shipped = 0
      AND s.name NOT IN ('information_schema', 'sys')
) x
ORDER BY
    x.SCHEMA_NAME,
    x.TABLE_NAME,
    x.CONSTRAINT_NAME,
    x.ORDINAL_POSITION
