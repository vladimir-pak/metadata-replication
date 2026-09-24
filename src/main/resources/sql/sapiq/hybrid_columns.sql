SELECT
    c.table_id,
    c.column_id AS ordinal_position,
    TRIM(c.column_name) AS column_name,
    UPPER(REPLACE(TRIM(d.domain_name), ' ', '_')) AS data_type,
    TRIM(c.base_type_str) AS data_type_display,
    CASE
        WHEN d.domain_name IN (
            'char', 'varchar', 'long varchar',
            'binary', 'varbinary', 'long binary'
        ) THEN c.width
        WHEN d.domain_name IN ('numeric', 'decimal') THEN c.width
        ELSE NULL
    END AS data_length,
    CASE
        WHEN c.nulls = 'N' THEN 'NOT_NULL'
        ELSE NULL
    END AS column_constraint,
    CAST(r.remarks AS LONG VARCHAR) AS description
FROM SYS.SYSTABCOL c
JOIN SYS.SYSTAB t
    ON t.table_id = c.table_id
JOIN SYS.SYSUSER u
    ON u.user_id = t.creator
JOIN SYS.SYSDOMAIN d
    ON d.domain_id = c.domain_id
LEFT JOIN SYS.SYSREMARK r
    ON r.object_id = c.object_id
WHERE t.table_type IN (1, 2, 21)
  AND TRIM(u.user_name) NOT IN ('SYS', 'dbo', 'DBA')
  AND MOD(CAST(c.table_id AS BIGINT), ?) = ?
ORDER BY c.table_id, c.column_id
