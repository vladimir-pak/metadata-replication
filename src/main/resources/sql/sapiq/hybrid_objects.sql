SELECT
    t.table_id AS table_id,
    t.object_id AS id,
    TRIM(u.user_name) AS schema_name,
    TRIM(t.table_name) AS table_name,
    CASE
        WHEN t.table_type = 1 THEN 'REGULAR'
        WHEN t.table_type = 2 THEN 'MATERIALIZED_VIEW'
        WHEN t.table_type = 21 THEN 'VIEW'
        ELSE 'OTHER'
    END AS table_type,
    t.table_type AS raw_table_type,
    CAST(r.remarks AS LONG VARCHAR) AS description
FROM SYS.SYSTAB t
JOIN SYS.SYSUSER u
    ON u.user_id = t.creator
LEFT JOIN SYS.SYSREMARK r
    ON r.object_id = t.object_id
WHERE t.table_type IN (1, 2, 21)
  AND TRIM(u.user_name) NOT IN ('SYS', 'dbo', 'DBA')
ORDER BY t.table_id
