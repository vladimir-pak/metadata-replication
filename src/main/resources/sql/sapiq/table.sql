SELECT
    t.table_id,
    t.object_id AS oid,
    TRIM(u.user_name) AS schema_name,
    TRIM(t.table_name) AS table_name,
    CASE
        WHEN t.table_type = 1 THEN 'REGULAR'
        WHEN t.table_type = 2 THEN 'MATERIALIZED_VIEW'
        WHEN t.table_type = 21 THEN 'VIEW'
        ELSE 'OTHER'
    END AS table_type,
    CAST(r.remarks AS LONG VARCHAR) AS description,
    CAST(src.source AS LONG VARCHAR) AS view_definition
FROM SYS.SYSTAB t
JOIN SYS.SYSUSER u ON u.user_id = t.creator
LEFT JOIN SYS.SYSREMARK r ON r.object_id = t.object_id
LEFT JOIN SYS.SYSSOURCE src ON src.object_id = t.object_id
WHERE t.table_type IN (1, 2, 21)
    AND TRIM(u.user_name) NOT IN ('SYS', 'dbo', 'DBA')
ORDER BY TRIM(u.user_name), TRIM(t.table_name)