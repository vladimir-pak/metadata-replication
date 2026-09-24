SELECT
    t.table_id,
    CAST(src.source AS LONG VARCHAR) AS view_definition
FROM SYS.SYSTAB t
JOIN SYS.SYSUSER u
    ON u.user_id = t.creator
LEFT JOIN SYS.SYSSOURCE src
    ON src.object_id = t.object_id
WHERE t.table_type IN (2, 21)
  AND TRIM(u.user_name) NOT IN ('SYS', 'dbo', 'DBA')
ORDER BY t.table_id
