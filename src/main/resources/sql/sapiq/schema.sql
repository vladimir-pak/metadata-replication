SELECT DISTINCT
    u.user_id AS oid,
    TRIM(u.user_name) AS schema_name
FROM SYS.SYSUSER u
JOIN SYS.SYSTAB t ON t.creator = u.user_id
WHERE t.table_type IN (1, 2, 21)
    AND TRIM(u.user_name) NOT IN ('SYS', 'dbo', 'DBA')
ORDER BY TRIM(u.user_name)