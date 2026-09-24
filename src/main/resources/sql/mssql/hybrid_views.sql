SELECT
    s.name AS SCHEMA_NAME,
    v.name AS TABLE_NAME,
    sm.definition AS VIEW_DEFINITION
FROM sys.views v
INNER JOIN sys.schemas s
    ON s.schema_id = v.schema_id
LEFT JOIN sys.sql_modules sm
    ON sm.object_id = v.object_id
WHERE v.is_ms_shipped = 0
  AND s.name NOT IN (
      'information_schema',
      'sys'
  )
ORDER BY v.object_id
