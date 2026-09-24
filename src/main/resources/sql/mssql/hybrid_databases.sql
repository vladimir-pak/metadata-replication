SELECT
    d.database_id AS ID,
    d.name AS DB_NAME
FROM sys.databases d
WHERE d.name NOT IN (
    'tempdb',
    'model',
    'msdb'
)
  AND d.state = 0
ORDER BY d.database_id
