SELECT database_id AS oid, name AS datname
FROM sys.databases
WHERE name NOT IN ('tempdb', 'model', 'msdb')
    AND state = 0 -- ONLINE;