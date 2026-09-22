SELECT
    d.oid::bigint AS id,
    d.datname     AS db_name
FROM pg_database d
WHERE d.datistemplate = false
  AND d.datallowconn = true
  AND d.datname <> 'postgres'
ORDER BY d.datname;