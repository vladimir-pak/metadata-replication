SELECT
    c.oid::bigint AS id,
    n.nspname AS schema_name,
    c.relname AS table_name,
    CASE
        WHEN c.relkind = 'r' THEN 'REGULAR'
        WHEN c.relkind = 'p' THEN 'REGULAR'
        WHEN c.relkind = 'v' THEN 'VIEW'
        WHEN c.relkind = 'm' THEN 'MATERIALIZED_VIEW'
        ELSE 'OTHER'
    END AS table_type,
    obj_description(c.oid, 'pg_class') AS description
FROM pg_class c
JOIN pg_namespace n
  ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm', 'p')
  AND c.relispartition = false
  AND n.nspname <> 'information_schema'
  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
ORDER BY c.oid
