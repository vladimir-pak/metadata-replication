SELECT
    n.oid::bigint AS id,
    n.nspname     AS schema_name
FROM pg_namespace n
WHERE n.nspname <> 'information_schema'
AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
ORDER BY n.nspname;