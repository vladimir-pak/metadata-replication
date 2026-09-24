SELECT
    c.oid::bigint AS object_id,
    pg_get_viewdef(c.oid, true) AS view_definition
FROM pg_class c
JOIN pg_namespace n
  ON n.oid = c.relnamespace
WHERE c.relkind IN ('v', 'm')
  AND c.relispartition = false
  AND n.nspname <> 'information_schema'
  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
ORDER BY c.oid
