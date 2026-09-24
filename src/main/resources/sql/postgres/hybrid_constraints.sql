SELECT
    c.oid::bigint AS object_id,
    con.conname AS constraint_name,
    CASE
        WHEN con.contype = 'p' THEN 'PRIMARY_KEY'
        WHEN con.contype = 'u' THEN 'UNIQUE'
        WHEN con.contype = 'f' THEN 'FOREIGN_KEY'
        WHEN con.contype = 'c' THEN 'CHECK'
        WHEN con.contype = 'x' THEN 'EXCLUSION'
        ELSE 'OTHER'
    END AS constraint_type,
    a.attname AS column_name,
    k.ordinality::int AS ordinal_position
FROM pg_constraint con
JOIN pg_class c
  ON c.oid = con.conrelid
JOIN pg_namespace n
  ON n.oid = c.relnamespace
LEFT JOIN LATERAL
    unnest(con.conkey) WITH ORDINALITY AS k(attnum, ordinality)
  ON true
LEFT JOIN pg_attribute a
  ON a.attrelid = con.conrelid
 AND a.attnum = k.attnum
WHERE c.relkind IN ('r', 'v', 'm', 'p')
  AND c.relispartition = false
  AND con.contype IN ('p', 'u', 'f', 'c', 'x')
  AND n.nspname <> 'information_schema'
  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
ORDER BY
    c.oid,
    con.conname,
    k.ordinality NULLS FIRST
