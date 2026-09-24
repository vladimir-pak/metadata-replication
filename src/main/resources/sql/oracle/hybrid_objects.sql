WITH allowed_schemas AS (
    SELECT u.username
    FROM dba_users u
    WHERE u.oracle_maintained = 'N'
),
mview_containers AS (
    SELECT mv.owner, mv.container_name
    FROM dba_mviews mv
    JOIN allowed_schemas s
      ON s.username = mv.owner
)
SELECT
    o.object_id AS ID,
    o.owner AS SCHEMA_NAME,
    o.object_name AS TABLE_NAME,
    CASE o.object_type
        WHEN 'TABLE' THEN 'REGULAR'
        WHEN 'VIEW' THEN 'VIEW'
        WHEN 'MATERIALIZED VIEW' THEN 'MATERIALIZED_VIEW'
    END AS TABLE_TYPE
FROM dba_objects o
JOIN allowed_schemas s
  ON s.username = o.owner
LEFT JOIN mview_containers mvc
  ON o.object_type = 'TABLE'
 AND mvc.owner = o.owner
 AND mvc.container_name = o.object_name
WHERE o.subobject_name IS NULL
  AND o.object_type IN (
      'TABLE',
      'VIEW',
      'MATERIALIZED VIEW'
  )
  AND (
      o.object_type <> 'TABLE'
      OR mvc.container_name IS NULL
  )
ORDER BY
    o.owner,
    o.object_name
