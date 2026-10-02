SELECT
    v.owner AS SCHEMA_NAME,
    v.view_name AS TABLE_NAME,
    v.text AS VIEW_DEFINITION
FROM dba_views v
JOIN dba_users u
  ON u.username = v.owner
WHERE u.oracle_maintained = 'N'