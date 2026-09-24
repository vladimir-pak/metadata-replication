SELECT
    mv.owner AS SCHEMA_NAME,
    mv.mview_name AS TABLE_NAME,
    mv.query AS VIEW_DEFINITION
FROM dba_mviews mv
JOIN dba_users u
  ON u.username = mv.owner
WHERE u.oracle_maintained = 'N'
