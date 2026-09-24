SELECT
    c.owner AS SCHEMA_NAME,
    c.table_name AS TABLE_NAME,
    c.column_name AS COLUMN_NAME,
    c.data_type AS DATA_TYPE,
    c.data_length AS DATA_LENGTH,
    c.data_precision AS DATA_PRECISION,
    c.data_scale AS DATA_SCALE,
    c.nullable AS NULLABLE,
    c.column_id AS ORDINAL_POSITION
FROM dba_tab_columns c
JOIN dba_users u
  ON u.username = c.owner
WHERE u.oracle_maintained = 'N'
ORDER BY
    c.owner,
    c.table_name,
    c.column_id
