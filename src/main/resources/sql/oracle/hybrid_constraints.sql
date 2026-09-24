SELECT
    ac.owner AS SCHEMA_NAME,
    ac.table_name AS TABLE_NAME,
    ac.constraint_name AS CONSTRAINT_NAME,
    ac.constraint_type AS CONSTRAINT_TYPE,
    acc.column_name AS COLUMN_NAME,
    acc.position AS POSITION
FROM dba_constraints ac
JOIN dba_cons_columns acc
  ON acc.owner = ac.owner
 AND acc.table_name = ac.table_name
 AND acc.constraint_name = ac.constraint_name
JOIN dba_users u
  ON u.username = ac.owner
WHERE u.oracle_maintained = 'N'
  AND ac.constraint_type IN ('P', 'R', 'U')
ORDER BY
    ac.owner,
    ac.table_name,
    ac.constraint_name,
    acc.position
