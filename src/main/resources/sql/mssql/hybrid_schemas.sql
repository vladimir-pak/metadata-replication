SELECT
    s.schema_id AS ID,
    s.name AS SCHEMA_NAME
FROM sys.schemas s
WHERE s.name NOT IN (
    'information_schema',
    'sys',
    'db_owner',
    'db_accessadmin',
    'db_securityadmin',
    'db_ddladmin',
    'db_backupoperator',
    'db_datareader',
    'db_datawriter',
    'db_denydatareader',
    'db_denydatawriter',
    'guest'
)
ORDER BY s.schema_id
