SELECT schema_id AS oid, name AS schema_name
FROM sys.schemas
WHERE name NOT IN (
    'information_schema','sys','db_owner','db_accessadmin','db_securityadmin',
    'db_ddladmin','db_backupoperator','db_datareader','db_datawriter',
    'db_denydatareader','db_denydatawriter','guest'
);