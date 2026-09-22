SELECT 
    d.dbid AS oid,
    SYS_CONTEXT('USERENV', 'SERVICE_NAME') as db_name
FROM v$database d