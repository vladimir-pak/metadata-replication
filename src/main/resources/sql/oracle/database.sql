SELECT 
    d.dbid AS id,
    SYS_CONTEXT('USERENV', 'SERVICE_NAME') as db_name
FROM v$database d