SELECT
    o.id AS ID,
    u.name AS SCHEMA_NAME,
    o.name AS TABLE_NAME,
    CASE o.type
        WHEN 'U' THEN 'REGULAR'
        WHEN 'V' THEN 'VIEW'
    END AS TABLE_TYPE,
    o.type AS RAW_TABLE_TYPE,
    convert(varchar(255), NULL) AS DESCRIPTION
FROM dbo.sysobjects o
INNER JOIN dbo.sysusers u ON u.uid = o.uid
WHERE o.type IN ('U', 'V')
ORDER BY u.name, o.name
