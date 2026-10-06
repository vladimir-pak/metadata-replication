SELECT
    u.name AS SCHEMA_NAME,
    o.name AS TABLE_NAME,
    c.name AS COLUMN_NAME,
    ty.name AS DATA_TYPE,
    c.length AS DATA_LENGTH,
    c.prec AS DATA_PRECISION,
    c.scale AS DATA_SCALE,
    c.colid AS ORDINAL_POSITION,
    CASE WHEN (c.status & 8) = 8 THEN 1 ELSE 0 END AS IS_NULLABLE,
    convert(varchar(255), NULL) AS DESCRIPTION
FROM dbo.sysobjects o
INNER JOIN dbo.sysusers u ON u.uid = o.uid
INNER JOIN dbo.syscolumns c ON c.id = o.id AND c.number = 0
INNER JOIN dbo.systypes ty ON ty.usertype = c.usertype
WHERE o.type IN ('U', 'V')
  AND (abs(convert(bigint, o.id)) % ?) = ?
ORDER BY u.name, o.name, c.colid
