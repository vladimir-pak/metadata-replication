SELECT
    s.name AS SCHEMA_NAME,
    o.name AS TABLE_NAME,
    c.name AS COLUMN_NAME,
    ty.name AS DATA_TYPE,
    c.max_length AS DATA_LENGTH,
    c.precision AS DATA_PRECISION,
    c.scale AS DATA_SCALE,
    c.column_id AS ORDINAL_POSITION,
    c.is_nullable AS IS_NULLABLE,
    CAST(ep.value AS NVARCHAR(MAX)) AS DESCRIPTION
FROM sys.objects o
INNER JOIN sys.schemas s
    ON s.schema_id = o.schema_id
INNER JOIN sys.columns c
    ON c.object_id = o.object_id
INNER JOIN sys.types ty
    ON ty.user_type_id = c.user_type_id
LEFT JOIN sys.extended_properties ep
    ON ep.class = 1
   AND ep.major_id = c.object_id
   AND ep.minor_id = c.column_id
   AND ep.name = N'MS_Description'
WHERE o.type IN ('U', 'V')
  AND o.is_ms_shipped = 0
  AND s.name NOT IN (
      'information_schema',
      'sys'
  )
  AND (CONVERT(BIGINT, o.object_id) % ?) = ?
ORDER BY
    o.object_id,
    c.column_id
