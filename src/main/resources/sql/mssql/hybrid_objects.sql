SELECT
    o.object_id AS ID,
    s.name AS SCHEMA_NAME,
    o.name AS TABLE_NAME,
    CASE o.type
        WHEN 'U' THEN 'REGULAR'
        WHEN 'V' THEN 'VIEW'
    END AS TABLE_TYPE,
    o.type AS RAW_TABLE_TYPE,
    CAST(ep.value AS NVARCHAR(MAX)) AS DESCRIPTION
FROM sys.objects o
INNER JOIN sys.schemas s
    ON s.schema_id = o.schema_id
LEFT JOIN sys.extended_properties ep
    ON ep.class = 1
   AND ep.major_id = o.object_id
   AND ep.minor_id = 0
   AND ep.name = N'MS_Description'
WHERE o.type IN ('U', 'V')
  AND o.is_ms_shipped = 0
  AND s.name NOT IN (
      'information_schema',
      'sys'
  )
ORDER BY o.object_id
