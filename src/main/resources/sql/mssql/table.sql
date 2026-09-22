SELECT 
    t.object_id AS oid,
    s.name AS schema_name,
    t.name AS table_name,
    CAST(ep.value AS NVARCHAR(MAX)) AS description,
    (
        SELECT
            case when t.type = 'U' then 'REGULAR'
                when t.type = 'V' then 'VIEW'
                end AS tableType,
            sm.definition AS viewDefinition,
            (
                SELECT 
                    c.name AS [name],
                    CONCAT(DB_NAME(), '.', s.name, '.', t.name, '.', c.name) AS [fqn],
                    ty.name AS [dataType],
                    ty.name +
                        CASE 
                            WHEN ty.name IN ('varchar','nvarchar','char','nchar') 
                                THEN '(' + IIF(c.max_length = -1, 'MAX', CAST(c.max_length AS VARCHAR)) + ')'
                            WHEN ty.name IN ('decimal','numeric') 
                                THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
                            ELSE ''
                        END AS [dataTypeDisplay],
                    c.max_length AS [dataLength],
                    ISNULL(CAST(epc.value AS NVARCHAR(MAX)), '') AS [description],
                    c.column_id AS [ordinalPosition],
                    IIF(c.is_nullable = 0, 'NOT_NULL', 'NULLABLE') AS [constraint]
                FROM sys.columns c
                INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                LEFT JOIN sys.extended_properties epc 
                    ON epc.major_id = c.object_id 
                    AND epc.minor_id = c.column_id 
                    AND epc.class = 1
                WHERE c.object_id = t.object_id
                FOR JSON PATH
            ) AS columns,
            (
                SELECT
                    JSON_QUERY(
                        '[' +
                        STUFF((
                            SELECT
                                ',' + QUOTENAME(kcu2.COLUMN_NAME, '"')
                            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
                            WHERE kcu2.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                            AND kcu2.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                            AND kcu2.TABLE_NAME = tc.TABLE_NAME
                            AND kcu2.TABLE_SCHEMA = tc.TABLE_SCHEMA
                            ORDER BY kcu2.ORDINAL_POSITION
                            FOR XML PATH(''), TYPE
                        ).value('.', 'nvarchar(max)'), 1, 1, '')
                        + ']'
                    ) AS columns,
                    CASE
                        WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 'PRIMARY_KEY'
                        WHEN tc.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN 'FOREIGN_KEY'
                        WHEN tc.CONSTRAINT_TYPE = 'UNIQUE' THEN 'UNIQUE'
                        WHEN tc.CONSTRAINT_TYPE = 'CHECK' THEN 'CHECK'
                        ELSE tc.CONSTRAINT_TYPE
                    END AS constraintType
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                WHERE tc.TABLE_NAME = t.name
                AND tc.TABLE_SCHEMA = s.name
                FOR JSON PATH
            ) AS tableConstraints
                ,
            t.type AS rawTableType
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ) AS table_structure
FROM sys.objects t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN sys.extended_properties ep 
    ON ep.major_id = t.object_id AND ep.minor_id = 0 AND ep.class = 1
LEFT JOIN sys.sql_modules sm 
    ON sm.object_id = t.object_id
WHERE s.name NOT IN ('information_schema','sys')
    AND t.type IN ('U', 'V'); -- включаем таблицы и вьюхи