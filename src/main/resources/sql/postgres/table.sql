SELECT
c.oid,
n.nspname as schema_name,
c.relname as table_name,
obj_description(c.oid, 'pg_class') as description,
jsonb_build_object(
    'tableType',
    CASE 
        WHEN c.relkind = 'r' THEN 'REGULAR'
        WHEN c.relkind = 'v' THEN 'VIEW'
        WHEN c.relkind = 'm' THEN 'MATERIALIZED_VIEW'
        WHEN c.relkind = 'p' THEN 'REGULAR'
        ELSE 'OTHER'
    END,
    'viewDefinition',
    CASE 
        WHEN c.relkind IN ('v', 'm') THEN 
            (SELECT pg_get_viewdef(c.oid, true))
        ELSE NULL
    END,
    'columns',
    COALESCE(
        (SELECT jsonb_agg(
            jsonb_build_object(
                'ordinalPosition', a.attnum,
                'name', a.attname,
                'dataType', replace(upper(split_part(format_type(a.atttypid, a.atttypmod), '(', 1)), ' ', '_'),
                'dataTypeDisplay', format_type(a.atttypid, a.atttypmod),
                'dataLength',
                CASE
                    WHEN a.atttypid IN (1042, 1043)
                        AND a.atttypmod >= 4
                        THEN a.atttypmod - 4

                    WHEN a.atttypid IN (1083, 1114, 1184, 1266)
                        THEN CASE
                            WHEN a.atttypmod >= 0
                                THEN a.atttypmod & 65535
                            ELSE 0
                        END

                    ELSE NULL
                END,
                'precision',
                CASE
                    WHEN a.atttypid = 1700
                        AND a.atttypmod >= 4
                        THEN ((a.atttypmod - 4) >> 16) & 65535
                    ELSE NULL
                END,
                'scale',
                CASE
                    WHEN a.atttypid = 1700
                        AND a.atttypmod >= 4
                        THEN (a.atttypmod - 4) & 65535
                    ELSE NULL
                END,
                'constraint', CASE WHEN a.attnotnull = true THEN 'NOT_NULL' ELSE null END,
                'description', col_description(a.attrelid, a.attnum)
            ) ORDER BY a.attnum
        )
        FROM pg_attribute a 
        WHERE a.attrelid = c.oid 
        AND a.attnum > 0 
        AND NOT a.attisdropped),
        '[]'::jsonb
    ),
    'tableConstraints',
    COALESCE(
        (SELECT jsonb_agg(
            jsonb_build_object(
                'columns', (
                    SELECT jsonb_agg(
                        a.attname
                        ORDER BY k.ordinality
                    )
                    FROM unnest(con.conkey)
                    WITH ORDINALITY AS k(attnum, ordinality)
                    JOIN pg_attribute a
                    ON a.attrelid = con.conrelid
                    AND a.attnum = k.attnum
                ),
                'constraintType',
                CASE 
                    WHEN con.contype = 'p' THEN 'PRIMARY_KEY'
                    WHEN con.contype = 'u' THEN 'UNIQUE'
                    WHEN con.contype = 'f' THEN 'FOREIGN_KEY'
                    WHEN con.contype = 'c' THEN 'CHECK'
                    WHEN con.contype = 'x' THEN 'EXCLUSION'
                    ELSE 'OTHER'
                END
            ) ORDER BY con.conname
        )
        FROM pg_constraint con
        WHERE con.conrelid = c.oid
        AND con.contype IN ('p', 'u', 'f', 'c', 'x')),
        '[]'::jsonb
    )
) as table_structure
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm', 'p')
AND c.relispartition = false
AND n.nspname <> 'information_schema'
AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
ORDER BY n.nspname, c.relname;