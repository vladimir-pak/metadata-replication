SELECT
    c.oid::bigint AS object_id,
    a.attnum::int AS ordinal_position,
    a.attname AS column_name,
    replace(
        upper(
            split_part(
                format_type(a.atttypid, a.atttypmod),
                '(',
                1
            )
        ),
        ' ',
        '_'
    ) AS data_type,
    format_type(a.atttypid, a.atttypmod) AS data_type_display,
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
    END AS data_length,
    CASE
        WHEN a.atttypid = 1700
             AND a.atttypmod >= 4
            THEN ((a.atttypmod - 4) >> 16) & 65535
        ELSE NULL
    END AS data_precision,
    CASE
        WHEN a.atttypid = 1700
             AND a.atttypmod >= 4
            THEN (a.atttypmod - 4) & 65535
        ELSE NULL
    END AS data_scale,
    CASE
        WHEN a.attnotnull THEN 'NOT_NULL'
        ELSE NULL
    END AS column_constraint,
    col_description(a.attrelid, a.attnum) AS description
FROM pg_class c
JOIN pg_namespace n
  ON n.oid = c.relnamespace
JOIN pg_attribute a
  ON a.attrelid = c.oid
 AND a.attnum > 0
 AND NOT a.attisdropped
WHERE c.relkind IN ('r', 'v', 'm', 'p')
  AND c.relispartition = false
  AND n.nspname <> 'information_schema'
  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
  AND mod(c.oid::bigint, CAST(? AS bigint)) = CAST(? AS bigint)
ORDER BY
    c.oid,
    a.attnum
