WITH allowed_schemas AS (
    SELECT u.username
    FROM dba_users u
    WHERE u.oracle_maintained = 'N'
),

regular_tables AS (
    SELECT
        o.object_id AS id,
        t.owner,
        t.table_name
    FROM dba_tables t

    JOIN allowed_schemas s
      ON s.username = t.owner

    JOIN dba_objects o
      ON o.owner = t.owner
     AND o.object_name = t.table_name
     AND o.object_type = 'TABLE'
     AND o.subobject_name IS NULL

    /*
     * Materialized View физически имеет container table.
     * Не реплицируем её второй раз как REGULAR.
     */
    LEFT JOIN dba_mviews mv
      ON mv.owner = t.owner
     AND mv.container_name = t.table_name

    WHERE mv.mview_name IS NULL
),

columns_agg AS (
    SELECT
        c.owner,
        c.table_name,

        JSON_ARRAYAGG(
            JSON_OBJECT(
                'name' VALUE c.column_name,

                'dataType' VALUE c.data_type,

                'dataTypeDisplay' VALUE
                    CASE
                        WHEN c.data_type IN (
                            'VARCHAR2',
                            'CHAR',
                            'NVARCHAR2',
                            'NCHAR'
                        )
                        THEN
                            c.data_type
                            || '('
                            || c.data_length
                            || ')'

                        WHEN c.data_type = 'NUMBER'
                        THEN
                            c.data_type
                            ||
                            CASE
                                WHEN c.data_precision IS NOT NULL
                                THEN
                                    '('
                                    || c.data_precision
                                    ||
                                    CASE
                                        WHEN c.data_scale IS NOT NULL
                                        THEN ',' || c.data_scale
                                        ELSE ''
                                    END
                                    || ')'
                                ELSE ''
                            END

                        ELSE c.data_type
                    END,

                'dataLength' VALUE c.data_length,

                'constraint' VALUE
                    CASE c.nullable
                        WHEN 'N' THEN 'NOT_NULL'
                        ELSE 'NULLABLE'
                    END,

                'ordinalPosition' VALUE c.column_id

                RETURNING CLOB
            )
            ORDER BY c.column_id
            RETURNING CLOB
        ) AS columns_json

    FROM dba_tab_columns c

    JOIN regular_tables t
      ON t.owner = c.owner
     AND t.table_name = c.table_name

    GROUP BY
        c.owner,
        c.table_name
),

constraint_columns_agg AS (
    SELECT
        cc.owner,
        cc.table_name,
        cc.constraint_name,

        JSON_ARRAYAGG(
            cc.column_name
            ORDER BY cc.position
            RETURNING CLOB
        ) AS columns_json

    FROM dba_cons_columns cc

    JOIN regular_tables t
      ON t.owner = cc.owner
     AND t.table_name = cc.table_name

    JOIN dba_constraints c
      ON c.owner = cc.owner
     AND c.table_name = cc.table_name
     AND c.constraint_name = cc.constraint_name

    WHERE c.constraint_type IN (
        'P',
        'R',
        'U'
    )

    GROUP BY
        cc.owner,
        cc.table_name,
        cc.constraint_name
),

constraints_agg AS (
    SELECT
        c.owner,
        c.table_name,

        JSON_ARRAYAGG(
            JSON_OBJECT(
                'constraintType' VALUE
                    CASE c.constraint_type
                        WHEN 'P' THEN 'PRIMARY_KEY'
                        WHEN 'R' THEN 'FOREIGN_KEY'
                        WHEN 'U' THEN 'UNIQUE'
                        ELSE 'OTHER'
                    END,

                'columns' VALUE
                    cc.columns_json FORMAT JSON

                RETURNING CLOB
            )
            ORDER BY c.constraint_name
            RETURNING CLOB
        ) AS table_constraints_json

    FROM dba_constraints c

    JOIN regular_tables t
      ON t.owner = c.owner
     AND t.table_name = c.table_name

    JOIN constraint_columns_agg cc
      ON cc.owner = c.owner
     AND cc.table_name = c.table_name
     AND cc.constraint_name = c.constraint_name

    WHERE c.constraint_type IN (
        'P',
        'R',
        'U'
    )

    GROUP BY
        c.owner,
        c.table_name
)

SELECT
    t.id AS ID,
    t.owner AS SCHEMA_NAME,
    t.table_name AS TABLE_NAME,

    'REGULAR' AS TABLE_TYPE,

    /*
     * Должно находиться именно здесь:
     * текущий serializeTable читает
     * VIEW_DEFINITION перед DESCRIPTION.
     */
    CAST(NULL AS VARCHAR2(1)) AS VIEW_DEFINITION,

    CAST(NULL AS VARCHAR2(1)) AS DESCRIPTION,

    c.columns_json AS COLUMNS_JSON,

    tc.table_constraints_json AS TABLE_CONSTRAINTS_JSON

FROM regular_tables t

LEFT JOIN columns_agg c
  ON c.owner = t.owner
 AND c.table_name = t.table_name

LEFT JOIN constraints_agg tc
  ON tc.owner = t.owner
 AND tc.table_name = t.table_name