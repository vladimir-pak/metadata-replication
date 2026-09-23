WITH allowed_schemas AS (
    SELECT u.username
    FROM dba_users u
    WHERE u.oracle_maintained = 'N'
),

view_objects AS (
    SELECT
        o.object_id AS id,
        o.owner,
        o.object_name,

        CASE o.object_type
            WHEN 'VIEW'
                THEN 'VIEW'

            WHEN 'MATERIALIZED VIEW'
                THEN 'MATERIALIZED_VIEW'
        END AS table_type

    FROM dba_objects o

    JOIN allowed_schemas s
      ON s.username = o.owner

    WHERE o.object_type IN (
        'VIEW',
        'MATERIALIZED VIEW'
    )
      AND o.subobject_name IS NULL
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

    JOIN view_objects v
      ON v.owner = c.owner
     AND v.object_name = c.table_name

    GROUP BY
        c.owner,
        c.table_name
)

SELECT
    vo.id AS ID,
    vo.owner AS SCHEMA_NAME,
    vo.object_name AS TABLE_NAME,
    vo.table_type AS TABLE_TYPE,

    CAST(NULL AS VARCHAR2(1)) AS DESCRIPTION,

    c.columns_json AS COLUMNS_JSON,

    CAST(NULL AS VARCHAR2(1))
        AS TABLE_CONSTRAINTS_JSON,

    /*
     * LONG columns специально находятся ПОСЛЕДНИМИ.
     *
     * Oracle JDBC должен прочитать их
     * последовательно.
     */
    v.text AS VIEW_DEFINITION,

    mv.query AS MVIEW_DEFINITION

FROM view_objects vo

LEFT JOIN dba_views v
  ON vo.table_type = 'VIEW'
 AND v.owner = vo.owner
 AND v.view_name = vo.object_name

LEFT JOIN dba_mviews mv
  ON vo.table_type = 'MATERIALIZED_VIEW'
 AND mv.owner = vo.owner
 AND mv.mview_name = vo.object_name

LEFT JOIN columns_agg c
  ON c.owner = vo.owner
 AND c.table_name = vo.object_name