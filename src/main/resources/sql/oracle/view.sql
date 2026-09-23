WITH allowed_schemas AS (
    SELECT u.username
    FROM dba_users u
    WHERE u.oracle_maintained = 'N'
),

view_objects AS (
    SELECT
        o.object_id AS ID,
        o.owner,
        o.object_name,

        CASE o.object_type
            WHEN 'VIEW'
                THEN 'VIEW'

            WHEN 'MATERIALIZED VIEW'
                THEN 'MATERIALIZED_VIEW'
        END AS TABLE_TYPE

    FROM dba_objects o

    JOIN allowed_schemas s
      ON s.username = o.owner

    WHERE o.object_type IN (
        'VIEW',
        'MATERIALIZED VIEW'
    )
      AND o.subobject_name IS NULL
)

SELECT
    vo.ID AS ID,
    vo.owner AS SCHEMA_NAME,
    vo.object_name AS TABLE_NAME,
    vo.TABLE_TYPE AS TABLE_TYPE,

    /*
     * До LONG должны находиться все обычные
     * и CLOB columns.
     */
    CAST(NULL AS VARCHAR2(1)) AS DESCRIPTION,

    col_data.COLUMNS_JSON,

    CAST(NULL AS VARCHAR2(1))
        AS TABLE_CONSTRAINTS_JSON,

    /*
     * ВАЖНО:
     * DBA_VIEWS.TEXT имеет тип LONG.
     *
     * Две LONG columns находятся последними
     * и serializeView() должен читать их
     * именно в этом порядке.
     */
    v.text AS VIEW_DEFINITION,

    mv.query AS MVIEW_DEFINITION

FROM view_objects vo

LEFT JOIN dba_views v
  ON vo.TABLE_TYPE = 'VIEW'
 AND v.owner = vo.owner
 AND v.view_name = vo.object_name

LEFT JOIN dba_mviews mv
  ON vo.TABLE_TYPE = 'MATERIALIZED_VIEW'
 AND mv.owner = vo.owner
 AND mv.mview_name = vo.object_name

OUTER APPLY (
    SELECT
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
            )
            ORDER BY c.column_id
            RETURNING CLOB
        ) AS COLUMNS_JSON

    FROM dba_tab_columns c

    WHERE c.owner = vo.owner
      AND c.table_name = vo.object_name
) col_data