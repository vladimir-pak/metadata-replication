WITH allowed_schemas AS (
    SELECT u.username
    FROM dba_users u
    WHERE u.oracle_maintained = 'N'
)

SELECT
    o.object_id AS ID,
    v.owner AS SCHEMA_NAME,
    v.view_name AS TABLE_NAME,

    'VIEW' AS TABLE_TYPE,

    CAST(NULL AS VARCHAR2(1)) AS DESCRIPTION,

    col_data.COLUMNS_JSON,

    CAST(NULL AS VARCHAR2(1))
        AS TABLE_CONSTRAINTS_JSON,

    /*
     * LONG.
     *
     * Обязательно последняя колонка ResultSet.
     */
    v.text AS VIEW_DEFINITION

FROM dba_views v

JOIN allowed_schemas s
  ON s.username = v.owner

JOIN dba_objects o
  ON o.owner = v.owner
 AND o.object_name = v.view_name
 AND o.object_type = 'VIEW'
 AND o.subobject_name IS NULL

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

                'dataLength' VALUE
                    c.data_length,

                'constraint' VALUE
                    CASE c.nullable
                        WHEN 'N' THEN 'NOT_NULL'
                        ELSE 'NULLABLE'
                    END,

                'ordinalPosition' VALUE
                    c.column_id
            )
            ORDER BY c.column_id
            RETURNING CLOB
        ) AS COLUMNS_JSON

    FROM dba_tab_columns c

    WHERE c.owner = v.owner
      AND c.table_name = v.view_name
) col_data

WHERE
       v.text_vc IS NULL
    OR v.text_length > 4000
    OR LENGTHB(v.text_vc) <> v.text_length