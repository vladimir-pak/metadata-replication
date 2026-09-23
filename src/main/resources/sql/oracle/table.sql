WITH allowed_schemas AS (
    SELECT u.username
    FROM dba_users u
    WHERE u.oracle_maintained = 'N'
)

SELECT
    o.object_id AS ID,
    t.owner AS SCHEMA_NAME,
    t.table_name AS TABLE_NAME,

    'REGULAR' AS TABLE_TYPE,

    /*
     * Здесь нет LONG.
     * Поле оставляем для совместимости с serializeTable().
     */
    CAST(NULL AS VARCHAR2(1)) AS VIEW_DEFINITION,

    CAST(NULL AS VARCHAR2(1)) AS DESCRIPTION,

    col_data.COLUMNS_JSON,

    cons_data.TABLE_CONSTRAINTS_JSON

FROM dba_tables t

JOIN allowed_schemas s
  ON s.username = t.owner

JOIN dba_objects o
  ON o.owner = t.owner
 AND o.object_name = t.table_name
 AND o.object_type = 'TABLE'
 AND o.subobject_name IS NULL

/*
 * Materialized View имеет внутреннюю physical table.
 * Не хотим загрузить её второй раз как REGULAR.
 */
LEFT JOIN dba_mviews mv
  ON mv.owner = t.owner
 AND mv.container_name = t.table_name

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

    WHERE c.owner = t.owner
      AND c.table_name = t.table_name
) col_data

OUTER APPLY (
    SELECT
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'constraintType' VALUE
                    CASE ac.constraint_type
                        WHEN 'P' THEN 'PRIMARY_KEY'
                        WHEN 'R' THEN 'FOREIGN_KEY'
                        WHEN 'U' THEN 'UNIQUE'
                        ELSE 'OTHER'
                    END,

                'columns' VALUE (
                    SELECT
                        JSON_ARRAYAGG(
                            acc.column_name
                            ORDER BY acc.position
                            RETURNING CLOB
                        )

                    FROM dba_cons_columns acc

                    WHERE acc.owner = ac.owner
                      AND acc.constraint_name = ac.constraint_name
                      AND acc.table_name = ac.table_name
                ) FORMAT JSON
            )
            ORDER BY ac.constraint_name
            RETURNING CLOB
        ) AS TABLE_CONSTRAINTS_JSON

    FROM dba_constraints ac

    WHERE ac.owner = t.owner
      AND ac.table_name = t.table_name
      AND ac.constraint_type IN (
          'P',
          'R',
          'U'
      )
) cons_data

WHERE mv.mview_name IS NULL