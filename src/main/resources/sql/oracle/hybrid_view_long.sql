SELECT
    v.owner AS SCHEMA_NAME,
    v.view_name AS TABLE_NAME,
    v.text AS VIEW_DEFINITION
FROM dba_views v
JOIN dba_users u
  ON u.username = v.owner
WHERE u.oracle_maintained = 'N'
  AND (
      v.text_vc IS NULL
      OR v.text_length IS NULL
      OR v.text_length > 4000
      OR (
          LENGTHB(v.text_vc) <> v.text_length
          AND LENGTH(v.text_vc) <> v.text_length
      )
  )
