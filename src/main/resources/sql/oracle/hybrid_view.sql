SELECT
    v.owner AS SCHEMA_NAME,
    v.view_name AS TABLE_NAME,
    v.text_vc AS VIEW_DEFINITION
FROM dba_views v
JOIN dba_users u
  ON u.username = v.owner
WHERE u.oracle_maintained = 'N'
  AND v.text_vc IS NOT NULL
  AND v.text_length <= 4000
  AND (
      LENGTHB(v.text_vc) = v.text_length
      OR LENGTH(v.text_vc) = v.text_length
  )
