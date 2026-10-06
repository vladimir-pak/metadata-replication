/* Do not concatenate in SQL or remove whitespace between syscomments rows.
   Java reads the fragments in their original order and suppresses hidden text. */
SELECT
    u.name AS SCHEMA_NAME,
    v.name AS TABLE_NAME,
    c.colid2 AS TEXT_ORDER_HIGH,
    c.colid AS TEXT_ORDER_LOW,
    c.status AS TEXT_STATUS,
    c.text AS VIEW_DEFINITION
FROM dbo.sysobjects v
INNER JOIN dbo.sysusers u ON u.uid = v.uid
LEFT JOIN dbo.syscomments c
    ON c.id = v.id
   AND c.number = 0
   AND c.texttype = 0
WHERE v.type = 'V'
ORDER BY u.name, v.name, c.colid2, c.colid
