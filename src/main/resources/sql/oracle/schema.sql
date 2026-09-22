SELECT 
    user_id AS "oid", 
    username AS "schema_name"
FROM all_users
WHERE (oracle_maintained = 'N' OR oracle_maintained IS NULL)
    AND username NOT LIKE 'OPS$%%'
ORDER BY username