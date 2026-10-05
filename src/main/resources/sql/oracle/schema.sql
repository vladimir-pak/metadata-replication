SELECT 
    user_id AS id, 
    username AS "schema_name"
FROM all_users u
WHERE u.oracle_maintained = 'N'
    AND username NOT LIKE 'OPS$%%'
ORDER BY username