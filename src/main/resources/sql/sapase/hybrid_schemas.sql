/* ASE object owners map to the application's schema metadata. */
SELECT
    u.uid AS ID,
    u.name AS SCHEMA_NAME
FROM dbo.sysusers u
WHERE u.suid >= 0
   OR EXISTS (
       SELECT 1
       FROM dbo.sysobjects o
       WHERE o.uid = u.uid
         AND o.type IN ('U', 'V')
   )
ORDER BY u.uid
