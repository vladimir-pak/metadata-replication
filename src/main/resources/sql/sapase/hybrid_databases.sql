/* Enumerate all user databases, including temporarily unavailable ones.
   Keeping them in discovery prevents stale cleanup from deleting their snapshots. */
SELECT
    d.dbid AS ID,
    d.name AS DB_NAME
FROM master.dbo.sysdatabases d
WHERE d.name NOT IN (
    'master', 'model', 'tempdb', 'sybsystemdb', 'sybsystemprocs',
    'sybsecurity', 'dbccdb', 'sybmgmtdb'
)
  AND (isnull(d.status3, 0) & 256) = 0
ORDER BY d.dbid
