SELECT
    x.SCHEMA_NAME,
    x.TABLE_NAME,
    x.CONSTRAINT_NAME,
    x.CONSTRAINT_TYPE,
    x.COLUMN_NAME,
    x.ORDINAL_POSITION
FROM (
    /* Only declarative PK/UNIQUE constraints, not standalone unique indexes.
       status2 bit 2 marks a PK/UNIQUE constraint; status bit 2048 marks PK. */
    SELECT
        u.name AS SCHEMA_NAME,
        t.name AS TABLE_NAME,
        i.name AS CONSTRAINT_NAME,
        CASE WHEN (i.status & 2048) = 2048
            THEN 'PRIMARY_KEY' ELSE 'UNIQUE' END AS CONSTRAINT_TYPE,
        index_col(t.name, i.indid, n.ORDINAL_POSITION, t.uid) AS COLUMN_NAME,
        n.ORDINAL_POSITION
    FROM dbo.sysindexes i
    INNER JOIN dbo.sysobjects t ON t.id = i.id AND t.type = 'U'
    INNER JOIN dbo.sysusers u ON u.uid = t.uid
    INNER JOIN (
        SELECT 1 AS ORDINAL_POSITION
        UNION ALL
        SELECT 2
        UNION ALL
        SELECT 3
        UNION ALL
        SELECT 4
        UNION ALL
        SELECT 5
        UNION ALL
        SELECT 6
        UNION ALL
        SELECT 7
        UNION ALL
        SELECT 8
        UNION ALL
        SELECT 9
        UNION ALL
        SELECT 10
        UNION ALL
        SELECT 11
        UNION ALL
        SELECT 12
        UNION ALL
        SELECT 13
        UNION ALL
        SELECT 14
        UNION ALL
        SELECT 15
        UNION ALL
        SELECT 16
        UNION ALL
        SELECT 17
        UNION ALL
        SELECT 18
        UNION ALL
        SELECT 19
        UNION ALL
        SELECT 20
        UNION ALL
        SELECT 21
        UNION ALL
        SELECT 22
        UNION ALL
        SELECT 23
        UNION ALL
        SELECT 24
        UNION ALL
        SELECT 25
        UNION ALL
        SELECT 26
        UNION ALL
        SELECT 27
        UNION ALL
        SELECT 28
        UNION ALL
        SELECT 29
        UNION ALL
        SELECT 30
        UNION ALL
        SELECT 31
        UNION ALL
        SELECT 32
    ) n ON n.ORDINAL_POSITION <= i.keycnt
    WHERE i.indid > 0
      AND i.indid <> 255
      AND (i.status2 & 2) = 2
      AND index_col(t.name, i.indid, n.ORDINAL_POSITION, t.uid) IS NOT NULL

    UNION ALL

    /* ASE represents the local foreign-key columns in fokey1 ... fokey16.
       Ignore mirror rows describing a foreign key owned by another database. */
    SELECT
        u.name AS SCHEMA_NAME,
        t.name AS TABLE_NAME,
        co.name AS CONSTRAINT_NAME,
        'FOREIGN_KEY' AS CONSTRAINT_TYPE,
        c.name AS COLUMN_NAME,
        n.ORDINAL_POSITION
    FROM dbo.sysreferences r
    INNER JOIN dbo.sysobjects t ON t.id = r.tableid AND t.type = 'U'
    INNER JOIN dbo.sysusers u ON u.uid = t.uid
    INNER JOIN dbo.sysobjects co ON co.id = r.constrid
    INNER JOIN (
        SELECT 1 AS ORDINAL_POSITION
        UNION ALL
        SELECT 2
        UNION ALL
        SELECT 3
        UNION ALL
        SELECT 4
        UNION ALL
        SELECT 5
        UNION ALL
        SELECT 6
        UNION ALL
        SELECT 7
        UNION ALL
        SELECT 8
        UNION ALL
        SELECT 9
        UNION ALL
        SELECT 10
        UNION ALL
        SELECT 11
        UNION ALL
        SELECT 12
        UNION ALL
        SELECT 13
        UNION ALL
        SELECT 14
        UNION ALL
        SELECT 15
        UNION ALL
        SELECT 16
        UNION ALL
        SELECT 17
        UNION ALL
        SELECT 18
        UNION ALL
        SELECT 19
        UNION ALL
        SELECT 20
        UNION ALL
        SELECT 21
        UNION ALL
        SELECT 22
        UNION ALL
        SELECT 23
        UNION ALL
        SELECT 24
        UNION ALL
        SELECT 25
        UNION ALL
        SELECT 26
        UNION ALL
        SELECT 27
        UNION ALL
        SELECT 28
        UNION ALL
        SELECT 29
        UNION ALL
        SELECT 30
        UNION ALL
        SELECT 31
        UNION ALL
        SELECT 32
    ) n ON n.ORDINAL_POSITION <= r.keycnt AND n.ORDINAL_POSITION <= 16
    INNER JOIN dbo.syscolumns c
        ON c.id = r.tableid
       AND c.number = 0
       AND c.colid = CASE n.ORDINAL_POSITION
            WHEN 1 THEN r.fokey1
            WHEN 2 THEN r.fokey2
            WHEN 3 THEN r.fokey3
            WHEN 4 THEN r.fokey4
            WHEN 5 THEN r.fokey5
            WHEN 6 THEN r.fokey6
            WHEN 7 THEN r.fokey7
            WHEN 8 THEN r.fokey8
            WHEN 9 THEN r.fokey9
            WHEN 10 THEN r.fokey10
            WHEN 11 THEN r.fokey11
            WHEN 12 THEN r.fokey12
            WHEN 13 THEN r.fokey13
            WHEN 14 THEN r.fokey14
            WHEN 15 THEN r.fokey15
            WHEN 16 THEN r.fokey16
       END
    WHERE r.frgndbname IS NULL OR r.frgndbname = db_name()

    UNION ALL

    /* CHECK constraints; colid 0 describes a table-level constraint.
       A rule object distinguishes checks from trigger status bits in newer ASE. */
    SELECT
        u.name AS SCHEMA_NAME,
        t.name AS TABLE_NAME,
        co.name AS CONSTRAINT_NAME,
        'CHECK' AS CONSTRAINT_TYPE,
        c.name AS COLUMN_NAME,
        convert(int, 1) AS ORDINAL_POSITION
    FROM dbo.sysconstraints sc
    INNER JOIN dbo.sysobjects t ON t.id = sc.tableid AND t.type = 'U'
    INNER JOIN dbo.sysusers u ON u.uid = t.uid
    INNER JOIN dbo.sysobjects co ON co.id = sc.constrid AND co.type = 'R'
    LEFT JOIN dbo.syscolumns c
        ON c.id = sc.tableid AND c.colid = sc.colid AND c.number = 0
    WHERE (sc.status & 128) = 128
) x
ORDER BY x.SCHEMA_NAME, x.TABLE_NAME, x.CONSTRAINT_NAME, x.ORDINAL_POSITION
