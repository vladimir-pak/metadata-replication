SELECT
    i.table_id,
    i.index_id,
    CASE
        WHEN i.index_category = 1 THEN 'PRIMARY_KEY'
        WHEN i.index_category = 2 THEN 'FOREIGN_KEY'
        WHEN i.index_category = 3 AND i."unique" IN (1, 2, 5) THEN 'UNIQUE'
        ELSE 'OTHER'
    END AS constraint_type,
    ic.sequence AS column_sequence,
    TRIM(c.column_name) AS column_name
FROM SYS.SYSIDX i
JOIN SYS.SYSIDXCOL ic
    ON ic.table_id = i.table_id
    AND ic.index_id = i.index_id
JOIN SYS.SYSTABCOL c
    ON c.table_id = ic.table_id
    AND c.column_id = ic.column_id
JOIN SYS.SYSTAB t ON t.table_id = i.table_id
JOIN SYS.SYSUSER u ON u.user_id = t.creator
WHERE t.table_type IN (1, 2, 21)
    AND TRIM(u.user_name) NOT IN ('SYS', 'dbo', 'DBA')
    AND i.index_category IN (1, 2, 3)
    AND (
        i.index_category IN (1, 2)
        OR i."unique" IN (1, 2, 5)
        )
ORDER BY i.table_id, i.index_id, ic.sequence