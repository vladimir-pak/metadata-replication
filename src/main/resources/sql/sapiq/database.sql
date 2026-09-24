SELECT
    CAST(DB_ID() AS BIGINT) AS id,
    TRIM(DB_NAME()) AS db_name
