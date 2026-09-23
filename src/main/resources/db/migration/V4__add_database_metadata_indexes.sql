CREATE INDEX IF NOT EXISTS
    schema_metadata_postgres_service_db_idx
ON metadata_replication.schema_metadata_postgres
    (service_name, db_name);

CREATE INDEX IF NOT EXISTS
    table_metadata_postgres_service_db_idx
ON metadata_replication.table_metadata_postgres
    (service_name, db_name);

CREATE INDEX IF NOT EXISTS
    schema_metadata_oracle_service_db_idx
ON metadata_replication.schema_metadata_oracle
    (service_name, db_name);

CREATE INDEX IF NOT EXISTS
    table_metadata_oracle_service_db_idx
ON metadata_replication.table_metadata_oracle
    (service_name, db_name);

CREATE INDEX IF NOT EXISTS
    schema_metadata_mssql_service_db_idx
ON metadata_replication.schema_metadata_mssql
    (service_name, db_name);

CREATE INDEX IF NOT EXISTS
    table_metadata_mssql_service_db_idx
ON metadata_replication.table_metadata_mssql
    (service_name, db_name);

CREATE INDEX IF NOT EXISTS
    schema_metadata_sapiq_service_db_idx
ON metadata_replication.schema_metadata_sapiq
    (service_name, db_name);

CREATE INDEX IF NOT EXISTS
    table_metadata_sapiq_service_db_idx
ON metadata_replication.table_metadata_sapiq
    (service_name, db_name);