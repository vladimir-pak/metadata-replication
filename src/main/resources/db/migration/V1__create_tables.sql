CREATE SCHEMA IF NOT EXISTS metadata_replication;

--------POSTGRES--------
CREATE TABLE IF NOT EXISTS metadata_replication.database_metadata_postgres (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    name            varchar(200) NOT NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT database_metadata_postgres_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS database_metadata_postgres_service_name_idx ON metadata_replication.database_metadata_postgres USING btree (service_name);

CREATE TABLE IF NOT EXISTS metadata_replication.schema_metadata_postgres (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    db_name         varchar(200) NOT NULL,
    name            varchar(500) NOT NULL,
    parent_fqn      text NOT NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT schema_metadata_postgres_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS schema_metadata_postgres_service_name_idx ON metadata_replication.schema_metadata_postgres USING btree (service_name);

CREATE TABLE IF NOT EXISTS metadata_replication.table_metadata_postgres (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    db_name         varchar(200) NOT NULL,
    schema_name     varchar(500) NOT NULL,
    description     text NULL,
    name            varchar(500) NOT NULL,
    parent_fqn      text NOT NULL,
    data            jsonb NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT table_metadata_postgres_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS table_metadata_postgres_service_name_idx ON metadata_replication.table_metadata_postgres USING btree (service_name);

--------ORACLE--------
CREATE TABLE IF NOT EXISTS metadata_replication.database_metadata_oracle (
    id              bigint NOT NULL default -1,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    name            varchar(200) NOT NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT database_metadata_oracle_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS database_metadata_oracle_service_name_idx ON metadata_replication.database_metadata_oracle USING btree (service_name);

CREATE TABLE IF NOT EXISTS metadata_replication.schema_metadata_oracle (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    db_name         varchar(200) NOT NULL,
    name            varchar(500) NOT NULL,
    parent_fqn      text NOT NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT schema_metadata_oracle_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS schema_metadata_oracle_service_name_idx ON metadata_replication.schema_metadata_oracle USING btree (service_name);

CREATE TABLE IF NOT EXISTS metadata_replication.table_metadata_oracle (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    db_name         varchar(200) NOT NULL,
    schema_name     varchar(500) NOT NULL,
    description     text NULL,
    name            varchar(500) NOT NULL,
    parent_fqn      text NOT NULL,
    data            jsonb NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT table_metadata_oracle_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS table_metadata_oracle_service_name_idx ON metadata_replication.table_metadata_oracle USING btree (service_name);

--------MSSQL--------
CREATE TABLE IF NOT EXISTS metadata_replication.database_metadata_mssql (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    name            varchar(200) NOT NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT database_metadata_mssql_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS database_metadata_mssql_service_name_idx ON metadata_replication.database_metadata_mssql USING btree (service_name);

CREATE TABLE IF NOT EXISTS metadata_replication.schema_metadata_mssql (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    db_name         varchar(200) NOT NULL,
    name            varchar(500) NOT NULL,
    parent_fqn      text NOT NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT schema_metadata_mssql_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS schema_metadata_mssql_service_name_idx ON metadata_replication.schema_metadata_mssql USING btree (service_name);

CREATE TABLE IF NOT EXISTS metadata_replication.table_metadata_mssql (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    db_name         varchar(200) NOT NULL,
    schema_name     varchar(500) NOT NULL,
    description     text NULL,
    name            varchar(500) NOT NULL,
    parent_fqn      text NOT NULL,
    data            jsonb NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT table_metadata_mssql_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS table_metadata_mssql_service_name_idx ON metadata_replication.table_metadata_mssql USING btree (service_name);

--------SAPIQ--------
CREATE TABLE IF NOT EXISTS metadata_replication.database_metadata_sapiq (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    name            varchar(200) NOT NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT database_metadata_sapiq_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS database_metadata_sapiq_service_name_idx ON metadata_replication.database_metadata_sapiq USING btree (service_name);

CREATE TABLE IF NOT EXISTS metadata_replication.schema_metadata_sapiq (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    db_name         varchar(200) NOT NULL,
    name            varchar(500) NOT NULL,
    parent_fqn      text NOT NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT schema_metadata_sapiq_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS schema_metadata_sapiq_service_name_idx ON metadata_replication.schema_metadata_sapiq USING btree (service_name);

CREATE TABLE IF NOT EXISTS metadata_replication.table_metadata_sapiq (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    db_name         varchar(200) NOT NULL,
    schema_name     varchar(500) NOT NULL,
    description     text NULL,
    name            varchar(500) NOT NULL,
    parent_fqn      text NOT NULL,
    data            jsonb NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT table_metadata_sapiq_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS table_metadata_sapiq_service_name_idx ON metadata_replication.table_metadata_sapiq USING btree (service_name);
