-- SAP ASE target tables use the existing metadata contract.
CREATE TABLE IF NOT EXISTS metadata_replication.database_metadata_sapase (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    name            varchar(200) NOT NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT database_metadata_sapase_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS database_metadata_sapase_service_name_idx ON metadata_replication.database_metadata_sapase USING btree (service_name);

CREATE TABLE IF NOT EXISTS metadata_replication.schema_metadata_sapase (
    id              bigint NOT NULL,
    fqn             text NOT NULL,
    service_name    varchar(100) NOT NULL,
    db_name         varchar(200) NOT NULL,
    name            varchar(500) NOT NULL,
    parent_fqn      text NOT NULL,
    hash_data       text NULL,
    created_at      timestamp without time zone NOT NULL default NOW(),
    CONSTRAINT schema_metadata_sapase_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS schema_metadata_sapase_service_name_idx ON metadata_replication.schema_metadata_sapase USING btree (service_name);

CREATE TABLE IF NOT EXISTS metadata_replication.table_metadata_sapase (
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
    CONSTRAINT table_metadata_sapase_pk PRIMARY KEY (fqn)
);
CREATE INDEX IF NOT EXISTS table_metadata_sapase_service_name_idx ON metadata_replication.table_metadata_sapase USING btree (service_name);

CREATE INDEX IF NOT EXISTS schema_metadata_sapase_service_db_idx
    ON metadata_replication.schema_metadata_sapase (service_name, db_name);
CREATE INDEX IF NOT EXISTS table_metadata_sapase_service_db_idx
    ON metadata_replication.table_metadata_sapase (service_name, db_name);
