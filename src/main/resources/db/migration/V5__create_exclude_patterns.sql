CREATE TABLE IF NOT EXISTS metadata_replication.exclude_patterns (
	id BIGSERIAL NOT NULL,
	database_type varchar(50) NOT NULL,
	entity_type varchar(50) NOT NULL,
	pattern_expr text NOT NULL,
	CONSTRAINT exclude_patterns_pkey PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS
    exclude_patterns_db_entity_idx
ON metadata_replication.exclude_patterns
    (database_type, entity_type);