CREATE TABLE IF NOT EXISTS metadata_replication.jwt_token_registry (
    jti uuid primary key,
    service varchar(255) not null,
    subject varchar(255) not null,
    issued_at timestamptz not null,
    expires_at timestamptz not null,
    revoked_at timestamptz
);

create index idx_jwt_token_registry_expires_at
    on metadata_replication.jwt_token_registry (expires_at);

create index idx_jwt_token_registry_service_active
    on metadata_replication.jwt_token_registry (service)
    where revoked_at is null;
    