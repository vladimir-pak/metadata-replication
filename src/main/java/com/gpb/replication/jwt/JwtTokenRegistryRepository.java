package com.gpb.replication.jwt;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class JwtTokenRegistryRepository {

    private final JdbcTemplate jdbcTemplate;

    public void register(
            UUID jti,
            String service,
            String subject,
            Instant issuedAt,
            Instant expiresAt
    ) {
        jdbcTemplate.update("""
                insert into replication.jwt_token_registry (
                    jti,
                    service,
                    subject,
                    issued_at,
                    expires_at
                )
                values (?, ?, ?, ?, ?)
                """,
                jti,
                service,
                subject,
                Timestamp.from(issuedAt),
                Timestamp.from(expiresAt)
        );
    }

    public boolean isActive(UUID jti, String service) {

        Boolean result = jdbcTemplate.queryForObject("""
                select exists (
                    select 1
                    from replication.jwt_token_registry
                    where jti = ?
                      and service = ?
                      and revoked_at is null
                      and expires_at > current_timestamp
                )
                """,
                Boolean.class,
                jti,
                service
        );

        return Boolean.TRUE.equals(result);
    }

    public boolean revoke(String service) {

        int updated = jdbcTemplate.update("""
                update replication.jwt_token_registry
                set revoked_at = current_timestamp
                where service = ?
                  and revoked_at is null
                """,
                service
        );

        return updated > 0;
    }

    public boolean existsActiveByService(String service) {

        Boolean exists = jdbcTemplate.queryForObject("""
                select exists (
                    select 1
                    from replication.jwt_token_registry
                    where service = ?
                    and revoked_at is null
                    and expires_at > current_timestamp
                )
                """,
                Boolean.class,
                service
        );

        return Boolean.TRUE.equals(exists);
    }
}