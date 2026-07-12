package com.gpb.replication.postgres.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.gpb.replication.postgres.model.DatabaseMetadata;
import com.gpb.replication.postgres.model.EntityId;

import jakarta.transaction.Transactional;

public interface DatabaseMetadataRepository extends JpaRepository<DatabaseMetadata, EntityId> {
    @Modifying
    @Transactional
    @Query(value = "DELETE FROM postgres_metadata.database_metadata WHERE service_name = :service", nativeQuery = true)
    void deleteByServiceName(@Param("service") String service);
}
