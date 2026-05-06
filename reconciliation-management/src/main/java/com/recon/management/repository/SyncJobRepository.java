package com.recon.management.repository;

import com.recon.management.entity.SyncJobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SyncJobRepository extends JpaRepository<SyncJobEntity, String> {

    @Query("SELECT j FROM SyncJobEntity j LEFT JOIN FETCH j.mappings WHERE j.id = :id")
    Optional<SyncJobEntity> findByIdWithMappings(String id);

    @Query("SELECT j FROM SyncJobEntity j LEFT JOIN FETCH j.mappings")
    List<SyncJobEntity> findAllWithMappings();

    List<SyncJobEntity> findByStatus(String status);
}
