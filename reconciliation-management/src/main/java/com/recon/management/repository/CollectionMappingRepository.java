package com.recon.management.repository;

import com.recon.management.entity.CollectionMappingEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CollectionMappingRepository extends JpaRepository<CollectionMappingEntity, String> {
    List<CollectionMappingEntity> findBySyncJobId(String syncJobId);
}
