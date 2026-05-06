package com.recon.management.repository;

import com.recon.common.enums.ResolutionStatus;
import com.recon.management.entity.ConflictLogEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;

@Repository
public interface ConflictLogRepository extends JpaRepository<ConflictLogEntity, String> {

    @Query("""
        SELECT c FROM ConflictLogEntity c
        WHERE (:syncJobId IS NULL OR c.syncJobId = :syncJobId)
          AND (:collectionName IS NULL OR c.collectionName = :collectionName)
          AND (:status IS NULL OR c.resolutionStatus = :status)
          AND (:from IS NULL OR c.occurredAt >= :from)
          AND (:to IS NULL OR c.occurredAt <= :to)
        ORDER BY c.occurredAt DESC
    """)
    Page<ConflictLogEntity> search(@Param("syncJobId") String syncJobId,
                                    @Param("collectionName") String collectionName,
                                    @Param("status") ResolutionStatus status,
                                    @Param("from") Instant from,
                                    @Param("to") Instant to,
                                    Pageable pageable);
}
