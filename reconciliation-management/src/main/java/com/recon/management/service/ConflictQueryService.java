package com.recon.management.service;

import com.recon.common.enums.ResolutionStatus;
import com.recon.management.dto.ManualResolveRequest;
import com.recon.management.entity.ConflictLogEntity;
import com.recon.management.repository.ConflictLogRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
@Transactional(readOnly = true)
public class ConflictQueryService {

    private final ConflictLogRepository repository;

    public ConflictQueryService(ConflictLogRepository repository) {
        this.repository = repository;
    }

    public Page<ConflictLogEntity> search(String syncJobId,
                                           String collectionName,
                                           ResolutionStatus status,
                                           Instant from,
                                           Instant to,
                                           int page,
                                           int size) {
        return repository.search(syncJobId, collectionName, status, from, to,
                PageRequest.of(page, size));
    }

    public ConflictLogEntity getById(String id) {
        return repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Conflict not found: " + id));
    }

    @Transactional
    public ConflictLogEntity resolve(String id, ManualResolveRequest request) {
        ConflictLogEntity conflict = getById(id);

        if (conflict.getResolutionStatus() != ResolutionStatus.MANUAL_REQUIRED) {
            throw new IllegalStateException("Conflict is not in MANUAL_REQUIRED status");
        }

        conflict.setResolvedVersion(request.resolvedVersion());
        conflict.setResolutionDetail(request.resolutionDetail());
        conflict.setResolutionStatus(ResolutionStatus.AUTO_RESOLVED);
        conflict.setResolvedAt(Instant.now());

        return repository.save(conflict);
    }
}
