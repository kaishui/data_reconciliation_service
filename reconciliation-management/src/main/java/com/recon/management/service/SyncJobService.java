package com.recon.management.service;

import com.recon.management.dto.CollectionMappingRequest;
import com.recon.management.dto.SyncJobRequest;
import com.recon.management.entity.CollectionMappingEntity;
import com.recon.management.entity.SyncJobEntity;
import com.recon.management.repository.DbConnectionRepository;
import com.recon.management.repository.SyncJobRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@Transactional
public class SyncJobService {

    private final SyncJobRepository repository;
    private final DbConnectionRepository connectionRepository;

    public SyncJobService(SyncJobRepository repository, DbConnectionRepository connectionRepository) {
        this.repository = repository;
        this.connectionRepository = connectionRepository;
    }

    public List<SyncJobEntity> listAll() {
        return repository.findAllWithMappings();
    }

    public SyncJobEntity getById(String id) {
        return repository.findByIdWithMappings(id)
                .orElseThrow(() -> new IllegalArgumentException("Sync job not found: " + id));
    }

    public SyncJobEntity create(SyncJobRequest request) {
        // Validate connections exist
        connectionRepository.findById(request.sourceConnectionId())
                .orElseThrow(() -> new IllegalArgumentException("Source connection not found: " + request.sourceConnectionId()));
        connectionRepository.findById(request.targetConnectionId())
                .orElseThrow(() -> new IllegalArgumentException("Target connection not found: " + request.targetConnectionId()));

        String jobId = UUID.randomUUID().toString();
        SyncJobEntity job = SyncJobEntity.builder()
                .id(jobId)
                .name(request.name())
                .sourceConnectionId(request.sourceConnectionId())
                .targetConnectionId(request.targetConnectionId())
                .parallelism(request.parallelism())
                .checkpointIntervalMs(request.checkpointIntervalMs())
                .windowSizeMs(request.windowSizeMs())
                .flinkClusterUrl(request.flinkClusterUrl())
                .build();

        List<CollectionMappingEntity> mappings = new ArrayList<>();
        for (CollectionMappingRequest m : request.mappings()) {
            mappings.add(CollectionMappingEntity.builder()
                    .id(UUID.randomUUID().toString())
                    .syncJob(job)
                    .sourceCollection(m.sourceCollection())
                    .targetCollection(m.targetCollection())
                    .timestampField(m.timestampField())
                    .strategy(m.strategy())
                    .strategyParams(m.strategyParams())
                    .windowSizeMs(m.windowSizeMs())
                    .build());
        }
        job.setMappings(mappings);

        return repository.save(job);
    }

    public SyncJobEntity update(String id, SyncJobRequest request) {
        SyncJobEntity job = getById(id);

        connectionRepository.findById(request.sourceConnectionId())
                .orElseThrow(() -> new IllegalArgumentException("Source connection not found"));
        connectionRepository.findById(request.targetConnectionId())
                .orElseThrow(() -> new IllegalArgumentException("Target connection not found"));

        job.setName(request.name());
        job.setSourceConnectionId(request.sourceConnectionId());
        job.setTargetConnectionId(request.targetConnectionId());
        job.setParallelism(request.parallelism());
        job.setCheckpointIntervalMs(request.checkpointIntervalMs());
        job.setWindowSizeMs(request.windowSizeMs());
        job.setFlinkClusterUrl(request.flinkClusterUrl());

        // Replace mappings
        job.getMappings().clear();
        for (CollectionMappingRequest m : request.mappings()) {
            job.getMappings().add(CollectionMappingEntity.builder()
                    .id(UUID.randomUUID().toString())
                    .syncJob(job)
                    .sourceCollection(m.sourceCollection())
                    .targetCollection(m.targetCollection())
                    .timestampField(m.timestampField())
                    .strategy(m.strategy())
                    .strategyParams(m.strategyParams())
                    .windowSizeMs(m.windowSizeMs())
                    .build());
        }

        return repository.save(job);
    }

    public void delete(String id) {
        SyncJobEntity job = getById(id);
        if ("RUNNING".equals(job.getStatus()) || "DEPLOYING".equals(job.getStatus())) {
            throw new IllegalStateException("Cannot delete a running job. Stop it first.");
        }
        repository.delete(job);
    }
}
