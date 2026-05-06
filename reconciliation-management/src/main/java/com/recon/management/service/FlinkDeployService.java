package com.recon.management.service;

import com.recon.management.entity.SyncJobEntity;
import com.recon.management.repository.SyncJobRepository;
import org.springframework.stereotype.Service;

/**
 * Service for deploying and managing Flink jobs.
 * In production, this would communicate with a Flink REST API
 * to submit JARs, start/stop jobs, and trigger savepoints.
 *
 * For now, this is a stub that updates job status in the database.
 */
@Service
public class FlinkDeployService {

    private final SyncJobRepository syncJobRepository;

    public FlinkDeployService(SyncJobRepository syncJobRepository) {
        this.syncJobRepository = syncJobRepository;
    }

    public SyncJobEntity deploy(String jobId) {
        SyncJobEntity job = syncJobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Sync job not found: " + jobId));

        if ("RUNNING".equals(job.getStatus())) {
            throw new IllegalStateException("Job is already running");
        }

        // TODO: Actual Flink deployment via REST API
        // 1. Package the sync job as a JAR
        // 2. Upload to Flink cluster
        // 3. Submit with job config as arguments
        // 4. Store flinkJobId

        job.setStatus("RUNNING");
        return syncJobRepository.save(job);
    }

    public SyncJobEntity stop(String jobId) {
        SyncJobEntity job = syncJobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Sync job not found: " + jobId));

        if (!"RUNNING".equals(job.getStatus())) {
            throw new IllegalStateException("Job is not running");
        }

        // TODO: Trigger savepoint and cancel via Flink REST API

        job.setStatus("STOPPED");
        return syncJobRepository.save(job);
    }

    public SyncJobEntity restart(String jobId) {
        SyncJobEntity job = syncJobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Sync job not found: " + jobId));

        if (!"STOPPED".equals(job.getStatus())) {
            throw new IllegalStateException("Job must be stopped before restarting");
        }

        // TODO: Restart from savepoint via Flink REST API

        job.setStatus("RUNNING");
        return syncJobRepository.save(job);
    }
}
