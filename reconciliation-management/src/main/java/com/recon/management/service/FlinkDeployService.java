package com.recon.management.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recon.management.config.FlinkClientConfig;
import com.recon.management.entity.CollectionMappingEntity;
import com.recon.management.entity.DbConnectionEntity;
import com.recon.management.entity.SyncJobEntity;
import com.recon.management.repository.DbConnectionRepository;
import com.recon.management.repository.SyncJobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Orchestrates Flink job lifecycle: deploy, stop, restart.
 *
 * Flow:
 *   deploy()  → 1. build job config JSON
 *               2. upload fat JAR to Flink cluster
 *               3. submit job with config
 *               4. persist flinkJobId
 *
 *   stop()    → 1. trigger savepoint
 *               2. cancel job
 *               3. persist savepoint path
 *
 *   restart() → 1. reload config
 *               2. submit job from same JAR with savepointPath
 */
@Service
public class FlinkDeployService {

    private static final Logger log = LoggerFactory.getLogger(FlinkDeployService.class);

    private final SyncJobRepository syncJobRepository;
    private final DbConnectionRepository connectionRepository;
    private final FlinkRestClient flinkClient;
    private final FlinkClientConfig config;
    private final ObjectMapper objectMapper;

    public FlinkDeployService(SyncJobRepository syncJobRepository,
                               DbConnectionRepository connectionRepository,
                               FlinkRestClient flinkClient,
                               FlinkClientConfig config) {
        this.syncJobRepository = syncJobRepository;
        this.connectionRepository = connectionRepository;
        this.flinkClient = flinkClient;
        this.config = config;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Deploy a sync job to Flink.
     * 1. Generate JSON config file
     * 2. Upload JAR to Flink
     * 3. Submit job
     */
    public SyncJobEntity deploy(String jobId) {
        SyncJobEntity job = loadJob(jobId);
        if ("RUNNING".equals(job.getStatus()) || "DEPLOYING".equals(job.getStatus())) {
            throw new IllegalStateException("Job is already running or deploying");
        }

        job.setStatus("DEPLOYING");
        syncJobRepository.save(job);

        try {
            // 1. Build job config JSON and write to temp file
            Map<String, Object> jobConfig = buildJobConfig(job);
            Path configFile = Files.createTempFile("recon-job-", ".json");
            objectMapper.writeValue(configFile.toFile(), jobConfig);
            log.info("Job config written to {}", configFile);

            // 2. Locate the fat JAR
            File jarFile = findRunnerJar();

            // 3. Upload JAR → Flink
            String jarId = flinkClient.uploadJar(jarFile).block();
            if (jarId == null) throw new RuntimeException("JAR upload returned null");

            // 4. Submit job
            String programArgs = "--config " + configFile.toAbsolutePath();
            String flinkJobId = flinkClient.runJob(jarId, programArgs, job.getParallelism(), null).block();
            if (flinkJobId == null) throw new RuntimeException("Job submission returned null");

            // 5. Persist
            job.setFlinkJobId(flinkJobId);
            job.setFlinkClusterUrl(config.getRestUrl());
            job.setStatus("RUNNING");
            log.info("Deploy complete. syncJobId={}, flinkJobId={}", jobId, flinkJobId);

        } catch (Exception e) {
            log.error("Deploy failed for job {}: {}", jobId, e.getMessage(), e);
            job.setStatus("ERROR");
            throw new RuntimeException("Flink deploy failed: " + e.getMessage(), e);
        }

        return syncJobRepository.save(job);
    }

    /**
     * Stop a running Flink job with a savepoint.
     */
    public SyncJobEntity stop(String jobId) {
        SyncJobEntity job = loadJob(jobId);
        if (!"RUNNING".equals(job.getStatus())) {
            throw new IllegalStateException("Job is not running");
        }
        if (job.getFlinkJobId() == null) {
            throw new IllegalStateException("No Flink job ID — was it deployed?");
        }

        try {
            String savepointPath = flinkClient.stopWithSavepoint(job.getFlinkJobId(), config.getSavepointDir()).block();
            job.setStatus("STOPPED");
            log.info("Stop complete. syncJobId={}, savepoint={}", jobId, savepointPath);
        } catch (Exception e) {
            log.error("Stop failed for job {}: {}", jobId, e.getMessage(), e);
            throw new RuntimeException("Flink stop failed: " + e.getMessage(), e);
        }

        return syncJobRepository.save(job);
    }

    /**
     * Restart a stopped job from the last savepoint.
     */
    public SyncJobEntity restart(String jobId) {
        SyncJobEntity job = loadJob(jobId);
        if (!"STOPPED".equals(job.getStatus())) {
            throw new IllegalStateException("Job must be STOPPED before restarting");
        }

        job.setStatus("DEPLOYING");
        syncJobRepository.save(job);

        try {
            Map<String, Object> jobConfig = buildJobConfig(job);
            Path configFile = Files.createTempFile("recon-job-", ".json");
            objectMapper.writeValue(configFile.toFile(), jobConfig);

            File jarFile = findRunnerJar();
            String jarId = flinkClient.uploadJar(jarFile).block();

            String programArgs = "--config " + configFile.toAbsolutePath();
            // Restart from savepoint — the previous stop() should have saved the path
            String savepointPath = null; // TODO: store savepoint path on stop()
            String flinkJobId = flinkClient.runJob(jarId, programArgs, job.getParallelism(), savepointPath).block();

            job.setFlinkJobId(flinkJobId);
            job.setStatus("RUNNING");
            log.info("Restart complete. syncJobId={}, flinkJobId={}", jobId, flinkJobId);
        } catch (Exception e) {
            log.error("Restart failed for job {}: {}", jobId, e.getMessage(), e);
            job.setStatus("ERROR");
            throw new RuntimeException("Flink restart failed: " + e.getMessage(), e);
        }

        return syncJobRepository.save(job);
    }

    /**
     * Get the runtime status from Flink.
     */
    public Map<String, Object> getRuntimeStatus(String jobId) {
        SyncJobEntity job = loadJob(jobId);
        if (job.getFlinkJobId() == null) {
            return Map.of("status", job.getStatus(), "detail", "Not deployed");
        }

        try {
            Map<String, Object> detail = flinkClient.getJobDetail(job.getFlinkJobId()).block();
            Map<String, Object> result = new HashMap<>();
            result.put("syncJobId", job.getId());
            result.put("syncJobName", job.getName());
            result.put("dbStatus", job.getStatus());
            result.put("flinkJobId", job.getFlinkJobId());
            result.put("flinkState", detail != null ? detail.get("state") : "UNKNOWN");
            result.put("startTime", detail != null ? detail.get("start-time") : null);
            result.put("duration", detail != null ? detail.get("duration") : null);
            return result;
        } catch (Exception e) {
            return Map.of("status", job.getStatus(), "error", e.getMessage());
        }
    }

    // ──── Internal helpers ────

    private SyncJobEntity loadJob(String jobId) {
        return syncJobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Sync job not found: " + jobId));
    }

    /**
     * Build the JSON config that the Flink runner expects.
     */
    private Map<String, Object> buildJobConfig(SyncJobEntity job) {
        DbConnectionEntity source = connectionRepository.findById(job.getSourceConnectionId())
                .orElseThrow(() -> new IllegalArgumentException("Source connection not found"));
        DbConnectionEntity target = connectionRepository.findById(job.getTargetConnectionId())
                .orElseThrow(() -> new IllegalArgumentException("Target connection not found"));

        List<Map<String, Object>> mappings = job.getMappings().stream()
                .map(this::mappingToMap)
                .toList();

        Map<String, Object> config = new HashMap<>();
        config.put("job_name", job.getName());
        config.put("sync_job_id", job.getId());
        config.put("source_cluster_label", source.getClusterLabel());
        config.put("remote_cluster_label", target.getClusterLabel());
        config.put("parallelism", job.getParallelism());
        config.put("checkpoint_interval_ms", job.getCheckpointIntervalMs());
        config.put("window_size_ms", job.getWindowSizeMs());

        config.put("source_config", Map.of(
                "db_type", source.getDbType().name(),
                "connection_string", source.getConnectionString(),
                "database", source.getProperties() != null
                        ? source.getProperties().getOrDefault("database", "admin") : "admin",
                "cluster_label", source.getClusterLabel()
        ));

        config.put("target_config", Map.of(
                "db_type", target.getDbType().name(),
                "connection_string", target.getConnectionString(),
                "database", target.getProperties() != null
                        ? target.getProperties().getOrDefault("database", "admin") : "admin",
                "cluster_label", target.getClusterLabel()
        ));

        config.put("mappings", mappings);
        return config;
    }

    private Map<String, Object> mappingToMap(CollectionMappingEntity m) {
        Map<String, Object> map = new HashMap<>();
        map.put("source_collection", m.getSourceCollection());
        map.put("target_collection", m.getTargetCollection());
        map.put("timestamp_field", m.getTimestampField());
        map.put("strategy", m.getStrategy());
        map.put("window_size_ms", m.getWindowSizeMs());
        if (m.getStrategyParams() != null) {
            map.put("strategy_params", m.getStrategyParams());
        }
        return map;
    }

    /**
     * Locate the reconciliation-flink-runner fat JAR.
     * Looks in: config.jarDirectory, then target/, then maven repo.
     */
    private File findRunnerJar() {
        // 1. Config-specified directory
        File dir = new File(config.getJarDirectory());
        if (dir.exists()) {
            File[] jars = dir.listFiles((d, name) ->
                    name.startsWith("reconciliation-flink-runner") && name.endsWith(".jar"));
            if (jars != null && jars.length > 0) return jars[0];
        }

        // 2. Module target directory
        File targetDir = new File("../reconciliation-flink-runner/target");
        if (targetDir.exists()) {
            File[] jars = targetDir.listFiles((d, name) ->
                    name.startsWith("reconciliation-flink-runner") && name.endsWith(".jar")
                    && !name.contains("original") && !name.contains("sources"));
            if (jars != null && jars.length > 0) return jars[0];
        }

        // 3. Maven local repo
        String home = System.getProperty("user.home");
        File m2Repo = new File(home, ".m2/repository/com/recon/reconciliation-flink-runner/1.0.0-SNAPSHOT");
        if (m2Repo.exists()) {
            File[] jars = m2Repo.listFiles((d, name) ->
                    name.endsWith(".jar") && !name.contains("sources") && !name.contains("javadoc"));
            if (jars != null && jars.length > 0) return jars[0];
        }

        throw new RuntimeException(
                "Cannot find reconciliation-flink-runner JAR. Build with: mvn package -pl reconciliation-flink-runner -DskipTests");
    }
}
