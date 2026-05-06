package com.recon.management.service;

import com.recon.management.config.FlinkClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.File;
import java.time.Duration;
import java.util.Map;

/**
 * Low-level REST client for Apache Flink's REST API.
 *
 * Flink REST API reference:
 *   POST   /jars/upload                    — upload JAR → returns { "filename": "uuid/jar.jar" }
 *   POST   /jars/:jarid/run                — submit job  → returns { "jobid": "..." }
 *   GET    /jobs/:jobid                    — job status   → returns { "state": "RUNNING" }
 *   PATCH  /jobs/:jobid?mode=cancel        — cancel job
 *   POST   /jobs/:jobid/stop               — stop with savepoint (Flink ≥1.18)
 *   POST   /jobs/:jobid/savepoints         — trigger savepoint → returns { "request-id": "..." }
 *   GET    /jobs/:jobid/savepoints/:trigger — check savepoint status
 *   GET    /jobs/overview                  — list all jobs
 */
@Component
public class FlinkRestClient {

    private static final Logger log = LoggerFactory.getLogger(FlinkRestClient.class);

    private final WebClient webClient;
    private final FlinkClientConfig config;

    public FlinkRestClient(FlinkClientConfig config) {
        this.config = config;
        this.webClient = WebClient.builder()
                .baseUrl(config.getRestUrl())
                .build();
    }

    // ──── JAR Management ────

    /**
     * Upload a JAR file to the Flink cluster.
     * @param jarFile the fat JAR file
     * @return the jar ID (e.g. "550e8400-e29b-41d4-a716-446655440000_recon-runner.jar")
     */
    public Mono<String> uploadJar(File jarFile) {
        log.info("Uploading JAR: {} to {}", jarFile.getName(), config.getRestUrl());

        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("jarfile", new FileSystemResource(jarFile));

        return webClient.post()
                .uri("/jars/upload")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(builder.build()))
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(config.getTimeoutSeconds()))
                .flatMap(response -> {
                    String filename = (String) response.get("filename");
                    if (filename == null) {
                        return Mono.error(new RuntimeException("Upload failed: no filename in response. " + response));
                    }
                    // filename format: "uuid/uuid_flink-job.jar" → extract basename as jarId
                    String jarId = filename.contains("/") ? filename.substring(filename.indexOf('/') + 1) : filename;
                    log.info("JAR uploaded. jarId={}", jarId);
                    return Mono.just(jarId);
                });
    }

    // ──── Job Submission ────

    /**
     * Submit a Flink job from an uploaded JAR.
     *
     * @param jarId       jar ID returned by uploadJar()
     * @param programArgs CLI arguments for the job (e.g. "--config /path/to/config.json")
     * @param parallelism parallelism setting
     * @param savepointPath optional savepoint path to restore from (null = fresh start)
     * @return the Flink job ID
     */
    public Mono<String> runJob(String jarId, String programArgs, int parallelism, String savepointPath) {
        log.info("Submitting job: jarId={}, parallelism={}, args={}", jarId, parallelism, programArgs);

        Map<String, Object> body = new java.util.HashMap<>();
        body.put("programArgs", programArgs != null ? programArgs : "");
        body.put("parallelism", parallelism);
        if (savepointPath != null && !savepointPath.isEmpty()) {
            body.put("savepointPath", savepointPath);
            body.put("allowNonRestoredState", true);
        }

        return webClient.post()
                .uri("/jars/{jarId}/run", jarId)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(config.getTimeoutSeconds()))
                .flatMap(response -> {
                    String jobId = (String) response.get("jobid");
                    if (jobId == null) {
                        return Mono.error(new RuntimeException("Job submission failed: " + response));
                    }
                    log.info("Job submitted. flinkJobId={}", jobId);
                    return Mono.just(jobId);
                });
    }

    // ──── Job Status ────

    /**
     * Get the current state of a Flink job.
     * @return state string: "RUNNING", "FINISHED", "CANCELED", "FAILED", etc.
     */
    public Mono<String> getJobState(String flinkJobId) {
        return webClient.get()
                .uri("/jobs/{jobId}", flinkJobId)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(config.getTimeoutSeconds()))
                .map(response -> (String) response.get("state"))
                .defaultIfEmpty("UNKNOWN");
    }

    /**
     * Get full job status including state, timestamps, and metrics summary.
     */
    @SuppressWarnings("unchecked")
    public Mono<Map<String, Object>> getJobDetail(String flinkJobId) {
        return webClient.get()
                .uri("/jobs/{jobId}", flinkJobId)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(config.getTimeoutSeconds()))
                .map(m -> (Map<String, Object>) m);
    }

    /**
     * List all jobs on the Flink cluster.
     */
    @SuppressWarnings("unchecked")
    public Mono<Map<String, Object>> listJobs() {
        return webClient.get()
                .uri("/jobs/overview")
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(config.getTimeoutSeconds()))
                .map(m -> (Map<String, Object>) m);
    }

    // ──── Job Lifecycle ────

    /**
     * Stop a running Flink job with a savepoint.
     * First triggers a savepoint, then cancels the job.
     *
     * Flink ≥1.18: use POST /jobs/:jobid/stop
     * Flink <1.18: use POST /jobs/:jobid/savepoints then PATCH /jobs/:jobid
     *
     * @return the savepoint path, or null if failed
     */
    public Mono<String> stopWithSavepoint(String flinkJobId, String savepointDir) {
        log.info("Stopping job {} with savepoint to {}", flinkJobId, savepointDir);

        // Try Flink ≥1.18 stop-with-savepoint API first
        Map<String, Object> stopBody = Map.of(
                "drain", false,
                "targetDirectory", savepointDir != null ? savepointDir : config.getSavepointDir()
        );

        return webClient.post()
                .uri("/jobs/{jobId}/stop", flinkJobId)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(stopBody)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(config.getTimeoutSeconds()))
                .flatMap(response -> {
                    String location = (String) response.get("savepointPath");
                    if (location != null) {
                        log.info("Job {} stopped. Savepoint: {}", flinkJobId, location);
                        return Mono.just(location);
                    }
                    // Fallback: trigger savepoint then cancel
                    return triggerSavepoint(flinkJobId, savepointDir)
                            .flatMap(sp -> cancelJob(flinkJobId).thenReturn(sp));
                })
                .onErrorResume(e -> {
                    log.warn("Stop API failed, falling back to savepoint + cancel: {}", e.getMessage());
                    return triggerSavepoint(flinkJobId, savepointDir)
                            .flatMap(sp -> cancelJob(flinkJobId).thenReturn(sp));
                });
    }

    /**
     * Trigger an async savepoint. Returns the trigger ID for polling.
     */
    public Mono<String> triggerSavepoint(String flinkJobId, String savepointDir) {
        Map<String, Object> body = Map.of(
                "target-directory", savepointDir != null ? savepointDir : config.getSavepointDir(),
                "cancel-job", false
        );

        return webClient.post()
                .uri("/jobs/{jobId}/savepoints", flinkJobId)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(config.getTimeoutSeconds()))
                .map(response -> (String) response.get("request-id"))
                .doOnSuccess(id -> log.info("Savepoint triggered. triggerId={}", id));
    }

    /**
     * Poll savepoint status until complete.
     * @return the savepoint path
     */
    public Mono<String> awaitSavepoint(String flinkJobId, String triggerId, int maxRetries) {
        return pollSavepoint(flinkJobId, triggerId, 0, maxRetries);
    }

    private Mono<String> pollSavepoint(String flinkJobId, String triggerId, int attempt, int maxRetries) {
        return webClient.get()
                .uri("/jobs/{jobId}/savepoints/{triggerId}", flinkJobId, triggerId)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(config.getTimeoutSeconds()))
                .flatMap(response -> {
                    String status = (String) ((Map) response.getOrDefault("status", Map.of())).get("id");
                    if ("COMPLETED".equals(status)) {
                        String location = (String) ((Map) response.getOrDefault("operation", Map.of())).get("location");
                        return Mono.just(location);
                    }
                    if (attempt >= maxRetries) {
                        return Mono.error(new RuntimeException("Savepoint timeout after " + maxRetries + " attempts"));
                    }
                    return Mono.delay(Duration.ofSeconds(2))
                            .then(pollSavepoint(flinkJobId, triggerId, attempt + 1, maxRetries));
                });
    }

    /**
     * Cancel a job (without savepoint).
     */
    public Mono<Void> cancelJob(String flinkJobId) {
        log.info("Cancelling job {}", flinkJobId);
        return webClient.patch()
                .uri("/jobs/{jobId}?mode=cancel", flinkJobId)
                .retrieve()
                .bodyToMono(Void.class)
                .timeout(Duration.ofSeconds(config.getTimeoutSeconds()))
                .doOnSuccess(v -> log.info("Job {} cancelled", flinkJobId));
    }
}
