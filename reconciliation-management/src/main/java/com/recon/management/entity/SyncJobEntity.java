package com.recon.management.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "sync_jobs")
public class SyncJobEntity {

    @Id
    @Column(length = 36)
    private String id;

    @Column(nullable = false, unique = true, length = 255)
    private String name;

    @Column(name = "source_connection_id", nullable = false, length = 36)
    private String sourceConnectionId;

    @Column(name = "target_connection_id", nullable = false, length = 36)
    private String targetConnectionId;

    @Column(nullable = false, length = 20)
    private String status = "STOPPED";

    @Column(name = "flink_job_id", length = 255)
    private String flinkJobId;

    @Column(name = "flink_cluster_url", length = 512)
    private String flinkClusterUrl;

    @Column(nullable = false)
    private int parallelism = 1;

    @Column(name = "checkpoint_interval_ms", nullable = false)
    private long checkpointIntervalMs = 10000;

    @Column(name = "window_size_ms", nullable = false)
    private long windowSizeMs = 30000;

    @OneToMany(mappedBy = "syncJob", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private List<CollectionMappingEntity> mappings = new ArrayList<>();

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public SyncJobEntity() {}

    public SyncJobEntity(String id, String name, String sourceConnectionId, String targetConnectionId,
                          String status, String flinkJobId, String flinkClusterUrl, int parallelism,
                          long checkpointIntervalMs, long windowSizeMs,
                          List<CollectionMappingEntity> mappings, Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.name = name;
        this.sourceConnectionId = sourceConnectionId;
        this.targetConnectionId = targetConnectionId;
        this.status = status;
        this.flinkJobId = flinkJobId;
        this.flinkClusterUrl = flinkClusterUrl;
        this.parallelism = parallelism;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.windowSizeMs = windowSizeMs;
        this.mappings = mappings != null ? mappings : new ArrayList<>();
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    @PrePersist
    void prePersist() {
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    @PreUpdate
    void preUpdate() {
        this.updatedAt = Instant.now();
    }

    // Getters/Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getSourceConnectionId() { return sourceConnectionId; }
    public void setSourceConnectionId(String id) { this.sourceConnectionId = id; }
    public String getTargetConnectionId() { return targetConnectionId; }
    public void setTargetConnectionId(String id) { this.targetConnectionId = id; }
    public String getStatus() { return status; }
    public void setStatus(String s) { this.status = s; }
    public String getFlinkJobId() { return flinkJobId; }
    public void setFlinkJobId(String id) { this.flinkJobId = id; }
    public String getFlinkClusterUrl() { return flinkClusterUrl; }
    public void setFlinkClusterUrl(String url) { this.flinkClusterUrl = url; }
    public int getParallelism() { return parallelism; }
    public void setParallelism(int p) { this.parallelism = p; }
    public long getCheckpointIntervalMs() { return checkpointIntervalMs; }
    public void setCheckpointIntervalMs(long ms) { this.checkpointIntervalMs = ms; }
    public long getWindowSizeMs() { return windowSizeMs; }
    public void setWindowSizeMs(long ms) { this.windowSizeMs = ms; }
    public List<CollectionMappingEntity> getMappings() { return mappings; }
    public void setMappings(List<CollectionMappingEntity> m) { this.mappings = m; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant i) { this.createdAt = i; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant i) { this.updatedAt = i; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String id, name, sourceConnectionId, targetConnectionId, status = "STOPPED",
                flinkJobId, flinkClusterUrl;
        private int parallelism = 1;
        private long checkpointIntervalMs = 10000, windowSizeMs = 30000;
        private List<CollectionMappingEntity> mappings = new ArrayList<>();
        private Instant createdAt, updatedAt;

        public Builder id(String v) { id = v; return this; }
        public Builder name(String v) { name = v; return this; }
        public Builder sourceConnectionId(String v) { sourceConnectionId = v; return this; }
        public Builder targetConnectionId(String v) { targetConnectionId = v; return this; }
        public Builder status(String v) { status = v; return this; }
        public Builder flinkJobId(String v) { flinkJobId = v; return this; }
        public Builder flinkClusterUrl(String v) { flinkClusterUrl = v; return this; }
        public Builder parallelism(int v) { parallelism = v; return this; }
        public Builder checkpointIntervalMs(long v) { checkpointIntervalMs = v; return this; }
        public Builder windowSizeMs(long v) { windowSizeMs = v; return this; }
        public Builder mappings(List<CollectionMappingEntity> v) { mappings = v; return this; }
        public Builder createdAt(Instant v) { createdAt = v; return this; }
        public Builder updatedAt(Instant v) { updatedAt = v; return this; }

        public SyncJobEntity build() {
            return new SyncJobEntity(id, name, sourceConnectionId, targetConnectionId, status,
                    flinkJobId, flinkClusterUrl, parallelism, checkpointIntervalMs, windowSizeMs,
                    mappings, createdAt, updatedAt);
        }
    }
}
