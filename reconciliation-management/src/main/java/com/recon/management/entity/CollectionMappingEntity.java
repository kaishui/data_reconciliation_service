package com.recon.management.entity;

import io.hypersistence.utils.hibernate.type.json.JsonBinaryType;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;

import java.time.Instant;
import java.util.Map;

@Entity
@Table(name = "collection_mappings",
       uniqueConstraints = @UniqueConstraint(columnNames = {"sync_job_id", "source_collection"}))
public class CollectionMappingEntity {

    @Id
    @Column(length = 36)
    private String id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "sync_job_id", nullable = false)
    private SyncJobEntity syncJob;

    @Column(name = "source_collection", nullable = false, length = 255)
    private String sourceCollection;

    @Column(name = "target_collection", nullable = false, length = 255)
    private String targetCollection;

    @Column(name = "timestamp_field", nullable = false, length = 255)
    private String timestampField;

    @Column(nullable = false, length = 50)
    private String strategy = "LAST_WRITE_WINS";

    @Type(JsonBinaryType.class)
    @Column(name = "strategy_params", columnDefinition = "jsonb")
    private Map<String, Object> strategyParams;

    @Column(name = "window_size_ms", nullable = false)
    private long windowSizeMs = 30000;

    @Column(nullable = false)
    private boolean enabled = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public CollectionMappingEntity() {}

    public CollectionMappingEntity(String id, SyncJobEntity syncJob, String sourceCollection,
                                    String targetCollection, String timestampField, String strategy,
                                    Map<String, Object> strategyParams, long windowSizeMs,
                                    boolean enabled, Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.syncJob = syncJob;
        this.sourceCollection = sourceCollection;
        this.targetCollection = targetCollection;
        this.timestampField = timestampField;
        this.strategy = strategy;
        this.strategyParams = strategyParams;
        this.windowSizeMs = windowSizeMs;
        this.enabled = enabled;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    @PrePersist
    void prePersist() { createdAt = Instant.now(); updatedAt = Instant.now(); }

    @PreUpdate
    void preUpdate() { updatedAt = Instant.now(); }

    // Getters/Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public SyncJobEntity getSyncJob() { return syncJob; }
    public void setSyncJob(SyncJobEntity j) { this.syncJob = j; }
    public String getSourceCollection() { return sourceCollection; }
    public void setSourceCollection(String s) { this.sourceCollection = s; }
    public String getTargetCollection() { return targetCollection; }
    public void setTargetCollection(String s) { this.targetCollection = s; }
    public String getTimestampField() { return timestampField; }
    public void setTimestampField(String s) { this.timestampField = s; }
    public String getStrategy() { return strategy; }
    public void setStrategy(String s) { this.strategy = s; }
    public Map<String, Object> getStrategyParams() { return strategyParams; }
    public void setStrategyParams(Map<String, Object> p) { this.strategyParams = p; }
    public long getWindowSizeMs() { return windowSizeMs; }
    public void setWindowSizeMs(long ms) { this.windowSizeMs = ms; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean e) { this.enabled = e; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant i) { this.createdAt = i; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant i) { this.updatedAt = i; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String id, sourceCollection, targetCollection, timestampField, strategy = "LAST_WRITE_WINS";
        private SyncJobEntity syncJob;
        private Map<String, Object> strategyParams;
        private long windowSizeMs = 30000;
        private boolean enabled = true;
        private Instant createdAt, updatedAt;

        public Builder id(String v) { id = v; return this; }
        public Builder syncJob(SyncJobEntity v) { syncJob = v; return this; }
        public Builder sourceCollection(String v) { sourceCollection = v; return this; }
        public Builder targetCollection(String v) { targetCollection = v; return this; }
        public Builder timestampField(String v) { timestampField = v; return this; }
        public Builder strategy(String v) { strategy = v; return this; }
        public Builder strategyParams(Map<String, Object> v) { strategyParams = v; return this; }
        public Builder windowSizeMs(long v) { windowSizeMs = v; return this; }
        public Builder enabled(boolean v) { enabled = v; return this; }
        public Builder createdAt(Instant v) { createdAt = v; return this; }
        public Builder updatedAt(Instant v) { updatedAt = v; return this; }

        public CollectionMappingEntity build() {
            return new CollectionMappingEntity(id, syncJob, sourceCollection, targetCollection,
                    timestampField, strategy, strategyParams, windowSizeMs, enabled, createdAt, updatedAt);
        }
    }
}
