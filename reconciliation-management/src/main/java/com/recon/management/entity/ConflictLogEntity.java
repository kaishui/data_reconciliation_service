package com.recon.management.entity;

import com.recon.common.enums.ResolutionStatus;
import io.hypersistence.utils.hibernate.type.json.JsonBinaryType;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;

import java.time.Instant;
import java.util.Map;

@Entity
@Table(name = "conflict_log")
public class ConflictLogEntity {

    @Id
    @Column(length = 36)
    private String id;

    @Column(name = "sync_job_id", nullable = false, length = 36)
    private String syncJobId;

    @Column(name = "sync_job_name", nullable = false, length = 255)
    private String syncJobName;

    @Column(name = "collection_name", nullable = false, length = 255)
    private String collectionName;

    @Column(name = "document_key", nullable = false, length = 512)
    private String documentKey;

    @Column(name = "source_cluster", nullable = false, length = 50)
    private String sourceCluster;

    @Column(name = "strategy_used", nullable = false, length = 50)
    private String strategyUsed;

    @Type(JsonBinaryType.class)
    @Column(name = "source_version", nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> sourceVersion;

    @Type(JsonBinaryType.class)
    @Column(name = "target_version", nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> targetVersion;

    @Column(name = "source_timestamp")
    private Long sourceTimestamp;

    @Column(name = "target_timestamp")
    private Long targetTimestamp;

    @Type(JsonBinaryType.class)
    @Column(name = "resolved_version", columnDefinition = "jsonb")
    private Map<String, Object> resolvedVersion;

    @Enumerated(EnumType.STRING)
    @Column(name = "resolution_status", nullable = false, length = 20)
    private ResolutionStatus resolutionStatus = ResolutionStatus.MANUAL_REQUIRED;

    @Column(name = "resolution_detail", columnDefinition = "TEXT")
    private String resolutionDetail;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt = Instant.now();

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    public ConflictLogEntity() {}

    public ConflictLogEntity(String id, String syncJobId, String syncJobName, String collectionName,
                              String documentKey, String sourceCluster, String strategyUsed,
                              Map<String, Object> sourceVersion, Map<String, Object> targetVersion,
                              Long sourceTimestamp, Long targetTimestamp,
                              Map<String, Object> resolvedVersion, ResolutionStatus resolutionStatus,
                              String resolutionDetail, Instant occurredAt, Instant resolvedAt) {
        this.id = id;
        this.syncJobId = syncJobId;
        this.syncJobName = syncJobName;
        this.collectionName = collectionName;
        this.documentKey = documentKey;
        this.sourceCluster = sourceCluster;
        this.strategyUsed = strategyUsed;
        this.sourceVersion = sourceVersion;
        this.targetVersion = targetVersion;
        this.sourceTimestamp = sourceTimestamp;
        this.targetTimestamp = targetTimestamp;
        this.resolvedVersion = resolvedVersion;
        this.resolutionStatus = resolutionStatus;
        this.resolutionDetail = resolutionDetail;
        this.occurredAt = occurredAt;
        this.resolvedAt = resolvedAt;
    }

    // Getters/Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getSyncJobId() { return syncJobId; }
    public void setSyncJobId(String id) { this.syncJobId = id; }
    public String getSyncJobName() { return syncJobName; }
    public void setSyncJobName(String n) { this.syncJobName = n; }
    public String getCollectionName() { return collectionName; }
    public void setCollectionName(String n) { this.collectionName = n; }
    public String getDocumentKey() { return documentKey; }
    public void setDocumentKey(String k) { this.documentKey = k; }
    public String getSourceCluster() { return sourceCluster; }
    public void setSourceCluster(String c) { this.sourceCluster = c; }
    public String getStrategyUsed() { return strategyUsed; }
    public void setStrategyUsed(String s) { this.strategyUsed = s; }
    public Map<String, Object> getSourceVersion() { return sourceVersion; }
    public void setSourceVersion(Map<String, Object> v) { this.sourceVersion = v; }
    public Map<String, Object> getTargetVersion() { return targetVersion; }
    public void setTargetVersion(Map<String, Object> v) { this.targetVersion = v; }
    public Long getSourceTimestamp() { return sourceTimestamp; }
    public void setSourceTimestamp(Long ts) { this.sourceTimestamp = ts; }
    public Long getTargetTimestamp() { return targetTimestamp; }
    public void setTargetTimestamp(Long ts) { this.targetTimestamp = ts; }
    public Map<String, Object> getResolvedVersion() { return resolvedVersion; }
    public void setResolvedVersion(Map<String, Object> v) { this.resolvedVersion = v; }
    public ResolutionStatus getResolutionStatus() { return resolutionStatus; }
    public void setResolutionStatus(ResolutionStatus s) { this.resolutionStatus = s; }
    public String getResolutionDetail() { return resolutionDetail; }
    public void setResolutionDetail(String d) { this.resolutionDetail = d; }
    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant i) { this.occurredAt = i; }
    public Instant getResolvedAt() { return resolvedAt; }
    public void setResolvedAt(Instant i) { this.resolvedAt = i; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String id, syncJobId, syncJobName, collectionName, documentKey,
                sourceCluster, strategyUsed, resolutionDetail;
        private Map<String, Object> sourceVersion, targetVersion, resolvedVersion;
        private Long sourceTimestamp, targetTimestamp;
        private ResolutionStatus resolutionStatus = ResolutionStatus.MANUAL_REQUIRED;
        private Instant occurredAt = Instant.now(), resolvedAt;

        public Builder id(String v) { id = v; return this; }
        public Builder syncJobId(String v) { syncJobId = v; return this; }
        public Builder syncJobName(String v) { syncJobName = v; return this; }
        public Builder collectionName(String v) { collectionName = v; return this; }
        public Builder documentKey(String v) { documentKey = v; return this; }
        public Builder sourceCluster(String v) { sourceCluster = v; return this; }
        public Builder strategyUsed(String v) { strategyUsed = v; return this; }
        public Builder sourceVersion(Map<String, Object> v) { sourceVersion = v; return this; }
        public Builder targetVersion(Map<String, Object> v) { targetVersion = v; return this; }
        public Builder sourceTimestamp(Long v) { sourceTimestamp = v; return this; }
        public Builder targetTimestamp(Long v) { targetTimestamp = v; return this; }
        public Builder resolvedVersion(Map<String, Object> v) { resolvedVersion = v; return this; }
        public Builder resolutionStatus(ResolutionStatus v) { resolutionStatus = v; return this; }
        public Builder resolutionDetail(String v) { resolutionDetail = v; return this; }
        public Builder occurredAt(Instant v) { occurredAt = v; return this; }
        public Builder resolvedAt(Instant v) { resolvedAt = v; return this; }

        public ConflictLogEntity build() {
            return new ConflictLogEntity(id, syncJobId, syncJobName, collectionName, documentKey,
                    sourceCluster, strategyUsed, sourceVersion, targetVersion, sourceTimestamp,
                    targetTimestamp, resolvedVersion, resolutionStatus, resolutionDetail,
                    occurredAt, resolvedAt);
        }
    }
}
