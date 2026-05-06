package com.recon.management.entity;

import com.recon.common.enums.ResolutionStatus;
import io.hypersistence.utils.hibernate.type.json.JsonBinaryType;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.Type;

import java.time.Instant;
import java.util.Map;

@Entity
@Table(name = "conflict_log")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
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
    @Builder.Default
    private ResolutionStatus resolutionStatus = ResolutionStatus.MANUAL_REQUIRED;

    @Column(name = "resolution_detail", columnDefinition = "TEXT")
    private String resolutionDetail;

    @Column(name = "occurred_at", nullable = false)
    @Builder.Default
    private Instant occurredAt = Instant.now();

    @Column(name = "resolved_at")
    private Instant resolvedAt;
}
