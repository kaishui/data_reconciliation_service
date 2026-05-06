package com.recon.management.entity;

import io.hypersistence.utils.hibernate.type.json.JsonBinaryType;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.Type;

import java.time.Instant;
import java.util.Map;

@Entity
@Table(name = "collection_mappings",
       uniqueConstraints = @UniqueConstraint(columnNames = {"sync_job_id", "source_collection"}))
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CollectionMappingEntity {

    @Id
    @Column(length = 36)
    private String id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "sync_job_id", nullable = false)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private SyncJobEntity syncJob;

    @Column(name = "source_collection", nullable = false, length = 255)
    private String sourceCollection;

    @Column(name = "target_collection", nullable = false, length = 255)
    private String targetCollection;

    @Column(name = "timestamp_field", nullable = false, length = 255)
    private String timestampField;

    @Column(nullable = false, length = 50)
    @Builder.Default
    private String strategy = "LAST_WRITE_WINS";

    @Type(JsonBinaryType.class)
    @Column(name = "strategy_params", columnDefinition = "jsonb")
    private Map<String, Object> strategyParams;

    @Column(name = "window_size_ms", nullable = false)
    @Builder.Default
    private long windowSizeMs = 30000;

    @Column(nullable = false)
    @Builder.Default
    private boolean enabled = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    void prePersist() {
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    @PreUpdate
    void preUpdate() {
        this.updatedAt = Instant.now();
    }
}
