package com.recon.management.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "sync_jobs")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
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
    @Builder.Default
    private String status = "STOPPED";

    @Column(name = "flink_job_id", length = 255)
    private String flinkJobId;

    @Column(name = "flink_cluster_url", length = 512)
    private String flinkClusterUrl;

    @Column(nullable = false)
    @Builder.Default
    private int parallelism = 1;

    @Column(name = "checkpoint_interval_ms", nullable = false)
    @Builder.Default
    private long checkpointIntervalMs = 10000;

    @Column(name = "window_size_ms", nullable = false)
    @Builder.Default
    private long windowSizeMs = 30000;

    @OneToMany(mappedBy = "syncJob", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    @Builder.Default
    private List<CollectionMappingEntity> mappings = new ArrayList<>();

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
