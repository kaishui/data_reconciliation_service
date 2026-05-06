package com.recon.common.model;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConflictContextTest {

    @Test
    void shouldBuildWithAllFields() {
        var ctx = ConflictContext.builder()
                .documentKey("doc-123")
                .collectionName("orders")
                .sourceVersion(Map.of("status", "new"))
                .targetVersion(Map.of("status", "old"))
                .sourceTimestamp(1000L)
                .targetTimestamp(500L)
                .sourceCluster("gcp")
                .metadata(Map.of("priorityCluster", "gcp"))
                .build();

        assertThat(ctx.getDocumentKey()).isEqualTo("doc-123");
        assertThat(ctx.getCollectionName()).isEqualTo("orders");
        assertThat(ctx.getSourceVersion()).containsEntry("status", "new");
        assertThat(ctx.getTargetVersion()).containsEntry("status", "old");
        assertThat(ctx.getSourceTimestamp()).isEqualTo(1000L);
        assertThat(ctx.getTargetTimestamp()).isEqualTo(500L);
        assertThat(ctx.getSourceCluster()).isEqualTo("gcp");
        assertThat(ctx.getMetadata()).containsEntry("priorityCluster", "gcp");
    }

    @Test
    void shouldAllowNullMetadata() {
        var ctx = ConflictContext.builder()
                .documentKey("key")
                .build();

        assertThat(ctx.getMetadata()).isNull();
    }

    @Test
    void shouldAllowNullTimestamps() {
        var ctx = ConflictContext.builder()
                .documentKey("key")
                .sourceVersion(Map.of("a", "b"))
                .build();

        assertThat(ctx.getSourceTimestamp()).isNull();
        assertThat(ctx.getTargetTimestamp()).isNull();
    }
}
