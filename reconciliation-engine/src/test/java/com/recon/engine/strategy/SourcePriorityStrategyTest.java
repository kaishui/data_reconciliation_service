package com.recon.engine.strategy;

import com.recon.common.model.ConflictContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("SourcePriorityStrategy")
class SourcePriorityStrategyTest {

    private final SourcePriorityStrategy strategy = new SourcePriorityStrategy();

    @Test
    void shouldReturnName() {
        assertThat(strategy.name()).isEqualTo("SOURCE_PRIORITY");
    }

    @Test
    void shouldPickSourceWhenPriorityClusterMatches() {
        var ctx = ConflictContext.builder()
                .sourceVersion(Map.of("v", "source"))
                .targetVersion(Map.of("v", "target"))
                .sourceCluster("gcp")
                .metadata(Map.of("priorityCluster", "gcp"))
                .build();

        var result = strategy.resolve(ctx);
        assertThat(result).containsEntry("v", "source");
    }

    @Test
    void shouldPickTargetWhenPriorityClusterDoesNotMatch() {
        var ctx = ConflictContext.builder()
                .sourceVersion(Map.of("v", "source"))
                .targetVersion(Map.of("v", "target"))
                .sourceCluster("hic")
                .metadata(Map.of("priorityCluster", "gcp"))
                .build();

        var result = strategy.resolve(ctx);
        assertThat(result).containsEntry("v", "target");
    }

    @Test
    void shouldPickTargetWhenMetadataIsNull() {
        var ctx = ConflictContext.builder()
                .sourceVersion(Map.of("v", "source"))
                .targetVersion(Map.of("v", "target"))
                .sourceCluster("gcp")
                .build();

        var result = strategy.resolve(ctx);
        assertThat(result).containsEntry("v", "target");
    }

    @Test
    void shouldPickTargetWhenPriorityClusterNotSet() {
        var ctx = ConflictContext.builder()
                .sourceVersion(Map.of("v", "source"))
                .targetVersion(Map.of("v", "target"))
                .sourceCluster("gcp")
                .metadata(Map.of("other", "value"))
                .build();

        var result = strategy.resolve(ctx);
        assertThat(result).containsEntry("v", "target");
    }
}
