package com.recon.engine.strategy;

import com.recon.common.model.ConflictContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("TargetPriorityStrategy")
class TargetPriorityStrategyTest {

    private final TargetPriorityStrategy strategy = new TargetPriorityStrategy();

    @Test
    void shouldReturnName() {
        assertThat(strategy.name()).isEqualTo("TARGET_PRIORITY");
    }

    @Test
    void shouldAlwaysReturnTargetVersion() {
        var ctx = ConflictContext.builder()
                .sourceVersion(Map.of("v", "source"))
                .targetVersion(Map.of("v", "target"))
                .build();

        var result = strategy.resolve(ctx);
        assertThat(result).containsEntry("v", "target");
    }

    @Test
    void shouldReturnTargetEvenWhenSourceIsNewer() {
        var ctx = ConflictContext.builder()
                .sourceVersion(Map.of("v", "newer-source"))
                .targetVersion(Map.of("v", "older-target"))
                .sourceTimestamp(9999L)
                .targetTimestamp(1L)
                .build();

        var result = strategy.resolve(ctx);
        assertThat(result).containsEntry("v", "older-target");
    }
}
