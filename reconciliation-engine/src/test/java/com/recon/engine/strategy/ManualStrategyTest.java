package com.recon.engine.strategy;

import com.recon.common.model.ConflictContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ManualStrategy")
class ManualStrategyTest {

    private final ManualStrategy strategy = new ManualStrategy();

    @Test
    void shouldReturnName() {
        assertThat(strategy.name()).isEqualTo("MANUAL");
    }

    @Test
    void shouldAlwaysReturnNull() {
        var ctx = ConflictContext.builder()
                .documentKey("doc-1")
                .sourceVersion(Map.of("v", "source"))
                .targetVersion(Map.of("v", "target"))
                .build();

        var result = strategy.resolve(ctx);
        assertThat(result).isNull();
    }

    @Test
    void shouldReturnNullEvenWithTimestamps() {
        var ctx = ConflictContext.builder()
                .documentKey("doc-2")
                .sourceVersion(Map.of("v", 1))
                .targetVersion(Map.of("v", 2))
                .sourceTimestamp(2000L)
                .targetTimestamp(1000L)
                .build();

        var result = strategy.resolve(ctx);
        assertThat(result).isNull();
    }
}
