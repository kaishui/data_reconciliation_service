package com.recon.engine.strategy;

import com.recon.common.model.ConflictContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("LastWriteWinsStrategy")
class LastWriteWinsStrategyTest {

    private final LastWriteWinsStrategy strategy = new LastWriteWinsStrategy();

    @Test
    void shouldReturnName() {
        assertThat(strategy.name()).isEqualTo("LAST_WRITE_WINS");
    }

    @Nested
    @DisplayName("Timestamp comparison")
    class TimestampComparison {

        @Test
        void shouldPickSourceWhenNewer() {
            var ctx = ConflictContext.builder()
                    .sourceVersion(Map.of("v", 2))
                    .targetVersion(Map.of("v", 1))
                    .sourceTimestamp(2000L)
                    .targetTimestamp(1000L)
                    .build();

            var result = strategy.resolve(ctx);
            assertThat(result).containsEntry("v", 2);
        }

        @Test
        void shouldPickTargetWhenNewer() {
            var ctx = ConflictContext.builder()
                    .sourceVersion(Map.of("v", 1))
                    .targetVersion(Map.of("v", 2))
                    .sourceTimestamp(1000L)
                    .targetTimestamp(2000L)
                    .build();

            var result = strategy.resolve(ctx);
            assertThat(result).containsEntry("v", 2);
        }

        @Test
        void shouldPickSourceWhenTimestampsEqual() {
            var ctx = ConflictContext.builder()
                    .sourceVersion(Map.of("v", "source"))
                    .targetVersion(Map.of("v", "target"))
                    .sourceTimestamp(1000L)
                    .targetTimestamp(1000L)
                    .build();

            var result = strategy.resolve(ctx);
            assertThat(result).containsEntry("v", "source");
        }
    }

    @Nested
    @DisplayName("Null timestamp handling")
    class NullTimestampHandling {

        @Test
        void shouldPickSourceWhenBothTimestampsNull() {
            var ctx = ConflictContext.builder()
                    .sourceVersion(Map.of("v", "from-source"))
                    .targetVersion(Map.of("v", "from-target"))
                    .build();

            var result = strategy.resolve(ctx);
            assertThat(result).containsEntry("v", "from-source");
        }

        @Test
        void shouldPickTargetWhenSourceTimestampNull() {
            var ctx = ConflictContext.builder()
                    .sourceVersion(Map.of("v", "source"))
                    .targetVersion(Map.of("v", "target"))
                    .targetTimestamp(1000L)
                    .build();

            var result = strategy.resolve(ctx);
            assertThat(result).containsEntry("v", "target");
        }

        @Test
        void shouldPickSourceWhenTargetTimestampNull() {
            var ctx = ConflictContext.builder()
                    .sourceVersion(Map.of("v", "source"))
                    .targetVersion(Map.of("v", "target"))
                    .sourceTimestamp(1000L)
                    .build();

            var result = strategy.resolve(ctx);
            assertThat(result).containsEntry("v", "source");
        }
    }
}
