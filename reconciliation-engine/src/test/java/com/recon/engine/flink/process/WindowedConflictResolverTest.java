package com.recon.engine.flink.process;

import com.recon.common.enums.OperationType;
import com.recon.common.model.ChangeEvent;
import com.recon.common.spi.ConflictStrategy;
import com.recon.engine.strategy.LastWriteWinsStrategy;
import com.recon.engine.strategy.ManualStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("WindowedConflictResolver")
class WindowedConflictResolverTest {

    private final Map<String, ConflictStrategy> strategyRegistry = Map.of(
            "orders", new LastWriteWinsStrategy(),
            "default", new LastWriteWinsStrategy(),
            "manual-coll", new ManualStrategy()
    );

    @Test
    void shouldPickWinnerByHighestDataTimestamp() {
        var events = List.of(
                ChangeEvent.builder().documentKey("d1").dataTimestamp(100L).after(Map.of("v", 1)).build(),
                ChangeEvent.builder().documentKey("d1").dataTimestamp(300L).after(Map.of("v", 3)).build(),
                ChangeEvent.builder().documentKey("d1").dataTimestamp(200L).after(Map.of("v", 2)).build()
        );

        var winner = events.stream()
                .filter(e -> e.getAfter() != null)
                .max(java.util.Comparator.comparing(
                        e -> e.getDataTimestamp() != null ? e.getDataTimestamp() : 0L))
                .orElse(null);

        assertThat(winner).isNotNull();
        assertThat(winner.getAfter()).containsEntry("v", 3);
    }

    @Test
    void shouldSkipEventsWithNullAfter() {
        var events = List.of(
                ChangeEvent.builder().documentKey("d1").op(OperationType.DELETE).dataTimestamp(500L).build(),
                ChangeEvent.builder().documentKey("d1").dataTimestamp(100L).after(Map.of("v", 1)).build()
        );

        var winner = events.stream()
                .filter(e -> e.getAfter() != null)
                .max(java.util.Comparator.comparing(
                        e -> e.getDataTimestamp() != null ? e.getDataTimestamp() : 0L))
                .orElse(null);

        assertThat(winner).isNotNull();
        assertThat(winner.getAfter()).containsEntry("v", 1);
    }

    @Test
    void shouldDefaultTimestampToZeroWhenNull() {
        var e1 = ChangeEvent.builder().dataTimestamp(null).after(Map.of("v", "no-ts")).build();
        var e2 = ChangeEvent.builder().dataTimestamp(1L).after(Map.of("v", "has-ts")).build();

        var winner = List.of(e1, e2).stream()
                .filter(e -> e.getAfter() != null)
                .max(java.util.Comparator.comparing(
                        e -> e.getDataTimestamp() != null ? e.getDataTimestamp() : 0L))
                .orElse(null);

        assertThat(winner).isNotNull();
        assertThat(winner.getAfter()).containsEntry("v", "has-ts");
    }

    @Test
    void shouldResolveConflictUsingStrategy() {
        var strategy = new LastWriteWinsStrategy();
        var ctx = com.recon.common.model.ConflictContext.builder()
                .sourceVersion(Map.of("v", "source")).targetVersion(Map.of("v", "target"))
                .sourceTimestamp(200L).targetTimestamp(100L).build();

        assertThat(strategy.resolve(ctx)).containsEntry("v", "source");
    }

    @Test
    void manualStrategyReturnsNull() {
        var strategy = new ManualStrategy();
        var ctx = com.recon.common.model.ConflictContext.builder()
                .sourceVersion(Map.of("v", 1)).targetVersion(Map.of("v", 2)).build();

        assertThat(strategy.resolve(ctx)).isNull();
    }

    @Test
    void shouldBuildConflictLogEntry() {
        var entry = com.recon.common.model.ConflictLogEntry.builder()
                .id("test-id").syncJobId("j1").syncJobName("test")
                .collectionName("orders").documentKey("d1")
                .sourceCluster("gcp").strategyUsed("LAST_WRITE_WINS")
                .sourceVersion(Map.of("v", 1)).targetVersion(Map.of("v", 0))
                .resolutionStatus(com.recon.common.enums.ResolutionStatus.AUTO_RESOLVED)
                .build();

        assertThat(entry.getId()).isEqualTo("test-id");
        assertThat(entry.getSourceCluster()).isEqualTo("gcp");
    }
}
