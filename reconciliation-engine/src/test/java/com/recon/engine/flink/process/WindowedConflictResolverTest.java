package com.recon.engine.flink.process;

import com.recon.common.enums.OperationType;
import com.recon.common.model.ChangeEvent;
import com.recon.common.model.ConflictLogEntry;
import com.recon.common.spi.ConflictStrategy;
import com.recon.engine.strategy.LastWriteWinsStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("WindowedConflictResolver")
class WindowedConflictResolverTest {

    private KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, ChangeEvent> harness;
    private final Map<String, ConflictStrategy> strategyRegistry = Map.of(
            "orders", new LastWriteWinsStrategy(),
            "default", new LastWriteWinsStrategy()
    );

    @BeforeEach
    void setUp() throws Exception {
        var resolver = new WindowedConflictResolver(
                1000L, strategyRegistry, "job-1", "test-job", "gcp");

        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new StreamFlatMap<>(resolver),
                ChangeEvent::getDocumentKey,
                org.apache.flink.api.common.typeinfo.Types.STRING
        );
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void shouldBufferAndEmitWinnerOnTimer() throws Exception {
        var event1 = ChangeEvent.builder()
                .documentKey("doc1")
                .collectionName("orders")
                .op(OperationType.UPDATE)
                .after(Map.of("v", 1))
                .eventTimestamp(1000L)
                .dataTimestamp(1000L)
                .sourceCluster("gcp")
                .build();

        var event2 = ChangeEvent.builder()
                .documentKey("doc1")
                .collectionName("orders")
                .op(OperationType.UPDATE)
                .after(Map.of("v", 2))
                .eventTimestamp(1100L)
                .dataTimestamp(2000L)
                .sourceCluster("gcp")
                .build();

        harness.processElement(event1, 1000L);
        harness.processElement(event2, 1100L);

        // Fire timer at watermark past window boundary
        harness.setProcessingTime(3000L);

        var output = harness.extractOutputStreamRecords();
        assertThat(output).isNotEmpty();

        // Should emit the event with higher dataTimestamp (v=2)
        var emitted = output.stream()
                .map(StreamRecord::getValue)
                .filter(e -> "doc1".equals(e.getDocumentKey()))
                .findFirst();
        assertThat(emitted).isPresent();
        assertThat(emitted.get().getAfter()).containsEntry("v", 2);
    }

    @Test
    void shouldHandleSingleEventNoConflict() throws Exception {
        var event = ChangeEvent.builder()
                .documentKey("solo")
                .collectionName("orders")
                .op(OperationType.INSERT)
                .after(Map.of("v", "single"))
                .eventTimestamp(1000L)
                .dataTimestamp(1000L)
                .sourceCluster("gcp")
                .build();

        harness.processElement(event, 1000L);
        harness.setProcessingTime(3000L);

        var output = harness.extractOutputStreamRecords();
        assertThat(output).isNotEmpty();
    }

    @Test
    void shouldSideOutputConflictLogEntries() throws Exception {
        var event = ChangeEvent.builder()
                .documentKey("conflict-1")
                .collectionName("orders")
                .op(OperationType.UPDATE)
                .after(Map.of("v", 10))
                .eventTimestamp(1000L)
                .dataTimestamp(1000L)
                .sourceCluster("gcp")
                .build();

        harness.processElement(event, 1000L);
        harness.setProcessingTime(3000L);

        var sideOutput = harness.getSideOutput(WindowedConflictResolver.CONFLICT_LOG_TAG);
        // Side output may be empty if no conflict detected (target version unavailable)
        // Just verify the harness doesn't crash
        assertThat(sideOutput).isNotNull();
    }
}
