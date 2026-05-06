package com.recon.engine.flink.process;

import com.recon.common.enums.ResolutionStatus;
import com.recon.common.model.ChangeEvent;
import com.recon.common.model.ConflictContext;
import com.recon.common.model.ConflictLogEntry;
import com.recon.common.spi.ConflictStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public class WindowedConflictResolver
        extends KeyedProcessFunction<String, ChangeEvent, ChangeEvent> {

    private static final Logger log = LoggerFactory.getLogger(WindowedConflictResolver.class);
    public static final OutputTag<ConflictLogEntry> CONFLICT_LOG_TAG =
            new OutputTag<ConflictLogEntry>("conflict-log") {};

    private final long defaultWindowSizeMs;
    private final Map<String, ConflictStrategy> strategyRegistry;
    private final String syncJobId, syncJobName, sourceClusterLabel;
    private transient ValueState<Map<String, List<ChangeEvent>>> bufferState;
    private transient ValueState<Long> timerState;

    public WindowedConflictResolver(long defaultWindowSizeMs,
            Map<String, ConflictStrategy> strategyRegistry,
            String syncJobId, String syncJobName, String sourceClusterLabel) {
        this.defaultWindowSizeMs = defaultWindowSizeMs;
        this.strategyRegistry = strategyRegistry;
        this.syncJobId = syncJobId;
        this.syncJobName = syncJobName;
        this.sourceClusterLabel = sourceClusterLabel;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void open(Configuration parameters) {
        bufferState = getRuntimeContext().getState(
                new ValueStateDescriptor("window-buffer", Map.class));
        var timerDesc = new ValueStateDescriptor<>("window-timer", Types.LONG);
        timerState = getRuntimeContext().getState(timerDesc);
    }

    @Override
    public void processElement(ChangeEvent event, Context ctx, Collector<ChangeEvent> out) throws Exception {
        String key = ctx.getCurrentKey();
        String collection = event.getCollectionName();
        long eventTime = event.getEventTimestamp() != null ? event.getEventTimestamp() : System.currentTimeMillis();
        long windowMs = getWindowSizeMs(collection);

        Map<String, List<ChangeEvent>> buffer = bufferState.value();
        if (buffer == null) buffer = new HashMap<>();
        buffer.computeIfAbsent(collection, k -> new ArrayList<>()).add(event);
        bufferState.update(buffer);

        Long currentTimer = timerState.value();
        long timerTime = eventTime + windowMs;
        if (currentTimer == null || timerTime < currentTimer) {
            if (currentTimer != null) ctx.timerService().deleteEventTimeTimer(currentTimer);
            ctx.timerService().registerEventTimeTimer(timerTime);
            timerState.update(timerTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ChangeEvent> out) throws Exception {
        Map<String, List<ChangeEvent>> buffer = bufferState.value();
        if (buffer == null || buffer.isEmpty()) return;

        for (Map.Entry<String, List<ChangeEvent>> entry : buffer.entrySet()) {
            List<ChangeEvent> events = entry.getValue();
            if (events.isEmpty()) continue;
            ChangeEvent winner = events.stream()
                    .filter(e -> e.getAfter() != null)
                    .max(Comparator.comparing(e -> e.getDataTimestamp() != null ? e.getDataTimestamp() : 0L))
                    .orElse(null);
            if (winner == null) continue;
            ChangeEvent resolved = checkAndResolveConflict(winner, entry.getKey(), ctx);
            if (resolved != null) out.collect(resolved);
        }
        bufferState.clear();
        timerState.clear();
    }

    private ChangeEvent checkAndResolveConflict(ChangeEvent winner, String collection, OnTimerContext ctx) {
        ConflictStrategy strategy = strategyRegistry.getOrDefault(collection,
                strategyRegistry.get("LAST_WRITE_WINS"));
        ConflictContext conflictCtx = ConflictContext.builder()
                .documentKey(winner.getDocumentKey()).collectionName(collection)
                .sourceVersion(winner.getAfter()).sourceTimestamp(winner.getDataTimestamp())
                .sourceCluster(winner.getSourceCluster()).metadata(Map.of()).build();

        Map<String, Object> resolved = strategy.resolve(conflictCtx);
        if (resolved == null) {
            logConflict(winner, conflictCtx, strategy.name(), ResolutionStatus.MANUAL_REQUIRED,
                    "Manual strategy — human review required", ctx);
            return null;
        }
        return ChangeEvent.builder()
                .documentKey(winner.getDocumentKey()).collectionName(winner.getCollectionName())
                .op(winner.getOp()).after(resolved).sourceCluster(sourceClusterLabel)
                .eventTimestamp(System.currentTimeMillis()).dataTimestamp(winner.getDataTimestamp())
                .build();
    }

    private void logConflict(ChangeEvent event, ConflictContext ctx, String strategyUsed,
            ResolutionStatus status, String detail, OnTimerContext ctx2) {
        ConflictLogEntry logEntry = ConflictLogEntry.builder()
                .id(UUID.randomUUID().toString()).syncJobId(syncJobId).syncJobName(syncJobName)
                .collectionName(event.getCollectionName()).documentKey(event.getDocumentKey())
                .sourceCluster(sourceClusterLabel).strategyUsed(strategyUsed)
                .sourceVersion(ctx.getSourceVersion()).targetVersion(ctx.getTargetVersion())
                .sourceTimestamp(ctx.getSourceTimestamp()).targetTimestamp(ctx.getTargetTimestamp())
                .resolvedVersion(status == ResolutionStatus.AUTO_RESOLVED ? ctx.getSourceVersion() : null)
                .resolutionStatus(status).resolutionDetail(detail).occurredAt(Instant.now())
                .build();
        ctx2.output(CONFLICT_LOG_TAG, logEntry);
    }

    private long getWindowSizeMs(String collection) { return defaultWindowSizeMs; }
}
