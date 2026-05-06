package com.recon.engine.flink.process;

import com.recon.common.model.ChangeEvent;
import com.recon.common.model.ConflictContext;
import com.recon.common.model.ConflictLogEntry;
import com.recon.common.enums.ResolutionStatus;
import com.recon.common.spi.ConflictStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * Buffers CDC events per document key within a sliding window,
 * then resolves conflicts when the window expires.
 *
 * Keyed by {@code documentKey}. For each key:
 *   1. Buffer incoming events in state
 *   2. Register a timer for (eventTime + windowSizeMs)
 *   3. On timer: drain buffer, detect conflicts, apply strategy, emit resolved event
 *   4. Side-output conflicts to PostgreSQL audit log
 */
public class WindowedConflictResolver
        extends KeyedProcessFunction<String, ChangeEvent, ChangeEvent> {

    private static final Logger log = LoggerFactory.getLogger(WindowedConflictResolver.class);

    /** Side output for conflict log entries. */
    public static final OutputTag<ConflictLogEntry> CONFLICT_LOG_TAG =
            new OutputTag<ConflictLogEntry>("conflict-log") {};

    private final long defaultWindowSizeMs;
    private final Map<String, ConflictStrategy> strategyRegistry;
    private final String syncJobId;
    private final String syncJobName;
    private final String sourceClusterLabel;

    // State: buffered events per key (collectionName -> list of events)
    private ValueState<Map<String, List<ChangeEvent>>> bufferState;
    // State: current timer timestamp
    private ValueState<Long> timerState;

    public WindowedConflictResolver(
            long defaultWindowSizeMs,
            Map<String, ConflictStrategy> strategyRegistry,
            String syncJobId,
            String syncJobName,
            String sourceClusterLabel) {
        this.defaultWindowSizeMs = defaultWindowSizeMs;
        this.strategyRegistry = strategyRegistry;
        this.syncJobId = syncJobId;
        this.syncJobName = syncJobName;
        this.sourceClusterLabel = sourceClusterLabel;
    }

    @Override
    public void open(Configuration parameters) {
        bufferState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("window-buffer", Map.class));
        timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("window-timer", Long.class));
    }

    @Override
    public void processElement(
            ChangeEvent event,
            Context ctx,
            Collector<ChangeEvent> out) throws Exception {

        String key = ctx.getCurrentKey();
        String collection = event.getCollectionName();
        long eventTime = event.getEventTimestamp() != null
                ? event.getEventTimestamp()
                : System.currentTimeMillis();
        long windowMs = getWindowSizeMs(collection);

        // Buffer event
        Map<String, List<ChangeEvent>> buffer = bufferState.value();
        if (buffer == null) {
            buffer = new HashMap<>();
        }
        buffer.computeIfAbsent(collection, k -> new ArrayList<>()).add(event);
        bufferState.update(buffer);

        // Register/update timer
        Long currentTimer = timerState.value();
        long timerTime = eventTime + windowMs;
        if (currentTimer == null || timerTime < currentTimer) {
            if (currentTimer != null) {
                ctx.timerService().deleteEventTimeTimer(currentTimer);
            }
            ctx.timerService().registerEventTimeTimer(timerTime);
            timerState.update(timerTime);
        }

        log.trace("Buffered event key={}, collection={}, bufferSize={}",
                key, collection, buffer.get(collection).size());
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<ChangeEvent> out) throws Exception {

        String key = ctx.getCurrentKey();
        Map<String, List<ChangeEvent>> buffer = bufferState.value();

        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        log.debug("Window expired for key={}, collections={}", key, buffer.keySet());

        for (Map.Entry<String, List<ChangeEvent>> entry : buffer.entrySet()) {
            String collection = entry.getKey();
            List<ChangeEvent> events = entry.getValue();

            if (events.isEmpty()) continue;

            // Pick the winning event: latest by dataTimestamp
            ChangeEvent winner = pickWinner(events, collection, ctx);

            if (winner != null) {
                // Check for conflict with target version
                ChangeEvent resolved = checkAndResolveConflict(winner, collection, ctx);

                if (resolved != null) {
                    out.collect(resolved);
                }
            }
        }

        // Clear state
        bufferState.clear();
        timerState.clear();
    }

    /**
     * Pick the event with the highest dataTimestamp from the buffer.
     */
    private ChangeEvent pickWinner(List<ChangeEvent> events, String collection, OnTimerContext ctx) {
        return events.stream()
                .filter(e -> e.getAfter() != null)
                .max(Comparator.comparing(
                        e -> e.getDataTimestamp() != null ? e.getDataTimestamp() : 0L))
                .orElse(null);
    }

    /**
     * Check if there's a conflict with the target version.
     * In practice, this would query the target DB for the current document.
     * For now, we apply the strategy based on what's in the buffer.
     */
    private ChangeEvent checkAndResolveConflict(
            ChangeEvent winner,
            String collection,
            OnTimerContext ctx) {

        ConflictStrategy strategy = getStrategy(collection);
        if (strategy == null) {
            log.warn("No strategy configured for collection={}, using LAST_WRITE_WINS fallback", collection);
            return winner; // No conflict to resolve — just emit the winner
        }

        // For the sliding window: if there were multiple versions, resolve conflict
        // between source and target (target would be fetched from DB in production)
        ConflictContext conflictCtx = ConflictContext.builder()
                .documentKey(winner.getDocumentKey())
                .collectionName(collection)
                .sourceVersion(winner.getAfter())
                .targetVersion(null) // TODO: fetch from target DB
                .sourceTimestamp(winner.getDataTimestamp())
                .targetTimestamp(null) // TODO: from target DB
                .sourceCluster(winner.getSourceCluster())
                .metadata(getStrategyParams(collection))
                .build();

        Map<String, Object> resolved = strategy.resolve(conflictCtx);

        if (resolved == null) {
            // Manual strategy — log conflict and skip
            logConflict(winner, conflictCtx, strategy.name(), ResolutionStatus.MANUAL_REQUIRED, "Manual strategy — human review required", ctx);
            return null;
        }

        // Log conflict if source and target differ (target is null for now)
        if (conflictCtx.getTargetVersion() != null) {
            logConflict(winner, conflictCtx, strategy.name(), ResolutionStatus.AUTO_RESOLVED,
                    "Resolved by " + strategy.name(), ctx);
        }

        // Return winner with resolved version
        ChangeEvent resolvedEvent = ChangeEvent.builder()
                .documentKey(winner.getDocumentKey())
                .collectionName(winner.getCollectionName())
                .op(winner.getOp())
                .after(resolved)
                .sourceCluster(sourceClusterLabel)
                .eventTimestamp(System.currentTimeMillis())
                .dataTimestamp(winner.getDataTimestamp())
                .build();

        return resolvedEvent;
    }

    private void logConflict(
            ChangeEvent event,
            ConflictContext ctx,
            String strategyUsed,
            ResolutionStatus status,
            String detail,
            OnTimerContext ctx2) {

        ConflictLogEntry logEntry = ConflictLogEntry.builder()
                .id(UUID.randomUUID().toString())
                .syncJobId(syncJobId)
                .syncJobName(syncJobName)
                .collectionName(event.getCollectionName())
                .documentKey(event.getDocumentKey())
                .sourceCluster(sourceClusterLabel)
                .strategyUsed(strategyUsed)
                .sourceVersion(ctx.getSourceVersion())
                .targetVersion(ctx.getTargetVersion())
                .sourceTimestamp(ctx.getSourceTimestamp())
                .targetTimestamp(ctx.getTargetTimestamp())
                .resolvedVersion(status == ResolutionStatus.AUTO_RESOLVED ? ctx.getSourceVersion() : null)
                .resolutionStatus(status)
                .resolutionDetail(detail)
                .occurredAt(Instant.now())
                .build();

        ctx2.output(CONFLICT_LOG_TAG, logEntry);
    }

    private ConflictStrategy getStrategy(String collection) {
        // Strategy is resolved per-collection from the registry.
        // The SyncJobBuilder populates this registry based on collection_mappings.
        return strategyRegistry.getOrDefault(collection, strategyRegistry.get("default"));
    }

    private Map<String, Object> getStrategyParams(String collection) {
        // TODO: return per-collection strategy params from config
        return Map.of();
    }

    private long getWindowSizeMs(String collection) {
        // TODO: return per-collection window size from config
        return defaultWindowSizeMs;
    }
}
