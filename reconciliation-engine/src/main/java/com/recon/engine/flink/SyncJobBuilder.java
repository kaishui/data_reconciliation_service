package com.recon.engine.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recon.common.enums.DbType;
import com.recon.common.model.ChangeEvent;
import com.recon.common.model.ConflictLogEntry;
import com.recon.common.spi.ConflictStrategy;
import com.recon.engine.flink.process.LoopPreventionFilter;
import com.recon.engine.flink.process.WindowedConflictResolver;
import com.recon.engine.flink.sink.MongoSinkFactory;
import com.recon.engine.flink.sink.PostgresSinkFactory;
import com.recon.engine.flink.source.MongoCdcSourceFactory;
import com.recon.engine.flink.source.PostgresCdcSourceFactory;
import com.recon.engine.strategy.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Assembles a complete Flink CDC sync job from configuration.
 *
 * Pipeline:
 *   CDC Source → Loop Prevention Filter → KeyBy → Windowed Conflict Resolver → Sink
 *                                                                              └→ Conflict Log (side output)
 */
public class SyncJobBuilder {

    private static final Logger log = LoggerFactory.getLogger(SyncJobBuilder.class);

    private final Map<String, Object> jobConfig;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public SyncJobBuilder(Map<String, Object> jobConfig) {
        this.jobConfig = jobConfig;
    }

    /**
     * Build and submit the Flink job.
     */
    public void buildAndExecute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Job config
        String jobName = (String) jobConfig.get("job_name");
        String syncJobId = (String) jobConfig.get("sync_job_id");
        String sourceClusterLabel = (String) jobConfig.get("source_cluster_label");
        String remoteClusterLabel = (String) jobConfig.get("remote_cluster_label");
        int parallelism = ((Number) jobConfig.getOrDefault("parallelism", 1)).intValue();
        long checkpointIntervalMs = ((Number) jobConfig.getOrDefault("checkpoint_interval_ms", 10000L)).longValue();
        long defaultWindowMs = ((Number) jobConfig.getOrDefault("window_size_ms", 30000L)).longValue();

        @SuppressWarnings("unchecked")
        Map<String, Object> sourceConfig = (Map<String, Object>) jobConfig.get("source_config");
        @SuppressWarnings("unchecked")
        Map<String, Object> targetConfig = (Map<String, Object>) jobConfig.get("target_config");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> mappings = (List<Map<String, Object>>) jobConfig.get("mappings");

        // Checkpoint configuration
        env.enableCheckpointing(checkpointIntervalMs);
        env.setParallelism(parallelism);

        // Build strategy registry from mappings
        Map<String, ConflictStrategy> strategyRegistry = buildStrategyRegistry(mappings);
        StrategyResolver strategyResolver = new StrategyResolver(strategyRegistry, mappings);

        // --- Build pipeline ---
        // For each mapping, create a CDC source and union them, or process per-collection
        // For simplicity, we process the first mapping as the primary source
        // In production, you'd fan-out per collection mapping

        Map<String, Object> primaryMapping = mappings.get(0);
        String sourceCollection = (String) primaryMapping.get("source_collection");
        String timestampField = (String) primaryMapping.get("timestamp_field");

        // 1. CDC Source
        DbType sourceDbType = DbType.valueOf((String) sourceConfig.get("db_type"));
        DataStream<ChangeEvent> sourceStream;

        if (sourceDbType == DbType.MONGODB) {
            MongoCdcSourceFactory sourceFactory = new MongoCdcSourceFactory();
            sourceStream = sourceFactory.createSource(env, sourceConfig, primaryMapping);
        } else {
            PostgresCdcSourceFactory sourceFactory = new PostgresCdcSourceFactory();
            sourceStream = sourceFactory.createSource(env, sourceConfig, primaryMapping);
        }

        // 2. Loop Prevention Filter
        DataStream<ChangeEvent> filtered = sourceStream
                .filter(new LoopPreventionFilter(sourceClusterLabel, remoteClusterLabel))
                .name("loop-prevention");

        // 3. Extract timestamp from configured field
        DataStream<ChangeEvent> withTimestamp = filtered
                .map(event -> {
                    if (event.getAfter() != null && timestampField != null) {
                        Object ts = event.getAfter().get(timestampField);
                        if (ts instanceof Number) {
                            event.setDataTimestamp(((Number) ts).longValue());
                        } else if (ts instanceof String) {
                            try {
                                event.setDataTimestamp(Long.parseLong((String) ts));
                            } catch (NumberFormatException ignored) {
                            }
                        }
                    }
                    if (event.getDataTimestamp() == null) {
                        event.setDataTimestamp(event.getEventTimestamp());
                    }
                    return event;
                })
                .name("extract-timestamp");

        // 4. KeyBy documentKey, then Windowed Conflict Resolver
        SingleOutputStreamOperator<ChangeEvent> resolved = withTimestamp
                .keyBy(event -> event.getDocumentKey() != null ? event.getDocumentKey() : "unknown")
                .process(new WindowedConflictResolver(
                        defaultWindowMs,
                        strategyRegistry,
                        syncJobId,
                        jobName,
                        sourceClusterLabel))
                .name("conflict-resolver");

        // 5. Side output: conflict log → PostgreSQL
        DataStream<ConflictLogEntry> conflictLog = resolved.getSideOutput(WindowedConflictResolver.CONFLICT_LOG_TAG);

        String auditDbUrl = (String) jobConfig.get("audit_db_url");
        String auditDbUser = (String) jobConfig.get("audit_db_user");
        String auditDbPassword = (String) jobConfig.get("audit_db_password");

        if (auditDbUrl != null) {
            conflictLog.addSink(JdbcSink.sink(
                    """
                    INSERT INTO conflict_log (id, sync_job_id, sync_job_name, collection_name,
                        document_key, source_cluster, strategy_used, source_version, target_version,
                        source_timestamp, target_timestamp, resolved_version, resolution_status,
                        resolution_detail, occurred_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?::jsonb, ?, ?, ?)
                    """,
                    (PreparedStatement ps, ConflictLogEntry entry) -> {
                        ps.setString(1, entry.getId());
                        ps.setString(2, entry.getSyncJobId());
                        ps.setString(3, entry.getSyncJobName());
                        ps.setString(4, entry.getCollectionName());
                        ps.setString(5, entry.getDocumentKey());
                        ps.setString(6, entry.getSourceCluster());
                        ps.setString(7, entry.getStrategyUsed());
                        ps.setString(8, objectMapper.writeValueAsString(entry.getSourceVersion()));
                        ps.setString(9, objectMapper.writeValueAsString(entry.getTargetVersion()));
                        ps.setObject(10, entry.getSourceTimestamp());
                        ps.setObject(11, entry.getTargetTimestamp());
                        ps.setString(12, entry.getResolvedVersion() != null
                                ? objectMapper.writeValueAsString(entry.getResolvedVersion()) : null);
                        ps.setString(13, entry.getResolutionStatus().name());
                        ps.setString(14, entry.getResolutionDetail());
                        ps.setTimestamp(15, Timestamp.from(entry.getOccurredAt()));
                    },
                    JdbcExecutionOptions.builder()
                            .withBatchSize(50)
                            .withMaxRetries(3)
                            .build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl(auditDbUrl)
                            .withDriverName("org.postgresql.Driver")
                            .withUsername(auditDbUser)
                            .withPassword(auditDbPassword)
                            .build()
            )).name("conflict-log-sink");
        } else {
            log.warn("No audit DB configured — conflict log side output will be discarded");
        }

        // 6. Sink to target
        DbType targetDbType = DbType.valueOf((String) targetConfig.get("db_type"));
        if (targetDbType == DbType.MONGODB) {
            MongoSinkFactory sinkFactory = new MongoSinkFactory();
            resolved.addSink(sinkFactory.createSink(targetConfig, primaryMapping))
                    .name("mongodb-sink");
        } else {
            PostgresSinkFactory sinkFactory = new PostgresSinkFactory();
            resolved.addSink(sinkFactory.createSink(targetConfig, primaryMapping))
                    .name("postgres-sink");
        }

        // Submit
        env.execute(jobName != null ? jobName : "reconciliation-sync-job");
    }

    /**
     * Build a registry of ConflictStrategy instances keyed by collection name.
     */
    private Map<String, ConflictStrategy> buildStrategyRegistry(List<Map<String, Object>> mappings) {
        Map<String, ConflictStrategy> registry = new HashMap<>();

        // Default strategies
        registry.put("LAST_WRITE_WINS", new LastWriteWinsStrategy());
        registry.put("SOURCE_PRIORITY", new SourcePriorityStrategy());
        registry.put("TARGET_PRIORITY", new TargetPriorityStrategy());
        registry.put("MANUAL", new ManualStrategy());

        // Per-collection mapping: collectionName → strategy name
        // The strategy is resolved at runtime via StrategyResolver
        return registry;
    }

    /**
     * Resolves which strategy to use for a given collection.
     */
    private record StrategyResolver(
            Map<String, ConflictStrategy> strategyRegistry,
            List<Map<String, Object>> mappings) {

        public ConflictStrategy resolve(String collectionName) {
            for (Map<String, Object> mapping : mappings) {
                if (collectionName.equals(mapping.get("source_collection"))) {
                    String strategyName = (String) mapping.get("strategy");
                    return strategyRegistry.get(strategyName);
                }
            }
            return strategyRegistry.get("LAST_WRITE_WINS");
        }
    }
}
