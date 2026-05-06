package com.recon.engine.flink;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
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

public class SyncJobBuilder {

    private static final Logger log = LoggerFactory.getLogger(SyncJobBuilder.class);
    private final Map<String, Object> jobConfig;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public SyncJobBuilder(Map<String, Object> jobConfig) { this.jobConfig = jobConfig; }

    public void buildAndExecute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        env.enableCheckpointing(checkpointIntervalMs);
        env.setParallelism(parallelism);

        Map<String, ConflictStrategy> strategyRegistry = buildStrategyRegistry(mappings);
        Map<String, Object> primaryMapping = mappings.get(0);
        String timestampField = (String) primaryMapping.get("timestamp_field");

        // CDC Source
        DbType sourceDbType = DbType.valueOf((String) sourceConfig.get("db_type"));
        DataStream<ChangeEvent> sourceStream = (sourceDbType == DbType.MONGODB)
                ? new MongoCdcSourceFactory().createSource(env, sourceConfig, primaryMapping)
                : new PostgresCdcSourceFactory().createSource(env, sourceConfig, primaryMapping);

        // Loop Prevention
        DataStream<ChangeEvent> filtered = sourceStream
                .filter(new LoopPreventionFilter(sourceClusterLabel, remoteClusterLabel))
                .name("loop-prevention");

        // Extract timestamp
        DataStream<ChangeEvent> withTimestamp = filtered.map(event -> {
            if (event.getAfter() != null && timestampField != null) {
                Object ts = event.getAfter().get(timestampField);
                if (ts instanceof Number) event.setDataTimestamp(((Number) ts).longValue());
                else if (ts instanceof String) {
                    try { event.setDataTimestamp(Long.parseLong((String) ts)); }
                    catch (NumberFormatException ignored) {}
                }
            }
            if (event.getDataTimestamp() == null) event.setDataTimestamp(event.getEventTimestamp());
            return event;
        }).name("extract-timestamp");

        // KeyBy → Windowed Conflict Resolver
        SingleOutputStreamOperator<ChangeEvent> resolved = withTimestamp
                .keyBy(event -> event.getDocumentKey() != null ? event.getDocumentKey() : "unknown")
                .process(new WindowedConflictResolver(defaultWindowMs, strategyRegistry,
                        syncJobId, jobName, sourceClusterLabel))
                .name("conflict-resolver");

        // Side output: conflict log → PostgreSQL
        DataStream<ConflictLogEntry> conflictLog = resolved.getSideOutput(WindowedConflictResolver.CONFLICT_LOG_TAG);
        String auditDbUrl = (String) jobConfig.get("audit_db_url");

        if (auditDbUrl != null) {
            String auditDbUser = (String) jobConfig.get("audit_db_user");
            String auditDbPassword = (String) jobConfig.get("audit_db_password");
            conflictLog.addSink(JdbcSink.sink(
                    "INSERT INTO conflict_log (id, sync_job_id, sync_job_name, collection_name,"
                            + " document_key, source_cluster, strategy_used, source_version, target_version,"
                            + " source_timestamp, target_timestamp, resolved_version, resolution_status,"
                            + " resolution_detail, occurred_at)"
                            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?::jsonb, ?, ?, ?)",
                    (PreparedStatement ps, ConflictLogEntry entry) -> {
                        ps.setString(1, entry.getId());
                        ps.setString(2, entry.getSyncJobId());
                        ps.setString(3, entry.getSyncJobName());
                        ps.setString(4, entry.getCollectionName());
                        ps.setString(5, entry.getDocumentKey());
                        ps.setString(6, entry.getSourceCluster());
                        ps.setString(7, entry.getStrategyUsed());
                        ps.setString(8, toJson(entry.getSourceVersion()));
                        ps.setString(9, toJson(entry.getTargetVersion()));
                        ps.setObject(10, entry.getSourceTimestamp());
                        ps.setObject(11, entry.getTargetTimestamp());
                        ps.setString(12, entry.getResolvedVersion() != null ? toJson(entry.getResolvedVersion()) : null);
                        ps.setString(13, entry.getResolutionStatus().name());
                        ps.setString(14, entry.getResolutionDetail());
                        ps.setTimestamp(15, Timestamp.from(entry.getOccurredAt()));
                    },
                    JdbcExecutionOptions.builder().withBatchSize(50).withMaxRetries(3).build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl(auditDbUrl).withDriverName("org.postgresql.Driver")
                            .withUsername(auditDbUser).withPassword(auditDbPassword).build()
            )).name("conflict-log-sink");
        }

        // Sink to target
        DbType targetDbType = DbType.valueOf((String) targetConfig.get("db_type"));
        if (targetDbType == DbType.MONGODB)
            resolved.addSink(new MongoSinkFactory().createSink(targetConfig, primaryMapping)).name("mongodb-sink");
        else
            resolved.addSink(new PostgresSinkFactory().createSink(targetConfig, primaryMapping)).name("postgres-sink");

        env.execute(jobName != null ? jobName : "reconciliation-sync-job");
    }

    private String toJson(Object obj) {
        try { return objectMapper.writeValueAsString(obj); }
        catch (JacksonException e) { return "{}"; }
    }

    private Map<String, ConflictStrategy> buildStrategyRegistry(List<Map<String, Object>> mappings) {
        Map<String, ConflictStrategy> registry = new HashMap<>();
        registry.put("LAST_WRITE_WINS", new LastWriteWinsStrategy());
        registry.put("SOURCE_PRIORITY", new SourcePriorityStrategy());
        registry.put("TARGET_PRIORITY", new TargetPriorityStrategy());
        registry.put("MANUAL", new ManualStrategy());
        return registry;
    }
}
