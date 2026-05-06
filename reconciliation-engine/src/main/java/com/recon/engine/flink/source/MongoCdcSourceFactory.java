package com.recon.engine.flink.source;

import com.recon.common.enums.DbType;
import com.recon.common.model.ChangeEvent;
import com.recon.common.spi.CdcSourceFactory;
import com.recon.engine.flink.serialization.ChangeEventDeserializer;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * Factory for creating MongoDB CDC sources using Flink CDC connector.
 * Uses MongoDB change streams for real-time data capture.
 */
public class MongoCdcSourceFactory implements CdcSourceFactory {

    @Override
    public boolean supports(DbType dbType) {
        return dbType == DbType.MONGODB;
    }

    @Override
    public DataStream<ChangeEvent> createSource(
            StreamExecutionEnvironment env,
            Map<String, Object> config,
            Map<String, Object> mapping) {

        String connectionString = (String) config.get("connection_string");
        String database = (String) config.getOrDefault("database", "admin");
        String collection = (String) mapping.get("source_collection");

        MongoDBSource<String> mongoSource = MongoDBSource.<String>builder()
                .connectionOptions(connectionString)
                .databaseList(database)
                .collectionList(database + "." + collection)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        return env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDB CDC - " + collection)
                .map(json -> {
                    // Parse Debezium JSON into ChangeEvent
                    // In production, use ChangeEventDeserializer for proper parsing
                    return parseFromDebeziumJson(json);
                })
                .name("deserialize-" + collection);
    }

    /**
     * Parse Debezium JSON into ChangeEvent.
     * The JsonDebeziumDeserializationSchema outputs a Debezium-format JSON string.
     */
    private ChangeEvent parseFromDebeziumJson(String json) {
        // Stub: in production, parse the Debezium JSON format
        // {"before":..., "after":..., "source":..., "op":"c/u/d/r", "ts_ms":...}
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            @SuppressWarnings("unchecked")
            Map<String, Object> raw = mapper.readValue(json, Map.class);

            ChangeEvent.ChangeEventBuilder builder = ChangeEvent.builder();
            builder.documentKey(raw.get("_id") != null ? raw.get("_id").toString() : null);
            builder.eventTimestamp(System.currentTimeMillis());

            String op = (String) raw.get("op");
            builder.op(switch (op != null ? op : "r") {
                case "c" -> com.recon.common.enums.OperationType.INSERT;
                case "u" -> com.recon.common.enums.OperationType.UPDATE;
                case "d" -> com.recon.common.enums.OperationType.DELETE;
                default -> com.recon.common.enums.OperationType.UPDATE;
            });

            // Extract source cluster from the document
            if (raw.containsKey("after")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> after = (Map<String, Object>) raw.get("after");
                builder.after(after);
                if (after != null && after.containsKey("sourceCluster")) {
                    builder.sourceCluster((String) after.get("sourceCluster"));
                }
            }

            return builder.build();
        } catch (Exception e) {
            return ChangeEvent.builder().eventTimestamp(System.currentTimeMillis()).build();
        }
    }
}
