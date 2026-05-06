package com.recon.engine.flink.source;

import com.recon.common.enums.DbType;
import com.recon.common.model.ChangeEvent;
import com.recon.common.spi.CdcSourceFactory;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

public class MongoCdcSourceFactory implements CdcSourceFactory {
    @Override
    public boolean supports(DbType dbType) { return dbType == DbType.MONGODB; }

    @Override
    public DataStream<ChangeEvent> createSource(StreamExecutionEnvironment env,
            Map<String, Object> config, Map<String, Object> mapping) {
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
                .map(this::parseFromDebeziumJson).name("deserialize-" + collection);
    }

    private ChangeEvent parseFromDebeziumJson(String json) {
        try {
            tools.jackson.databind.ObjectMapper m = new tools.jackson.databind.ObjectMapper();
            @SuppressWarnings("unchecked") Map<String, Object> raw = m.readValue(json, Map.class);
            ChangeEvent.ChangeEventBuilder b = ChangeEvent.builder();
            b.documentKey(raw.get("_id") != null ? raw.get("_id").toString() : null);
            b.eventTimestamp(System.currentTimeMillis());
            String op = (String) raw.get("op");
            b.op(switch (op != null ? op : "r") {
                case "c" -> com.recon.common.enums.OperationType.INSERT;
                case "u" -> com.recon.common.enums.OperationType.UPDATE;
                case "d" -> com.recon.common.enums.OperationType.DELETE;
                default -> com.recon.common.enums.OperationType.UPDATE;
            });
            if (raw.containsKey("after")) {
                @SuppressWarnings("unchecked") Map<String, Object> after = (Map<String, Object>) raw.get("after");
                b.after(after);
                if (after != null && after.containsKey("sourceCluster"))
                    b.sourceCluster((String) after.get("sourceCluster"));
            }
            return b.build();
        } catch (Exception e) { return ChangeEvent.builder().eventTimestamp(System.currentTimeMillis()).build(); }
    }
}
