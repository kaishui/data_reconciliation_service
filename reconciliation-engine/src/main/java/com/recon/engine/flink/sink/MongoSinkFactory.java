package com.recon.engine.flink.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.ReplaceOptions;
import com.recon.common.enums.DbType;
import com.recon.common.model.ChangeEvent;
import com.recon.common.spi.SinkFactory;
import com.recon.engine.flink.process.SourceClusterInjector;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

import java.util.Map;

/**
 * Factory for creating MongoDB sink connectors.
 * Writes resolved ChangeEvents to the target MongoDB cluster.
 */
public class MongoSinkFactory implements SinkFactory {

    @Override
    public boolean supports(DbType dbType) {
        return dbType == DbType.MONGODB;
    }

    @Override
    public SinkFunction<ChangeEvent> createSink(
            Map<String, Object> config,
            Map<String, Object> mapping) {

        String connectionString = (String) config.get("connection_string");
        String database = (String) config.getOrDefault("database", "admin");
        String collection = (String) mapping.get("target_collection");
        String sourceClusterLabel = (String) config.get("cluster_label");

        SourceClusterInjector injector = new SourceClusterInjector(
                sourceClusterLabel != null ? sourceClusterLabel : "unknown");

        return MongoSink.<ChangeEvent>builder()
                .setConnectionOptions(connectionString)
                .setDatabase(database)
                .setCollection(collection)
                .setBatchSize(100)
                .setMaxRetries(3)
                .setSerializationSchema(
                        (event, context) -> {
                            // Convert ChangeEvent to BSON Document
                            Document doc = convertToDocument(event);
                            // Inject sourceCluster before writing
                            injector.inject(doc, event);
                            return doc;
                        })
                .build();
    }

    private Document convertToDocument(ChangeEvent event) {
        if (event.getAfter() == null) return new Document();
        try {
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(event.getAfter());
            return Document.parse(json);
        } catch (Exception e) {
            return new Document(event.getAfter());
        }
    }
}
