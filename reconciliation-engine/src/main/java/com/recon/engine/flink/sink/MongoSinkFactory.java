package com.recon.engine.flink.sink;

import com.recon.common.enums.DbType;
import com.recon.common.model.ChangeEvent;
import com.recon.common.spi.SinkFactory;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Map;

public class MongoSinkFactory implements SinkFactory {

    @Override
    public boolean supports(DbType dbType) { return dbType == DbType.MONGODB; }

    @Override
    @SuppressWarnings("deprecation")
    public SinkFunction<ChangeEvent> createSink(Map<String, Object> config, Map<String, Object> mapping) {
        // In production, use Flink MongoDB sink connector:
        // return MongoSink.<ChangeEvent>builder()
        //         .setUri((String) config.get("connection_string"))
        //         .setDatabase((String) config.getOrDefault("database", "admin"))
        //         .setCollection((String) mapping.get("target_collection"))
        //         .setSerializationSchema(...)
        //         .build();

        // Placeholder: log-based no-op sink for compilation
        return new org.apache.flink.streaming.api.functions.sink.RichSinkFunction<ChangeEvent>() {
            @Override
            public void invoke(ChangeEvent value, Context context) {
                // Production: write to MongoDB with sourceCluster injection
            }
        };
    }
}
