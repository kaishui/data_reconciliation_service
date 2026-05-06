package com.recon.engine.flink.source;

import com.recon.common.enums.DbType;
import com.recon.common.model.ChangeEvent;
import com.recon.common.spi.CdcSourceFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * Factory for creating PostgreSQL CDC sources using Flink CDC connector.
 * Uses PostgreSQL WAL decoding (pgoutput plugin) for change data capture.
 *
 * This is a future extension point — PostgreSQL support planned for Milestone 5.
 */
public class PostgresCdcSourceFactory implements CdcSourceFactory {

    @Override
    public boolean supports(DbType dbType) {
        return dbType == DbType.POSTGRESQL;
    }

    @Override
    public DataStream<ChangeEvent> createSource(
            StreamExecutionEnvironment env,
            Map<String, Object> config,
            Map<String, Object> mapping) {

        String hostname = (String) config.get("hostname");
        int port = ((Number) config.getOrDefault("port", 5432)).intValue();
        String database = (String) config.get("database");
        String schema = (String) mapping.getOrDefault("source_schema", "public");
        String table = (String) mapping.get("source_collection");
        String username = (String) config.get("username");
        String password = (String) config.get("password");

        // TODO: Implement with flink-connector-postgres-cdc when enabled
        // PostgresCdcSource<ChangeEvent> source = PostgresCdcSource.<ChangeEvent>builder()
        //         .hostname(hostname)
        //         .port(port)
        //         .database(database)
        //         .schemaList(schema)
        //         .tableList(schema + "." + table)
        //         .username(username)
        //         .password(password)
        //         .deserializer(new ChangeEventDeserializer())
        //         .build();
        //
        // return env.fromSource(source, WatermarkStrategy.noWatermarks(),
        //         "PostgreSQL CDC - " + table);

        throw new UnsupportedOperationException(
                "PostgreSQL CDC not yet implemented. Planned for Milestone 5.");
    }
}
