package com.recon.engine.flink.sink;

import com.recon.common.enums.DbType;
import com.recon.common.model.ChangeEvent;
import com.recon.common.spi.SinkFactory;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Map;

/**
 * Factory for creating PostgreSQL sink connectors.
 * Writes resolved ChangeEvents to the target PostgreSQL table.
 *
 * This is a future extension point — PostgreSQL support planned for Milestone 5.
 */
public class PostgresSinkFactory implements SinkFactory {

    @Override
    public boolean supports(DbType dbType) {
        return dbType == DbType.POSTGRESQL;
    }

    @Override
    public SinkFunction<ChangeEvent> createSink(
            Map<String, Object> config,
            Map<String, Object> mapping) {

        // TODO: Implement with flink-connector-jdbc when PostgreSQL support is enabled
        // JdbcExecutionOptions execOptions = JdbcExecutionOptions.builder()
        //         .withBatchSize(100)
        //         .withMaxRetries(3)
        //         .build();
        //
        // JdbcSink<ChangeEvent> sink = JdbcSink.sink(
        //         upsertSql,
        //         (statement, event) -> { /* bind params */ },
        //         execOptions,
        //         new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        //                 .withUrl(url)
        //                 .withDriverName("org.postgresql.Driver")
        //                 .withUsername(username)
        //                 .withPassword(password)
        //                 .build()
        // );
        //
        // return sink;

        throw new UnsupportedOperationException(
                "PostgreSQL sink not yet implemented. Planned for Milestone 5.");
    }
}
