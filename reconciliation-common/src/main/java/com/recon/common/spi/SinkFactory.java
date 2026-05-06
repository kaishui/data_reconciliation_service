package com.recon.common.spi;

import com.recon.common.enums.DbType;
import com.recon.common.model.ChangeEvent;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Map;

/**
 * Factory for creating database sink connectors.
 * One implementation per database type (MongoDB, PostgreSQL).
 */
public interface SinkFactory {

    /** Which database type this factory supports. */
    boolean supports(DbType dbType);

    /**
     * Create a Flink sink function.
     *
     * @param config   database connection configuration
     * @param mapping  collection/table mapping (targetCollection, etc.)
     * @return SinkFunction for ChangeEvents
     */
    SinkFunction<ChangeEvent> createSink(
            Map<String, Object> config,
            Map<String, Object> mapping);
}
