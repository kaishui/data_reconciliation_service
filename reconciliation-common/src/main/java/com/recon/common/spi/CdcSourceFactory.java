package com.recon.common.spi;

import com.recon.common.enums.DbType;
import com.recon.common.model.ChangeEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * Factory for creating CDC source connectors.
 * One implementation per database type (MongoDB, PostgreSQL).
 */
public interface CdcSourceFactory {

    /** Which database type this factory supports. */
    boolean supports(DbType dbType);

    /**
     * Create a Flink CDC source DataStream.
     *
     * @param env        Flink execution environment
     * @param config     database connection configuration
     * @param mapping    collection/table mapping (sourceCollection, timestampField, etc.)
     * @return DataStream of canonical ChangeEvents
     */
    DataStream<ChangeEvent> createSource(
            StreamExecutionEnvironment env,
            Map<String, Object> config,
            Map<String, Object> mapping);
}
