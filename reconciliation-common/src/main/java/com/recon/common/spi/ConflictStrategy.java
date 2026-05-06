package com.recon.common.spi;

import com.recon.common.model.ConflictContext;

import java.util.Map;

/**
 * Pluggable conflict resolution strategy.
 * Implementations decide which version wins when the same document
 * was modified on both clusters within the sliding window.
 *
 * New strategies can be added as a JAR drop-in — register via
 * {@code META-INF/services/com.recon.common.spi.ConflictStrategy}
 * or Spring component scanning.
 */
public interface ConflictStrategy {

    /**
     * Unique strategy identifier used in collection mapping configuration.
     * Examples: "LAST_WRITE_WINS", "SOURCE_PRIORITY", "MANUAL".
     */
    String name();

    /**
     * Resolve a conflict.
     *
     * @param ctx context with source/target versions and timestamps
     * @return the winning document, or null to skip this write
     */
    Map<String, Object> resolve(ConflictContext ctx);
}
