package com.recon.engine.strategy;

import com.recon.common.model.ConflictContext;
import com.recon.common.spi.ConflictStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Does not auto-resolve. Returns null to signal that the conflict
 * should be logged and queued for human review. The event is skipped
 * (not written to target).
 */
public class ManualStrategy implements ConflictStrategy {

    private static final Logger log = LoggerFactory.getLogger(ManualStrategy.class);

    @Override
    public String name() {
        return "MANUAL";
    }

    @Override
    public Map<String, Object> resolve(ConflictContext ctx) {
        log.info("MANUAL: conflict requires human review, key={}", ctx.getDocumentKey());
        return null; // Skip write
    }
}
