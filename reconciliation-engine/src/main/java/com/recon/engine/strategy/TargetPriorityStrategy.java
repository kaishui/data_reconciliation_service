package com.recon.engine.strategy;

import com.recon.common.model.ConflictContext;
import com.recon.common.spi.ConflictStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Always picks the version currently in the target database.
 * This effectively means the target cluster always wins conflicts.
 */
public class TargetPriorityStrategy implements ConflictStrategy {

    private static final Logger log = LoggerFactory.getLogger(TargetPriorityStrategy.class);

    @Override
    public String name() {
        return "TARGET_PRIORITY";
    }

    @Override
    public Map<String, Object> resolve(ConflictContext ctx) {
        log.debug("TARGET_PRIORITY: key={}, keeping target version", ctx.getDocumentKey());
        return ctx.getTargetVersion();
    }
}
