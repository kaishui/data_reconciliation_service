package com.recon.engine.strategy;

import com.recon.common.model.ConflictContext;
import com.recon.common.spi.ConflictStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Resolves conflicts by comparing timestamps — the version with the newest
 * timestamp wins. If timestamps are equal, the source version wins.
 */
public class LastWriteWinsStrategy implements ConflictStrategy {

    private static final Logger log = LoggerFactory.getLogger(LastWriteWinsStrategy.class);

    @Override
    public String name() {
        return "LAST_WRITE_WINS";
    }

    @Override
    public Map<String, Object> resolve(ConflictContext ctx) {
        log.debug("LAST_WRITE_WINS: key={}, sourceTs={}, targetTs={}",
                ctx.getDocumentKey(), ctx.getSourceTimestamp(), ctx.getTargetTimestamp());

        if (ctx.getSourceTimestamp() == null && ctx.getTargetTimestamp() == null) {
            // No timestamps — source wins by default
            log.info("No timestamps available, source wins: key={}", ctx.getDocumentKey());
            return ctx.getSourceVersion();
        }

        if (ctx.getSourceTimestamp() == null) {
            return ctx.getTargetVersion();
        }

        if (ctx.getTargetTimestamp() == null) {
            return ctx.getSourceVersion();
        }

        if (ctx.getSourceTimestamp() >= ctx.getTargetTimestamp()) {
            return ctx.getSourceVersion();
        }

        return ctx.getTargetVersion();
    }
}
