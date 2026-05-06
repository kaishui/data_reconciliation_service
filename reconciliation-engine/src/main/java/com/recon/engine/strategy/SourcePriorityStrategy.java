package com.recon.engine.strategy;

import com.recon.common.model.ConflictContext;
import com.recon.common.spi.ConflictStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Always picks the version from a configured priority source cluster.
 * The priority cluster is specified in strategyParams as "priorityCluster".
 */
public class SourcePriorityStrategy implements ConflictStrategy {

    private static final Logger log = LoggerFactory.getLogger(SourcePriorityStrategy.class);

    @Override
    public String name() {
        return "SOURCE_PRIORITY";
    }

    @Override
    public Map<String, Object> resolve(ConflictContext ctx) {
        String priorityCluster = getPriorityCluster(ctx.getMetadata());
        log.debug("SOURCE_PRIORITY: key={}, priorityCluster={}, sourceCluster={}",
                ctx.getDocumentKey(), priorityCluster, ctx.getSourceCluster());

        if (priorityCluster != null && priorityCluster.equals(ctx.getSourceCluster())) {
            return ctx.getSourceVersion();
        }

        return ctx.getTargetVersion();
    }

    private String getPriorityCluster(Map<String, Object> metadata) {
        if (metadata == null) return null;
        Object val = metadata.get("priorityCluster");
        return val != null ? val.toString() : null;
    }
}
