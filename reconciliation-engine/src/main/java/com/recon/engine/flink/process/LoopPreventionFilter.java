package com.recon.engine.flink.process;

import com.recon.common.model.ChangeEvent;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drops CDC events that originated from the remote cluster,
 * preventing infinite sync loops.
 *
 * For a GCP→HIC sync job, this filter drops events where
 * sourceCluster == "hic" (they were already synced by the HIC→GCP job).
 */
public class LoopPreventionFilter extends RichFilterFunction<ChangeEvent> {

    private static final Logger log = LoggerFactory.getLogger(LoopPreventionFilter.class);

    private final String sourceClusterLabel;
    private final String remoteClusterLabel;

    /**
     * @param sourceClusterLabel the cluster this job reads FROM (e.g., "gcp")
     * @param remoteClusterLabel the cluster this job writes TO (e.g., "hic")
     */
    public LoopPreventionFilter(String sourceClusterLabel, String remoteClusterLabel) {
        this.sourceClusterLabel = sourceClusterLabel;
        this.remoteClusterLabel = remoteClusterLabel;
    }

    @Override
    public void open(Configuration parameters) {
        log.info("LoopPreventionFilter: source={}, remote={}", sourceClusterLabel, remoteClusterLabel);
    }

    @Override
    public boolean filter(ChangeEvent event) {
        String eventSourceCluster = event.getSourceCluster();

        // If sourceCluster is null/unset, let it through (it's a local change)
        if (eventSourceCluster == null || eventSourceCluster.isEmpty()) {
            return true;
        }

        // Drop events from the remote cluster (they were already synced)
        if (eventSourceCluster.equals(remoteClusterLabel)) {
            log.trace("Dropping remote-origin event: key={}, sourceCluster={}",
                    event.getDocumentKey(), eventSourceCluster);
            return false;
        }

        // Allow local-origin events through
        return true;
    }
}
