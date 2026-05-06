package com.recon.engine.flink.process;

import com.recon.common.model.ChangeEvent;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Injects the sourceCluster field into every document before writing to target.
 * Sets sourceCluster to the source cluster label so that the reverse sync job
 * can identify and drop these events.
 */
public class SourceClusterInjector {

    private static final Logger log = LoggerFactory.getLogger(SourceClusterInjector.class);

    private final String sourceClusterLabel;

    public SourceClusterInjector(String sourceClusterLabel) {
        this.sourceClusterLabel = sourceClusterLabel;
    }

    /**
     * Inject sourceCluster into a BSON Document before upsert.
     */
    public Document inject(Document doc, ChangeEvent event) {
        if (doc == null) return null;
        doc.put("sourceCluster", sourceClusterLabel);
        log.trace("Injected sourceCluster={} into doc key={}",
                sourceClusterLabel, event != null ? event.getDocumentKey() : "unknown");
        return doc;
    }

    /**
     * Inject sourceCluster into a Map-based document.
     */
    public java.util.Map<String, Object> injectMap(java.util.Map<String, Object> doc) {
        if (doc == null) return null;
        doc.put("sourceCluster", sourceClusterLabel);
        return doc;
    }

    public String getSourceClusterLabel() {
        return sourceClusterLabel;
    }
}
