package com.recon.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Context passed to a ConflictStrategy when a conflict is detected.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConflictContext {

    /** Document _id or primary key. */
    private String documentKey;

    /** Collection or table name. */
    private String collectionName;

    /** Incoming CDC event's post-image. */
    private Map<String, Object> sourceVersion;

    /** Current version in the target database. */
    private Map<String, Object> targetVersion;

    /** Timestamp from the source event (configured field). */
    private Long sourceTimestamp;

    /** Timestamp from the target document (configured field). */
    private Long targetTimestamp;

    /** Which cluster the source event came from. */
    private String sourceCluster;

    /** Strategy-specific parameters (e.g., priorityCluster for SOURCE_PRIORITY). */
    private Map<String, Object> metadata;
}
