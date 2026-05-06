package com.recon.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.recon.common.enums.ResolutionStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

/**
 * Audit log entry for a conflict resolution decision.
 * Serialized to PostgreSQL conflict_log table.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConflictLogEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String syncJobId;
    private String syncJobName;
    private String collectionName;
    private String documentKey;
    private String sourceCluster;
    private String strategyUsed;

    /** Full document from the incoming CDC event. */
    private Map<String, Object> sourceVersion;

    /** Full document currently in the target database. */
    private Map<String, Object> targetVersion;

    private Long sourceTimestamp;
    private Long targetTimestamp;

    /** Winning document after strategy applied (null if MANUAL_REQUIRED). */
    private Map<String, Object> resolvedVersion;

    private ResolutionStatus resolutionStatus;
    private String resolutionDetail;

    private Instant occurredAt;
    private Instant resolvedAt;
}
