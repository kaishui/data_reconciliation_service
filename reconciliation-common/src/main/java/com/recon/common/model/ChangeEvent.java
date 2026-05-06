package com.recon.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.recon.common.enums.OperationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * Canonical change event model — database-agnostic.
 * MongoDB documents and PostgreSQL rows both normalize to this event type,
 * so the strategy layer never touches DB-specific types.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChangeEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Document _id (MongoDB) or primary key (PostgreSQL). */
    private String documentKey;

    /** Collection name (MongoDB) or table name (PostgreSQL). */
    private String collectionName;

    /** Operation type: INSERT, UPDATE, DELETE, REPLACE. */
    private OperationType op;

    /** Pre-image document (null for INSERT). */
    private Map<String, Object> before;

    /** Post-image document (null for DELETE). */
    private Map<String, Object> after;

    /** Cluster where the change originated ("gcp" or "hic"). */
    private String sourceCluster;

    /** Wall-clock time of the CDC event (millis). */
    private Long eventTimestamp;

    /** Value of the user-configured timestamp field (millis). */
    private Long dataTimestamp;

    /** Source cluster operation time (millis). */
    private Long sourceTsMs;
}
