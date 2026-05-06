package com.recon.engine.flink.serialization;

import tools.jackson.databind.ObjectMapper;
import com.recon.common.enums.OperationType;
import com.recon.common.model.ChangeEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Deserializes JSON bytes into ChangeEvent.
 * Used as a bridge between MongoDB CDC JSON output and the canonical ChangeEvent model.
 */
public class ChangeEventDeserializer implements DeserializationSchema<ChangeEvent> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    @Override
    public ChangeEvent deserialize(byte[] message) throws IOException {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> raw = mapper.readValue(message, Map.class);

        ChangeEvent.ChangeEventBuilder builder = ChangeEvent.builder();

        // MongoDB CDC format: "documentKey", "fullDocument", "operationType", etc.
        if (raw.containsKey("documentKey")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> docKey = (Map<String, Object>) raw.get("documentKey");
            Object id = docKey != null ? docKey.get("_id") : null;
            builder.documentKey(id != null ? id.toString() : null);
        }

        if (raw.containsKey("ns")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> ns = (Map<String, Object>) raw.get("ns");
            builder.collectionName(ns != null ? (String) ns.get("coll") : null);
        }

        // Operation type mapping
        String opType = (String) raw.get("operationType");
        builder.op(mapOperationType(opType));

        // Full document (after)
        if (raw.containsKey("fullDocument")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> fullDoc = (Map<String, Object>) raw.get("fullDocument");
            builder.after(fullDoc);
            if (fullDoc != null && fullDoc.containsKey("sourceCluster")) {
                builder.sourceCluster((String) fullDoc.get("sourceCluster"));
            }
        }

        // Pre-image (before) — MongoDB 6.0+ with changeStreamPreAndPostImages
        if (raw.containsKey("fullDocumentBeforeChange")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> beforeDoc = (Map<String, Object>) raw.get("fullDocumentBeforeChange");
            builder.before(beforeDoc);
        }

        // Timestamps
        if (raw.containsKey("clusterTime")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> clusterTime = (Map<String, Object>) raw.get("clusterTime");
            if (clusterTime != null && clusterTime.containsKey("$timestamp")) {
                builder.eventTimestamp((Long) clusterTime.get("$timestamp"));
            }
        }

        return builder.build();
    }

    @Override
    public boolean isEndOfStream(ChangeEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ChangeEvent> getProducedType() {
        return TypeInformation.of(ChangeEvent.class);
    }

    private OperationType mapOperationType(String opType) {
        if (opType == null) return OperationType.UPDATE;
        return switch (opType) {
            case "insert" -> OperationType.INSERT;
            case "update" -> OperationType.UPDATE;
            case "replace" -> OperationType.REPLACE;
            case "delete" -> OperationType.DELETE;
            default -> OperationType.UPDATE;
        };
    }
}
