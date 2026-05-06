package com.recon.engine.flink.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recon.common.enums.OperationType;
import com.recon.common.model.ChangeEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ChangeEventDeserializer")
class ChangeEventDeserializerTest {

    private final ChangeEventDeserializer deserializer = new ChangeEventDeserializer();
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void shouldDeserializeInsertEvent() throws Exception {
        String json = mapper.writeValueAsString(Map.of(
                "documentKey", Map.of("_id", "abc-123"),
                "ns", Map.of("coll", "orders"),
                "operationType", "insert",
                "fullDocument", Map.of("_id", "abc-123", "status", "new", "sourceCluster", "gcp"),
                "clusterTime", Map.of("$timestamp", 1700000000000L)
        ));

        ChangeEvent event = deserializer.deserialize(json.getBytes());

        assertThat(event.getDocumentKey()).isEqualTo("abc-123");
        assertThat(event.getCollectionName()).isEqualTo("orders");
        assertThat(event.getOp()).isEqualTo(OperationType.INSERT);
        assertThat(event.getAfter()).containsEntry("status", "new");
        assertThat(event.getSourceCluster()).isEqualTo("gcp");
    }

    @Test
    void shouldDeserializeUpdateEvent() throws Exception {
        String json = mapper.writeValueAsString(Map.of(
                "documentKey", Map.of("_id", "xyz-789"),
                "operationType", "update",
                "fullDocument", Map.of("_id", "xyz-789", "value", 42)
        ));

        ChangeEvent event = deserializer.deserialize(json.getBytes());

        assertThat(event.getOp()).isEqualTo(OperationType.UPDATE);
        assertThat(event.getAfter()).containsEntry("value", 42);
    }

    @Test
    void shouldDeserializeDeleteEvent() throws Exception {
        String json = mapper.writeValueAsString(Map.of(
                "documentKey", Map.of("_id", "del-1"),
                "operationType", "delete"
        ));

        ChangeEvent event = deserializer.deserialize(json.getBytes());

        assertThat(event.getOp()).isEqualTo(OperationType.DELETE);
        assertThat(event.getAfter()).isNull();
    }

    @Test
    void shouldDeserializeReplaceEvent() throws Exception {
        String json = mapper.writeValueAsString(Map.of(
                "documentKey", Map.of("_id", "rep-1"),
                "operationType", "replace",
                "fullDocument", Map.of("_id", "rep-1", "field", "replaced")
        ));

        ChangeEvent event = deserializer.deserialize(json.getBytes());

        assertThat(event.getOp()).isEqualTo(OperationType.REPLACE);
    }

    @Test
    void shouldHandleNullOperationType() throws Exception {
        String json = mapper.writeValueAsString(Map.of(
                "documentKey", Map.of("_id", "nop-1")
        ));

        ChangeEvent event = deserializer.deserialize(json.getBytes());
        assertThat(event.getOp()).isEqualTo(OperationType.UPDATE);
    }

    @Test
    void shouldHandleUnknownOperationType() throws Exception {
        String json = mapper.writeValueAsString(Map.of(
                "operationType", "invalidOp"
        ));

        ChangeEvent event = deserializer.deserialize(json.getBytes());
        assertThat(event.getOp()).isEqualTo(OperationType.UPDATE);
    }

    @Test
    void shouldNotBeEndOfStream() {
        assertThat(deserializer.isEndOfStream(null)).isFalse();
    }

    @Test
    void shouldHandleNullDocumentKey() throws Exception {
        String json = mapper.writeValueAsString(Map.of(
                "operationType", "update"
        ));

        ChangeEvent event = deserializer.deserialize(json.getBytes());
        assertThat(event.getDocumentKey()).isNull();
    }
}
