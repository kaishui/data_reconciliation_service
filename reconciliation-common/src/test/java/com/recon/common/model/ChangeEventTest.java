package com.recon.common.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recon.common.enums.OperationType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeEventTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void shouldSerializeAndDeserializeViaJackson() throws Exception {
        var event = ChangeEvent.builder()
                .documentKey("abc123")
                .collectionName("orders")
                .op(OperationType.UPDATE)
                .after(Map.of("status", "shipped"))
                .sourceCluster("gcp")
                .eventTimestamp(1700000000000L)
                .dataTimestamp(1699999999000L)
                .build();

        String json = mapper.writeValueAsString(event);
        ChangeEvent deserialized = mapper.readValue(json, ChangeEvent.class);

        assertThat(deserialized.getDocumentKey()).isEqualTo("abc123");
        assertThat(deserialized.getCollectionName()).isEqualTo("orders");
        assertThat(deserialized.getOp()).isEqualTo(OperationType.UPDATE);
        assertThat(deserialized.getAfter()).containsEntry("status", "shipped");
        assertThat(deserialized.getSourceCluster()).isEqualTo("gcp");
    }

    @Test
    void shouldBuildInsertEventWithNullBefore() {
        var event = ChangeEvent.builder()
                .documentKey("doc1")
                .op(OperationType.INSERT)
                .after(Map.of("name", "test"))
                .build();

        assertThat(event.getBefore()).isNull();
        assertThat(event.getAfter()).isNotNull();
    }

    @Test
    void shouldBuildDeleteEventWithNullAfter() {
        var event = ChangeEvent.builder()
                .documentKey("doc1")
                .op(OperationType.DELETE)
                .before(Map.of("name", "test"))
                .build();

        assertThat(event.getAfter()).isNull();
        assertThat(event.getBefore()).isNotNull();
    }

    @Test
    void shouldHandleNullDocumentKey() {
        var event = ChangeEvent.builder()
                .op(OperationType.UPDATE)
                .build();

        assertThat(event.getDocumentKey()).isNull();
    }

    @Test
    void shouldDefaultTimestampsToNull() {
        var event = ChangeEvent.builder().build();
        assertThat(event.getEventTimestamp()).isNull();
        assertThat(event.getDataTimestamp()).isNull();
        assertThat(event.getSourceTsMs()).isNull();
    }

    @Test
    void shouldSupportBuilderToBuilderMutation() {
        var original = ChangeEvent.builder()
                .documentKey("key1")
                .sourceCluster("gcp")
                .build();

        var modified = original.toBuilder()
                .sourceCluster("hic")
                .build();

        assertThat(modified.getDocumentKey()).isEqualTo("key1");
        assertThat(modified.getSourceCluster()).isEqualTo("hic");
        // Original unchanged
        assertThat(original.getSourceCluster()).isEqualTo("gcp");
    }
}
