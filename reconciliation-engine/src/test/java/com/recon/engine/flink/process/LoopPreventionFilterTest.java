package com.recon.engine.flink.process;

import com.recon.common.enums.OperationType;
import com.recon.common.model.ChangeEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("LoopPreventionFilter")
class LoopPreventionFilterTest {

    private final LoopPreventionFilter filter = new LoopPreventionFilter("gcp", "hic");

    @Test
    void shouldPassLocalOriginEvent() throws Exception {
        var event = ChangeEvent.builder()
                .documentKey("doc1")
                .sourceCluster("gcp")
                .op(OperationType.UPDATE)
                .build();

        assertThat(filter.filter(event)).isTrue();
    }

    @Test
    void shouldPassEventWithNullSourceCluster() throws Exception {
        var event = ChangeEvent.builder()
                .documentKey("doc1")
                .op(OperationType.INSERT)
                .build();

        assertThat(filter.filter(event)).isTrue();
    }

    @Test
    void shouldPassEventWithEmptySourceCluster() throws Exception {
        var event = ChangeEvent.builder()
                .documentKey("doc1")
                .sourceCluster("")
                .op(OperationType.UPDATE)
                .build();

        assertThat(filter.filter(event)).isTrue();
    }

    @Test
    void shouldDropRemoteOriginEvent() throws Exception {
        var event = ChangeEvent.builder()
                .documentKey("doc1")
                .sourceCluster("hic")
                .op(OperationType.UPDATE)
                .build();

        assertThat(filter.filter(event)).isFalse();
    }

    @Test
    void shouldDropRemoteOriginEventCaseSensitively() throws Exception {
        var event = ChangeEvent.builder()
                .documentKey("doc1")
                .sourceCluster("hic")
                .op(OperationType.UPDATE)
                .build();

        assertThat(filter.filter(event)).isFalse();

        // "HIC" (uppercase) would NOT match "hic" — passes through
        var upperEvent = ChangeEvent.builder()
                .documentKey("doc2")
                .sourceCluster("HIC")
                .op(OperationType.UPDATE)
                .build();

        assertThat(filter.filter(upperEvent)).isTrue();
    }

    @Test
    void shouldPreserveReversedDirectionLogic() throws Exception {
        // HIC→GCP filter: drops gcp-origin events
        var reversedFilter = new LoopPreventionFilter("hic", "gcp");

        var gcpEvent = ChangeEvent.builder()
                .sourceCluster("gcp")
                .build();
        assertThat(reversedFilter.filter(gcpEvent)).isFalse();

        var hicEvent = ChangeEvent.builder()
                .sourceCluster("hic")
                .build();
        assertThat(reversedFilter.filter(hicEvent)).isTrue();
    }
}
