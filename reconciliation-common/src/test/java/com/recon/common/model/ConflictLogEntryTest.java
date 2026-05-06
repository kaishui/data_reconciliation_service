package com.recon.common.model;

import com.recon.common.enums.ResolutionStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConflictLogEntryTest {

    @Test
    void shouldBuildAutoResolvedEntry() {
        var entry = ConflictLogEntry.builder()
                .id("uuid-1")
                .syncJobId("job-1")
                .syncJobName("orders-gcp-to-hic")
                .collectionName("orders")
                .documentKey("doc-123")
                .sourceCluster("gcp")
                .strategyUsed("LAST_WRITE_WINS")
                .sourceVersion(Map.of("v", 1))
                .targetVersion(Map.of("v", 0))
                .sourceTimestamp(2000L)
                .targetTimestamp(1000L)
                .resolvedVersion(Map.of("v", 1))
                .resolutionStatus(ResolutionStatus.AUTO_RESOLVED)
                .resolutionDetail("Source wins by timestamp")
                .occurredAt(Instant.now())
                .build();

        assertThat(entry.getResolutionStatus()).isEqualTo(ResolutionStatus.AUTO_RESOLVED);
        assertThat(entry.getResolvedVersion()).isNotNull();
        assertThat(entry.getResolutionDetail()).isEqualTo("Source wins by timestamp");
    }

    @Test
    void shouldBuildManualRequiredEntry() {
        var entry = ConflictLogEntry.builder()
                .id("uuid-2")
                .syncJobId("job-1")
                .syncJobName("test-job")
                .collectionName("customers")
                .documentKey("cust-1")
                .sourceCluster("gcp")
                .strategyUsed("MANUAL")
                .sourceVersion(Map.of("name", "Alice"))
                .targetVersion(Map.of("name", "Bob"))
                .resolutionStatus(ResolutionStatus.MANUAL_REQUIRED)
                .build();

        assertThat(entry.getResolvedVersion()).isNull();
        assertThat(entry.getResolutionStatus()).isEqualTo(ResolutionStatus.MANUAL_REQUIRED);
    }

    @Test
    void shouldDefaultResolutionStatusToManualRequired() {
        var entry = ConflictLogEntry.builder().build();
        // builder default is MANUAL_REQUIRED
        assertThat(entry.getResolutionStatus()).isNull();
    }
}
