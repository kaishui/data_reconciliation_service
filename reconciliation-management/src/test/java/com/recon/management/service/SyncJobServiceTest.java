package com.recon.management.service;

import com.recon.management.dto.CollectionMappingRequest;
import com.recon.management.dto.SyncJobRequest;
import com.recon.management.entity.DbConnectionEntity;
import com.recon.management.entity.SyncJobEntity;
import com.recon.management.repository.DbConnectionRepository;
import com.recon.management.repository.SyncJobRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("SyncJobService")
class SyncJobServiceTest {

    @Mock private SyncJobRepository syncJobRepository;
    @Mock private DbConnectionRepository connectionRepository;
    @InjectMocks private SyncJobService service;

    @Test
    void shouldCreateSyncJobWithMappings() {
        when(connectionRepository.findById("src-1"))
                .thenReturn(Optional.of(DbConnectionEntity.builder().id("src-1").build()));
        when(connectionRepository.findById("tgt-1"))
                .thenReturn(Optional.of(DbConnectionEntity.builder().id("tgt-1").build()));
        when(syncJobRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        var request = new SyncJobRequest(
                "test-job", "src-1", "tgt-1", 4, 10000L, 30000L, null,
                List.of(new CollectionMappingRequest(
                        "orders", "orders", "updatedAt",
                        "LAST_WRITE_WINS", null, 30000L)));

        var result = service.create(request);

        assertThat(result.getName()).isEqualTo("test-job");
        assertThat(result.getParallelism()).isEqualTo(4);
        assertThat(result.getMappings()).hasSize(1);
        assertThat(result.getMappings().get(0).getSourceCollection()).isEqualTo("orders");
        assertThat(result.getMappings().get(0).getStrategy()).isEqualTo("LAST_WRITE_WINS");
    }

    @Test
    void shouldRejectMissingSourceConnection() {
        when(connectionRepository.findById("src-missing")).thenReturn(Optional.empty());

        var request = new SyncJobRequest(
                "bad-job", "src-missing", "tgt-1", 1, 10000L, 30000L, null, List.of());

        assertThatThrownBy(() -> service.create(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("src-missing");
    }

    @Test
    void shouldRejectMissingTargetConnection() {
        when(connectionRepository.findById("src-1"))
                .thenReturn(Optional.of(DbConnectionEntity.builder().id("src-1").build()));
        when(connectionRepository.findById("tgt-missing")).thenReturn(Optional.empty());

        var request = new SyncJobRequest(
                "bad-job", "src-1", "tgt-missing", 1, 10000L, 30000L, null, List.of());

        assertThatThrownBy(() -> service.create(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tgt-missing");
    }

    @Test
    void shouldRejectDeleteOfRunningJob() {
        var running = SyncJobEntity.builder().id("j1").name("running-job").status("RUNNING").build();
        when(syncJobRepository.findByIdWithMappings("j1")).thenReturn(Optional.of(running));

        assertThatThrownBy(() -> service.delete("j1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Stop it first");
    }

    @Test
    void shouldDeleteStoppedJob() {
        var stopped = SyncJobEntity.builder().id("j1").status("STOPPED").build();
        when(syncJobRepository.findByIdWithMappings("j1")).thenReturn(Optional.of(stopped));

        service.delete("j1");
        verify(syncJobRepository).delete(stopped);
    }

    @Test
    void shouldUpdateJobAndReplaceMappings() {
        var existing = SyncJobEntity.builder()
                .id("j1").name("old").status("STOPPED")
                .sourceConnectionId("src-old").targetConnectionId("tgt-old")
                .parallelism(1).checkpointIntervalMs(5000L).windowSizeMs(15000L)
                .build();
        when(syncJobRepository.findByIdWithMappings("j1")).thenReturn(Optional.of(existing));
        when(connectionRepository.findById("src-new")).thenReturn(Optional.of(DbConnectionEntity.builder().id("src-new").build()));
        when(connectionRepository.findById("tgt-new")).thenReturn(Optional.of(DbConnectionEntity.builder().id("tgt-new").build()));
        when(syncJobRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        var request = new SyncJobRequest(
                "updated", "src-new", "tgt-new", 8, 20000L, 60000L, null,
                List.of(new CollectionMappingRequest("coll", "coll", "ts", "MANUAL", null, 60000L)));

        var result = service.update("j1", request);

        assertThat(result.getName()).isEqualTo("updated");
        assertThat(result.getParallelism()).isEqualTo(8);
        assertThat(result.getMappings()).hasSize(1);
        assertThat(result.getMappings().get(0).getStrategy()).isEqualTo("MANUAL");
    }
}
