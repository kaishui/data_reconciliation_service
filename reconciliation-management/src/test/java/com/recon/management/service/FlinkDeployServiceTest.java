package com.recon.management.service;

import com.recon.management.config.FlinkClientConfig;
import com.recon.management.entity.SyncJobEntity;
import com.recon.management.repository.DbConnectionRepository;
import com.recon.management.repository.SyncJobRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("FlinkDeployService")
class FlinkDeployServiceTest {

    @Mock private SyncJobRepository repository;
    @Mock private DbConnectionRepository connectionRepository;
    @Mock private FlinkRestClient flinkClient;
    @Mock private FlinkClientConfig config;
    @InjectMocks private FlinkDeployService service;

    @Test
    void shouldRejectDeployOfRunningJob() {
        var job = SyncJobEntity.builder().id("j1").status("RUNNING").build();
        when(repository.findById("j1")).thenReturn(Optional.of(job));

        assertThatThrownBy(() -> service.deploy("j1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already running or deploying");
    }

    @Test
    void shouldRejectStopOfNonRunningJob() {
        var job = SyncJobEntity.builder().id("j1").status("STOPPED").build();
        when(repository.findById("j1")).thenReturn(Optional.of(job));

        assertThatThrownBy(() -> service.stop("j1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not running");
    }

    @Test
    void shouldRejectRestartOfRunningJob() {
        var job = SyncJobEntity.builder().id("j1").status("RUNNING").build();
        when(repository.findById("j1")).thenReturn(Optional.of(job));

        assertThatThrownBy(() -> service.restart("j1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must be STOPPED");
    }

    @Test
    void shouldRejectStopWithNoFlinkJobId() {
        var job = SyncJobEntity.builder().id("j1").status("RUNNING").build();
        when(repository.findById("j1")).thenReturn(Optional.of(job));

        assertThatThrownBy(() -> service.stop("j1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No Flink job ID");
    }

    @Test
    void shouldReturnNotDeployedStatus() {
        var job = SyncJobEntity.builder().id("j1").status("STOPPED").build();
        when(repository.findById("j1")).thenReturn(Optional.of(job));

        var result = service.getRuntimeStatus("j1");
        assertThat(result).containsEntry("status", "STOPPED");
        assertThat(result).containsEntry("detail", "Not deployed");
    }
}
