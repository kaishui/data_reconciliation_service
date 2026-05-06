package com.recon.management.controller;

import com.recon.management.dto.CollectionMappingRequest;
import com.recon.management.dto.SyncJobRequest;
import com.recon.management.entity.CollectionMappingEntity;
import com.recon.management.entity.SyncJobEntity;
import com.recon.management.service.FlinkDeployService;
import com.recon.management.service.SyncJobService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@WebFluxTest(SyncJobController.class)
@DisplayName("SyncJobController")
class SyncJobControllerTest {

    @Autowired private WebTestClient webClient;
    @MockitoBean private SyncJobService syncJobService;
    @MockitoBean private FlinkDeployService flinkDeployService;

    @Test
    void shouldListSyncJobs() {
        var job = SyncJobEntity.builder().id("j1").name("test-job").status("STOPPED").build();
        when(syncJobService.listAll()).thenReturn(List.of(job));

        webClient.get().uri("/api/v1/sync-jobs")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].name").isEqualTo("test-job");
    }

    @Test
    void shouldCreateSyncJob() {
        var request = new SyncJobRequest("new-job", "src-1", "tgt-1", 4, 10000L, 30000L, null,
                List.of(new CollectionMappingRequest("orders", "orders", "ts", "LAST_WRITE_WINS", null, 30000L)));
        var mapping = CollectionMappingEntity.builder().id("m1").sourceCollection("orders").strategy("LAST_WRITE_WINS").build();
        var job = SyncJobEntity.builder().id("j1").name("new-job").mappings(List.of(mapping)).build();

        when(syncJobService.create(any())).thenReturn(job);

        webClient.post().uri("/api/v1/sync-jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.id").isEqualTo("j1");
    }

    @Test
    void shouldDeployJob() {
        var job = SyncJobEntity.builder().id("j1").status("RUNNING").build();
        when(flinkDeployService.deploy("j1")).thenReturn(job);

        webClient.post().uri("/api/v1/sync-jobs/j1/deploy")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("RUNNING");
    }

    @Test
    void shouldStopJob() {
        var job = SyncJobEntity.builder().id("j1").status("STOPPED").build();
        when(flinkDeployService.stop("j1")).thenReturn(job);

        webClient.post().uri("/api/v1/sync-jobs/j1/stop")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("STOPPED");
    }

    @Test
    void shouldDeleteJob() {
        webClient.delete().uri("/api/v1/sync-jobs/j1")
                .exchange()
                .expectStatus().isNoContent();
    }
}
