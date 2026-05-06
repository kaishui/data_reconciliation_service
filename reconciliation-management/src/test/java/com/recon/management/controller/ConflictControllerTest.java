package com.recon.management.controller;

import com.recon.common.enums.ResolutionStatus;
import com.recon.management.entity.ConflictLogEntity;
import com.recon.management.service.ConflictQueryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.data.domain.PageImpl;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@WebFluxTest(ConflictController.class)
@DisplayName("ConflictController")
class ConflictControllerTest {

    @Autowired private WebTestClient webClient;
    @MockitoBean private ConflictQueryService service;

    @Test
    void shouldSearchConflicts() {
        var entry = ConflictLogEntity.builder()
                .id("cf-1").syncJobId("j1").syncJobName("test-job")
                .collectionName("orders").documentKey("doc1")
                .sourceCluster("gcp").strategyUsed("LAST_WRITE_WINS")
                .sourceVersion(Map.of("v", 1)).targetVersion(Map.of("v", 0))
                .resolutionStatus(ResolutionStatus.AUTO_RESOLVED)
                .build();

        when(service.search(isNull(), isNull(), isNull(), isNull(), isNull(), eq(0), eq(20)))
                .thenReturn(new PageImpl<>(List.of(entry)));

        webClient.get().uri("/api/v1/conflicts?page=0&size=20")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.content[0].id").isEqualTo("cf-1")
                .jsonPath("$.totalElements").isEqualTo(1);
    }

    @Test
    void shouldGetConflict() {
        var entry = ConflictLogEntity.builder()
                .id("cf-1").documentKey("doc1").resolutionStatus(ResolutionStatus.MANUAL_REQUIRED).build();
        when(service.getById("cf-1")).thenReturn(entry);

        webClient.get().uri("/api/v1/conflicts/cf-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.id").isEqualTo("cf-1");
    }
}
