package com.recon.management.controller;

import com.recon.common.enums.DbType;
import com.recon.management.dto.ConnectionRequest;
import com.recon.management.entity.DbConnectionEntity;
import com.recon.management.service.ConnectionService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@WebFluxTest(ConnectionController.class)
@DisplayName("ConnectionController")
class ConnectionControllerTest {

    @Autowired private WebTestClient webClient;
    @MockitoBean private ConnectionService service;

    @Test
    void shouldListConnections() {
        var conn = DbConnectionEntity.builder().id("c1").name("gcp-prod")
                .dbType(DbType.MONGODB).clusterLabel("gcp").build();
        when(service.listAll()).thenReturn(List.of(conn));

        webClient.get().uri("/api/v1/connections")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].name").isEqualTo("gcp-prod")
                .jsonPath("$[0].id").isEqualTo("c1");
    }

    @Test
    void shouldGetConnection() {
        var conn = DbConnectionEntity.builder().id("c1").name("test")
                .dbType(DbType.MONGODB).clusterLabel("gcp").build();
        when(service.getById("c1")).thenReturn(conn);

        webClient.get().uri("/api/v1/connections/c1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.name").isEqualTo("test");
    }

    @Test
    void shouldCreateConnection() {
        var request = new ConnectionRequest("new-conn", DbType.MONGODB,
                "mongodb://h", "gcp", null, null, null, true);
        var entity = DbConnectionEntity.builder().id("new-id").name("new-conn")
                .dbType(DbType.MONGODB).clusterLabel("gcp").build();
        when(service.create(any())).thenReturn(entity);

        webClient.post().uri("/api/v1/connections")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.id").isEqualTo("new-id");
    }

    @Test
    void shouldUpdateConnection() {
        var request = new ConnectionRequest("updated", DbType.POSTGRESQL,
                "jdbc://h", "hic", null, null, null, false);
        var entity = DbConnectionEntity.builder().id("c1").name("updated")
                .dbType(DbType.POSTGRESQL).clusterLabel("hic").build();
        when(service.update(any(), any())).thenReturn(entity);

        webClient.put().uri("/api/v1/connections/c1")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.name").isEqualTo("updated");
    }

    @Test
    void shouldDeleteConnection() {
        webClient.delete().uri("/api/v1/connections/c1")
                .exchange()
                .expectStatus().isNoContent();
    }

    @Test
    void shouldReturn400OnValidationError() {
        var invalid = new ConnectionRequest("", null, "", "",
                null, null, null, null);

        webClient.post().uri("/api/v1/connections")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(invalid)
                .exchange()
                .expectStatus().isBadRequest();
    }
}
