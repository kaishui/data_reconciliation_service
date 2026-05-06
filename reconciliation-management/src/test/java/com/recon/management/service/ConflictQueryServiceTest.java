package com.recon.management.service;

import com.recon.common.enums.ResolutionStatus;
import com.recon.management.dto.ManualResolveRequest;
import com.recon.management.entity.ConflictLogEntity;
import com.recon.management.repository.ConflictLogRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("ConflictQueryService")
class ConflictQueryServiceTest {

    @Mock private ConflictLogRepository repository;
    @InjectMocks private ConflictQueryService service;

    @Test
    void shouldSearchConflicts() {
        var entry = ConflictLogEntity.builder()
                .id("cf-1").syncJobId("j1").collectionName("orders")
                .documentKey("doc1").sourceCluster("gcp").strategyUsed("LAST_WRITE_WINS")
                .sourceVersion(Map.of("v", 1)).targetVersion(Map.of("v", 0))
                .resolutionStatus(ResolutionStatus.AUTO_RESOLVED)
                .build();
        Page<ConflictLogEntity> page = new PageImpl<>(List.of(entry));

        when(repository.search(any(), any(), any(), any(), any(), any()))
                .thenReturn(page);

        var result = service.search("j1", "orders", ResolutionStatus.AUTO_RESOLVED,
                null, null, 0, 20);

        assertThat(result.getContent()).hasSize(1);
        assertThat(result.getContent().get(0).getDocumentKey()).isEqualTo("doc1");
    }

    @Test
    void shouldGetById() {
        var entry = ConflictLogEntity.builder().id("cf-1").build();
        when(repository.findById("cf-1")).thenReturn(Optional.of(entry));

        var result = service.getById("cf-1");
        assertThat(result.getId()).isEqualTo("cf-1");
    }

    @Test
    void shouldThrowWhenConflictNotFound() {
        when(repository.findById("bad")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.getById("bad"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldResolveManualConflict() {
        var entry = ConflictLogEntity.builder()
                .id("cf-1").resolutionStatus(ResolutionStatus.MANUAL_REQUIRED).build();
        when(repository.findById("cf-1")).thenReturn(Optional.of(entry));
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        var request = new ManualResolveRequest(
                "cf-1", Map.of("resolved", true), "Admin override");

        var result = service.resolve("cf-1", request);

        assertThat(result.getResolutionStatus()).isEqualTo(ResolutionStatus.AUTO_RESOLVED);
        assertThat(result.getResolvedVersion()).containsEntry("resolved", true);
        assertThat(result.getResolutionDetail()).isEqualTo("Admin override");
    }

    @Test
    void shouldRejectResolveOfNonManualConflict() {
        var entry = ConflictLogEntity.builder()
                .id("cf-1").resolutionStatus(ResolutionStatus.AUTO_RESOLVED).build();
        when(repository.findById("cf-1")).thenReturn(Optional.of(entry));

        var request = new ManualResolveRequest("cf-1", Map.of(), "test");

        assertThatThrownBy(() -> service.resolve("cf-1", request))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("MANUAL_REQUIRED");
    }
}
