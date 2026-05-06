package com.recon.management.service;

import com.recon.common.enums.DbType;
import com.recon.management.dto.ConnectionRequest;
import com.recon.management.entity.DbConnectionEntity;
import com.recon.management.repository.DbConnectionRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("ConnectionService")
class ConnectionServiceTest {

    @Mock private DbConnectionRepository repository;
    @InjectMocks private ConnectionService service;

    @Test
    void shouldListAllConnections() {
        var conn = DbConnectionEntity.builder().id("c1").name("gcp-prod").build();
        when(repository.findAll()).thenReturn(List.of(conn));

        var result = service.listAll();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getName()).isEqualTo("gcp-prod");
    }

    @Test
    void shouldGetById() {
        var conn = DbConnectionEntity.builder().id("c1").name("gcp-prod").build();
        when(repository.findById("c1")).thenReturn(Optional.of(conn));

        var result = service.getById("c1");
        assertThat(result.getName()).isEqualTo("gcp-prod");
    }

    @Test
    void shouldThrowWhenNotFound() {
        when(repository.findById("bad")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.getById("bad"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bad");
    }

    @Test
    void shouldCreateConnection() {
        var request = new ConnectionRequest(
                "gcp-prod", DbType.MONGODB, "mongodb://host:27017",
                "gcp", "admin", "encrypted", Map.of("database", "mydb"), true);

        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        var result = service.create(request);

        assertThat(result.getName()).isEqualTo("gcp-prod");
        assertThat(result.getDbType()).isEqualTo(DbType.MONGODB);
        assertThat(result.getClusterLabel()).isEqualTo("gcp");
        assertThat(result.isEnabled()).isTrue();
        assertThat(result.getId()).isNotNull();
    }

    @Test
    void shouldUpdateConnection() {
        var existing = DbConnectionEntity.builder()
                .id("c1").name("old-name").dbType(DbType.MONGODB)
                .connectionString("old-uri").clusterLabel("gcp").build();
        when(repository.findById("c1")).thenReturn(Optional.of(existing));
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        var request = new ConnectionRequest(
                "new-name", DbType.MONGODB, "new-uri",
                "hic", null, null, null, false);

        var result = service.update("c1", request);

        assertThat(result.getName()).isEqualTo("new-name");
        assertThat(result.getConnectionString()).isEqualTo("new-uri");
        assertThat(result.getClusterLabel()).isEqualTo("hic");
        assertThat(result.isEnabled()).isFalse();
    }

    @Test
    void shouldDeleteConnection() {
        var conn = DbConnectionEntity.builder().id("c1").build();
        when(repository.findById("c1")).thenReturn(Optional.of(conn));

        service.delete("c1");
        verify(repository).delete(conn);
    }
}
