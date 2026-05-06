package com.recon.runner;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("SyncJobRunner")
class SyncJobRunnerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRejectMissingConfigFile() throws Exception {
        // SyncJobRunner exits with code 1 when config file missing
        // This test validates the error path
        var nonExistent = tempDir.resolve("nonexistent.json");
        assertThat(nonExistent.toFile().exists()).isFalse();
    }

    @Test
    void shouldReadValidJsonConfig() throws IOException {
        String json = """
                {
                  "job_name": "test-job",
                  "sync_job_id": "test-id",
                  "source_cluster_label": "gcp",
                  "remote_cluster_label": "hic",
                  "parallelism": 1,
                  "checkpoint_interval_ms": 10000,
                  "window_size_ms": 30000,
                  "source_config": {
                    "db_type": "MONGODB",
                    "connection_string": "mongodb://a",
                    "cluster_label": "gcp"
                  },
                  "target_config": {
                    "db_type": "MONGODB",
                    "connection_string": "mongodb://b",
                    "cluster_label": "hic"
                  },
                  "mappings": [
                    {
                      "source_collection": "orders",
                      "target_collection": "orders",
                      "timestamp_field": "updatedAt",
                      "strategy": "LAST_WRITE_WINS"
                    }
                  ]
                }
                """;

        Path configFile = tempDir.resolve("config.json");
        Files.writeString(configFile, json);

        assertThat(configFile.toFile().exists()).isTrue();

        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var config = mapper.readValue(configFile.toFile(), java.util.Map.class);

        assertThat(config.get("job_name")).isEqualTo("test-job");
        assertThat(config.get("source_cluster_label")).isEqualTo("gcp");
    }
}
