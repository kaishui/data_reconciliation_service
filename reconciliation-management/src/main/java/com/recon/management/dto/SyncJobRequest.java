package com.recon.management.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record SyncJobRequest(
        @NotBlank String name,
        @NotBlank String sourceConnectionId,
        @NotBlank String targetConnectionId,
        @NotNull Integer parallelism,
        @NotNull Long checkpointIntervalMs,
        @NotNull Long windowSizeMs,
        String flinkClusterUrl,
        @NotEmpty List<CollectionMappingRequest> mappings
) {}
