package com.recon.management.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Map;

public record CollectionMappingRequest(
        @NotBlank String sourceCollection,
        @NotBlank String targetCollection,
        @NotBlank String timestampField,
        @NotBlank String strategy,
        Map<String, Object> strategyParams,
        @NotNull Long windowSizeMs
) {}
