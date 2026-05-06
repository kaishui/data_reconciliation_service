package com.recon.management.dto;

import jakarta.validation.constraints.NotBlank;

import java.util.Map;

public record ManualResolveRequest(
        @NotBlank String conflictId,
        Map<String, Object> resolvedVersion,
        String resolutionDetail
) {}
