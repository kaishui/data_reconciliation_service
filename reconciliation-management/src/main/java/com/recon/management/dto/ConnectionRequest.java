package com.recon.management.dto;

import com.recon.common.enums.DbType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Map;

public record ConnectionRequest(
        @NotBlank String name,
        @NotNull DbType dbType,
        @NotBlank String connectionString,
        @NotBlank String clusterLabel,
        String username,
        String passwordEncrypted,
        Map<String, Object> properties,
        Boolean enabled
) {}
