package com.recon.management.dto;

import java.util.List;

public record PagedResponse<T>(
        List<T> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {
    public static <T> PagedResponse<T> of(List<T> content, int page, int size, long totalElements) {
        return new PagedResponse<>(
                content,
                page,
                size,
                totalElements,
                (int) Math.ceil((double) totalElements / size)
        );
    }
}
