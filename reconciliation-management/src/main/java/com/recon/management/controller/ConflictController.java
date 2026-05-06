package com.recon.management.controller;

import com.recon.common.enums.ResolutionStatus;
import com.recon.management.dto.ManualResolveRequest;
import com.recon.management.dto.PagedResponse;
import com.recon.management.entity.ConflictLogEntity;
import com.recon.management.service.ConflictQueryService;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;

@RestController
@RequestMapping("/api/v1/conflicts")
public class ConflictController {

    private final ConflictQueryService service;

    public ConflictController(ConflictQueryService service) {
        this.service = service;
    }

    @GetMapping
    public PagedResponse<ConflictLogEntity> search(
            @RequestParam(required = false) String syncJobId,
            @RequestParam(required = false) String collectionName,
            @RequestParam(required = false) ResolutionStatus status,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        Page<ConflictLogEntity> result = service.search(syncJobId, collectionName, status, from, to, page, size);
        return PagedResponse.of(result.getContent(), page, size, result.getTotalElements());
    }

    @GetMapping("/{id}")
    public ConflictLogEntity get(@PathVariable String id) {
        return service.getById(id);
    }

    @PostMapping("/{id}/resolve")
    public ConflictLogEntity resolve(@PathVariable String id, @Valid @RequestBody ManualResolveRequest request) {
        return service.resolve(id, request);
    }
}
