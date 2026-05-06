package com.recon.management.controller;

import com.recon.management.dto.SyncJobRequest;
import com.recon.management.entity.SyncJobEntity;
import com.recon.management.service.FlinkDeployService;
import com.recon.management.service.SyncJobService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/sync-jobs")
public class SyncJobController {

    private final SyncJobService syncJobService;
    private final FlinkDeployService flinkDeployService;

    public SyncJobController(SyncJobService syncJobService, FlinkDeployService flinkDeployService) {
        this.syncJobService = syncJobService;
        this.flinkDeployService = flinkDeployService;
    }

    @GetMapping
    public List<SyncJobEntity> list() { return syncJobService.listAll(); }

    @GetMapping("/{id}")
    public SyncJobEntity get(@PathVariable String id) { return syncJobService.getById(id); }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public SyncJobEntity create(@Valid @RequestBody SyncJobRequest req) { return syncJobService.create(req); }

    @PutMapping("/{id}")
    public SyncJobEntity update(@PathVariable String id, @Valid @RequestBody SyncJobRequest req) { return syncJobService.update(id, req); }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String id) { syncJobService.delete(id); }

    @PostMapping("/{id}/deploy")
    public SyncJobEntity deploy(@PathVariable String id) { return flinkDeployService.deploy(id); }

    @PostMapping("/{id}/stop")
    public SyncJobEntity stop(@PathVariable String id) { return flinkDeployService.stop(id); }

    @PostMapping("/{id}/restart")
    public SyncJobEntity restart(@PathVariable String id) { return flinkDeployService.restart(id); }

    @GetMapping("/{id}/status")
    public Map<String, Object> status(@PathVariable String id) { return flinkDeployService.getRuntimeStatus(id); }
}
