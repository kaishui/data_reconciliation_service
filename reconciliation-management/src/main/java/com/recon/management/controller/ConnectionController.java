package com.recon.management.controller;

import com.recon.management.dto.ConnectionRequest;
import com.recon.management.entity.DbConnectionEntity;
import com.recon.management.service.ConnectionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/connections")
public class ConnectionController {

    private final ConnectionService service;

    public ConnectionController(ConnectionService service) {
        this.service = service;
    }

    @GetMapping
    public List<DbConnectionEntity> list() {
        return service.listAll();
    }

    @GetMapping("/{id}")
    public DbConnectionEntity get(@PathVariable String id) {
        return service.getById(id);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public DbConnectionEntity create(@Valid @RequestBody ConnectionRequest request) {
        return service.create(request);
    }

    @PutMapping("/{id}")
    public DbConnectionEntity update(@PathVariable String id, @Valid @RequestBody ConnectionRequest request) {
        return service.update(id, request);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String id) {
        service.delete(id);
    }
}
