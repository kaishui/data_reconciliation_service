package com.recon.management.service;

import com.recon.management.dto.ConnectionRequest;
import com.recon.management.entity.DbConnectionEntity;
import com.recon.management.repository.DbConnectionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
@Transactional
public class ConnectionService {

    private final DbConnectionRepository repository;

    public ConnectionService(DbConnectionRepository repository) {
        this.repository = repository;
    }

    public List<DbConnectionEntity> listAll() {
        return repository.findAll();
    }

    public DbConnectionEntity getById(String id) {
        return repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Connection not found: " + id));
    }

    public DbConnectionEntity create(ConnectionRequest request) {
        DbConnectionEntity entity = DbConnectionEntity.builder()
                .id(UUID.randomUUID().toString())
                .name(request.name())
                .dbType(request.dbType())
                .connectionString(request.connectionString())
                .clusterLabel(request.clusterLabel())
                .username(request.username())
                .passwordEncrypted(request.passwordEncrypted())
                .properties(request.properties())
                .enabled(request.enabled() != null ? request.enabled() : true)
                .build();
        return repository.save(entity);
    }

    public DbConnectionEntity update(String id, ConnectionRequest request) {
        DbConnectionEntity entity = getById(id);
        entity.setName(request.name());
        entity.setDbType(request.dbType());
        entity.setConnectionString(request.connectionString());
        entity.setClusterLabel(request.clusterLabel());
        entity.setUsername(request.username());
        entity.setPasswordEncrypted(request.passwordEncrypted());
        entity.setProperties(request.properties());
        if (request.enabled() != null) {
            entity.setEnabled(request.enabled());
        }
        return repository.save(entity);
    }

    public void delete(String id) {
        DbConnectionEntity entity = getById(id);
        repository.delete(entity);
    }
}
