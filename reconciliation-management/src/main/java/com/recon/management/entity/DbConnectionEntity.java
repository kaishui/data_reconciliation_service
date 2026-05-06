package com.recon.management.entity;

import com.recon.common.enums.DbType;
import io.hypersistence.utils.hibernate.type.json.JsonBinaryType;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;

import java.time.Instant;
import java.util.Map;

@Entity
@Table(name = "db_connections")
public class DbConnectionEntity {

    @Id
    @Column(length = 36)
    private String id;

    @Column(nullable = false, unique = true, length = 255)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(name = "db_type", nullable = false, length = 20)
    private DbType dbType;

    @Column(name = "connection_string", nullable = false, columnDefinition = "TEXT")
    private String connectionString;

    @Column(name = "cluster_label", nullable = false, length = 50)
    private String clusterLabel;

    @Column(length = 255)
    private String username;

    @Column(name = "password_encrypted", columnDefinition = "TEXT")
    private String passwordEncrypted;

    @Type(JsonBinaryType.class)
    @Column(columnDefinition = "jsonb")
    private Map<String, Object> properties;

    @Column(nullable = false)
    private boolean enabled = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public DbConnectionEntity() {}

    public DbConnectionEntity(String id, String name, DbType dbType, String connectionString,
                               String clusterLabel, String username, String passwordEncrypted,
                               Map<String, Object> properties, boolean enabled,
                               Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.name = name;
        this.dbType = dbType;
        this.connectionString = connectionString;
        this.clusterLabel = clusterLabel;
        this.username = username;
        this.passwordEncrypted = passwordEncrypted;
        this.properties = properties;
        this.enabled = enabled;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    @PrePersist
    void prePersist() {
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    @PreUpdate
    void preUpdate() {
        this.updatedAt = Instant.now();
    }

    // --- Getters and Setters ---

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public DbType getDbType() { return dbType; }
    public void setDbType(DbType dbType) { this.dbType = dbType; }

    public String getConnectionString() { return connectionString; }
    public void setConnectionString(String connectionString) { this.connectionString = connectionString; }

    public String getClusterLabel() { return clusterLabel; }
    public void setClusterLabel(String clusterLabel) { this.clusterLabel = clusterLabel; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPasswordEncrypted() { return passwordEncrypted; }
    public void setPasswordEncrypted(String passwordEncrypted) { this.passwordEncrypted = passwordEncrypted; }

    public Map<String, Object> getProperties() { return properties; }
    public void setProperties(Map<String, Object> properties) { this.properties = properties; }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    // --- Builder ---

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String id;
        private String name;
        private DbType dbType;
        private String connectionString;
        private String clusterLabel;
        private String username;
        private String passwordEncrypted;
        private Map<String, Object> properties;
        private boolean enabled = true;
        private Instant createdAt;
        private Instant updatedAt;

        public Builder id(String id) { this.id = id; return this; }
        public Builder name(String name) { this.name = name; return this; }
        public Builder dbType(DbType dbType) { this.dbType = dbType; return this; }
        public Builder connectionString(String s) { this.connectionString = s; return this; }
        public Builder clusterLabel(String s) { this.clusterLabel = s; return this; }
        public Builder username(String s) { this.username = s; return this; }
        public Builder passwordEncrypted(String s) { this.passwordEncrypted = s; return this; }
        public Builder properties(Map<String, Object> p) { this.properties = p; return this; }
        public Builder enabled(boolean e) { this.enabled = e; return this; }
        public Builder createdAt(Instant i) { this.createdAt = i; return this; }
        public Builder updatedAt(Instant i) { this.updatedAt = i; return this; }

        public DbConnectionEntity build() {
            return new DbConnectionEntity(id, name, dbType, connectionString, clusterLabel,
                    username, passwordEncrypted, properties, enabled, createdAt, updatedAt);
        }
    }
}
