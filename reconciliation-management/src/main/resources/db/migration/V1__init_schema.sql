-- V1: Initial schema for Data Reconciliation Service
-- PostgreSQL 16

CREATE TABLE IF NOT EXISTS db_connections (
    id                VARCHAR(36)  PRIMARY KEY,
    name              VARCHAR(255) NOT NULL UNIQUE,
    db_type           VARCHAR(20)  NOT NULL CHECK (db_type IN ('MONGODB', 'POSTGRESQL')),
    connection_string TEXT         NOT NULL,
    cluster_label     VARCHAR(50)  NOT NULL,
    username          VARCHAR(255),
    password_encrypted TEXT,
    properties        JSONB,
    enabled           BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sync_jobs (
    id                    VARCHAR(36)  PRIMARY KEY,
    name                  VARCHAR(255) NOT NULL UNIQUE,
    source_connection_id  VARCHAR(36)  NOT NULL REFERENCES db_connections(id),
    target_connection_id  VARCHAR(36)  NOT NULL REFERENCES db_connections(id),
    status                VARCHAR(20)  NOT NULL DEFAULT 'STOPPED'
                          CHECK (status IN ('RUNNING', 'STOPPED', 'ERROR', 'PAUSED', 'DEPLOYING')),
    flink_job_id          VARCHAR(255),
    flink_cluster_url     VARCHAR(512),
    parallelism           INTEGER      NOT NULL DEFAULT 1,
    checkpoint_interval_ms BIGINT      NOT NULL DEFAULT 10000,
    window_size_ms        BIGINT       NOT NULL DEFAULT 30000,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS collection_mappings (
    id                 VARCHAR(36)  PRIMARY KEY,
    sync_job_id        VARCHAR(36)  NOT NULL REFERENCES sync_jobs(id) ON DELETE CASCADE,
    source_collection  VARCHAR(255) NOT NULL,
    target_collection  VARCHAR(255) NOT NULL,
    timestamp_field    VARCHAR(255) NOT NULL,
    strategy           VARCHAR(50)  NOT NULL DEFAULT 'LAST_WRITE_WINS'
                       CHECK (strategy IN ('LAST_WRITE_WINS', 'SOURCE_PRIORITY', 'TARGET_PRIORITY', 'MANUAL')),
    strategy_params    JSONB,
    window_size_ms     BIGINT       NOT NULL DEFAULT 30000,
    enabled            BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (sync_job_id, source_collection)
);

CREATE TABLE IF NOT EXISTS conflict_log (
    id                  VARCHAR(36)   PRIMARY KEY,
    sync_job_id         VARCHAR(36)   NOT NULL,
    sync_job_name       VARCHAR(255)  NOT NULL,
    collection_name     VARCHAR(255)  NOT NULL,
    document_key        VARCHAR(512)  NOT NULL,
    source_cluster      VARCHAR(50)   NOT NULL,
    strategy_used       VARCHAR(50)   NOT NULL,
    source_version      JSONB         NOT NULL,
    target_version      JSONB         NOT NULL,
    source_timestamp    BIGINT,
    target_timestamp    BIGINT,
    resolved_version    JSONB,
    resolution_status   VARCHAR(20)   NOT NULL DEFAULT 'MANUAL_REQUIRED'
                        CHECK (resolution_status IN ('AUTO_RESOLVED', 'MANUAL_REQUIRED', 'SKIPPED', 'ERROR')),
    resolution_detail   TEXT,
    occurred_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    resolved_at         TIMESTAMPTZ
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_sync_jobs_status ON sync_jobs(status);
CREATE INDEX IF NOT EXISTS idx_collection_mappings_job ON collection_mappings(sync_job_id);
CREATE INDEX IF NOT EXISTS idx_conflict_log_job ON conflict_log(sync_job_id);
CREATE INDEX IF NOT EXISTS idx_conflict_log_collection ON conflict_log(collection_name);
CREATE INDEX IF NOT EXISTS idx_conflict_log_occurred ON conflict_log(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_conflict_log_status ON conflict_log(resolution_status);
