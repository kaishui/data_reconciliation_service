# Data Reconciliation Service — Architecture Design

> **Version**: 1.0.0  
> **Stack**: Java 21, Spring Boot 4.1.0-RC1, Apache Flink 1.19, PostgreSQL 16  
> **Author**: Architecture design for bidirectional multi-master data synchronization

---

## 1. Overview

A config-driven, bidirectional data reconciliation system that synchronizes MongoDB clusters (and later PostgreSQL) using Flink CDC for change data capture, with pluggable conflict resolution strategies, a Spring Boot management plane, and full audit logging.

### Core Capabilities

| Capability | Detail |
|---|---|
| **Bidirectional sync** | GCP MongoDB ↔ HIC MongoDB, two independent Flink jobs per direction |
| **CDC engine** | Flink CDC connectors (MongoDB change streams, PostgreSQL WAL decoding) |
| **Loop prevention** | `sourceCluster` field per document; CDC filters foreign-origin events |
| **Conflict resolution** | Sliding window per document key + pluggable strategy (4 built-in) |
| **Config-driven** | REST API: register connections → map collections → pick strategy → deploy |
| **Audit log** | Every conflict logged to PostgreSQL, queryable via API |
| **Multi-DB** | PostgreSQL support via SPI abstraction layer |

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                Reconciliation Management (Spring Boot 4.1)       │
│  ┌──────────┐  ┌────────────┐  ┌──────────┐  ┌──────────────┐  │
│  │ REST API │  │ Job Manager│  │ Conflict │  │ Health/Metrics│  │
│  │ (config) │  │ (deploy)   │  │ Log API  │  │              │  │
│  └──────────┘  └────────────┘  └──────────┘  └──────────────┘  │
│                         │                                       │
│              ┌──────────┴──────────┐                            │
│              │  PostgreSQL 16 (meta)│                            │
│              └─────────────────────┘                            │
└─────────────────────────────────────────────────────────────────┘
                               │
                    deploys / monitors
                               │
┌──────────────────────────────┼──────────────────────────────────┐
│                    Flink Cluster                                 │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  Sync Job: GCP → HIC                                    │   │
│   │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────┐ │   │
│   │  │ Mongo CDC│ → │ Loop     │ → │ Conflict │ → │Mongo │ │   │
│   │  │ Source   │   │ Filter   │   │ Resolver │   │Sink  │ │   │
│   │  │ (GCP)    │   │          │   │+Window   │   │(HIC) │ │   │
│   │  └──────────┘   └──────────┘   └──────────┘   └──────┘ │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  Sync Job: HIC → GCP  (mirror, reverse direction)       │   │
│   └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Flow (Single Direction: GCP → HIC)

```
GCP MongoDB                Flink Job                    HIC MongoDB
    │                         │                              │
    │  change event           │                              │
    │  {_id, fields,          │                              │
    │   sourceCluster:"gcp"}  │                              │
    │ ──────────────────────► │                              │
    │                         │  ┌─ LoopPreventionFilter ─┐  │
    │                         │  │ sourceCluster == hic?  │  │
    │                         │  │ → DROP                 │  │
    │                         │  └────────────────────────┘  │
    │                         │                              │
    │                         │  ┌─ KeyBy(_id) ──────────┐  │
    │                         │  │ WindowedConflictResolv │  │
    │                         │  │   buffer 30s window    │  │
    │                         │  │   onTimer:             │  │
    │                         │  │     read target ver    │  │
    │                         │  │     compare timestamps │  │
    │                         │  │     apply strategy     │  │
    │                         │  └────────────────────────┘  │
    │                         │                              │
    │                         │  ┌─ Side output ─────────┐  │
    │                         │  │ conflict → PostgreSQL  │  │
    │                         │  └────────────────────────┘  │
    │                         │                              │
    │                         │  resolved document           │
    │                         │  sourceCluster = "gcp"       │
    │                         │ ─────────────────────────────►│
```

---

## 4. Loop Prevention

### sourceCluster Semantics

| Scenario | sourceCluster | Action |
|---|---|---|
| User writes to GCP directly | `"gcp"` | GCP CDC emits, syncs to HIC |
| User writes to HIC directly | `"hic"` | HIC CDC emits, syncs to GCP |
| GCP sync writes to HIC | `"gcp"` (preserved) | HIC CDC sees `"gcp"` → **DROP** |
| HIC sync writes to GCP | `"hic"` (preserved) | GCP CDC sees `"hic"` → **DROP** |

### Implementation

- **Sink-side injection**: `SourceClusterInjector` sets `sourceCluster` = source cluster label on every write
- **Source-side filter**: `LoopPreventionFilter` drops any event where `sourceCluster` equals the remote cluster
- **Edge case - user clears field**: MongoDB schema validation can enforce presence of `sourceCluster` as a non-null string

---

## 5. Conflict Resolution Engine

### Strategy SPI

```java
public interface ConflictStrategy {
    String name();
    Map<String, Object> resolve(ConflictContext ctx);
}

public class ConflictContext {
    String documentKey;
    String collectionName;
    Map<String, Object> sourceVersion;    // incoming from CDC
    Map<String, Object> targetVersion;    // current in target DB
    Long sourceTimestamp;
    Long targetTimestamp;
    String sourceCluster;
    Map<String, Object> metadata;
}
```

### Built-in Strategies

| Strategy | Behavior |
|---|---|
| `LAST_WRITE_WINS` | Compare `dataTimestamp`; newest version wins |
| `SOURCE_PRIORITY` | Configured source cluster always wins regardless of timestamp |
| `TARGET_PRIORITY` | Target cluster version always wins |
| `MANUAL` | Log conflict, skip write, queue for human review via API |

### Sliding Window Semantics

- **Problem**: CDC events may arrive out of order. If user updates doc D on GCP at T1 and HIC at T2 (T2 > T1), but the GCP CDC event arrives 5s after HIC's, without a window the older version would overwrite the newer one.
- **Solution**: `WindowedConflictResolver` (a Flink `KeyedProcessFunction`) buffers all events for a given document key within a configurable window (default 30s). When the window expires (timer fires), it drains the buffer, compares timestamps, applies strategy, and emits a single resolved event.
- **State**: Per-key `ValueState<Map<String, List<ChangeEvent>>>` + window timer.

---

## 6. PostgreSQL Extension Point

### SPI Abstraction

```java
public interface CdcSourceFactory {
    boolean supports(DbType dbType);
    DataStream<ChangeEvent> createSource(
        StreamExecutionEnvironment env,
        DbConnectionConfig config,
        CollectionMapping mapping);
}

public interface SinkFactory {
    boolean supports(DbType dbType);
    SinkFunction<ChangeEvent> createSink(
        DbConnectionConfig config,
        CollectionMapping mapping);
}
```

| Concern | MongoDB | PostgreSQL |
|---|---|---|
| Primary key | `_id` (ObjectId/String) | Composite or single-column PK |
| Change stream | `$changeStream` | WAL decoding (`pgoutput` plugin) |
| Schema evolution | Schemaless | DDL handling via schema registry |
| Timestamp field | Any document field | `TIMESTAMP` column |
| sourceCluster | Document field | Dedicated `source_cluster VARCHAR(50)` column |

---

## 7. PostgreSQL Management Schema

```sql
CREATE TABLE db_connections (
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

CREATE TABLE sync_jobs (
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

CREATE TABLE collection_mappings (
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

CREATE TABLE conflict_log (
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

CREATE INDEX idx_sync_jobs_status ON sync_jobs(status);
CREATE INDEX idx_collection_mappings_job ON collection_mappings(sync_job_id);
CREATE INDEX idx_conflict_log_job ON conflict_log(sync_job_id);
CREATE INDEX idx_conflict_log_collection ON conflict_log(collection_name);
CREATE INDEX idx_conflict_log_occurred ON conflict_log(occurred_at DESC);
CREATE INDEX idx_conflict_log_status ON conflict_log(resolution_status);
```

---

## 8. REST API Design

```
POST   /api/v1/connections                  Register DB connection
GET    /api/v1/connections                  List all connections
GET    /api/v1/connections/{id}             Get connection detail
PUT    /api/v1/connections/{id}             Update connection
DELETE /api/v1/connections/{id}             Remove connection

POST   /api/v1/sync-jobs                    Create sync job with mappings
GET    /api/v1/sync-jobs                    List sync jobs
GET    /api/v1/sync-jobs/{id}               Get job detail with mappings
PUT    /api/v1/sync-jobs/{id}               Update job config
DELETE /api/v1/sync-jobs/{id}               Delete job + mappings

POST   /api/v1/sync-jobs/{id}/deploy        Deploy Flink job
POST   /api/v1/sync-jobs/{id}/stop          Stop Flink job (savepoint)
POST   /api/v1/sync-jobs/{id}/restart       Restart from savepoint
GET    /api/v1/sync-jobs/{id}/status        Runtime status + metrics

GET    /api/v1/conflicts                    Query conflict log (paginated, filterable)
GET    /api/v1/conflicts/{id}               Conflict detail
POST   /api/v1/conflicts/{id}/resolve       Manual resolution (MANUAL strategy)
```

### Example: Create Sync Job

```json
POST /api/v1/sync-jobs
{
  "name": "orders-gcp-to-hic",
  "sourceConnectionId": "conn-gcp-001",
  "targetConnectionId": "conn-hic-001",
  "parallelism": 4,
  "checkpointIntervalMs": 10000,
  "windowSizeMs": 30000,
  "mappings": [
    {
      "sourceCollection": "orders",
      "targetCollection": "orders",
      "timestampField": "updatedAt",
      "strategy": "LAST_WRITE_WINS",
      "windowSizeMs": 30000
    },
    {
      "sourceCollection": "customers",
      "targetCollection": "customers",
      "timestampField": "lastModified",
      "strategy": "SOURCE_PRIORITY",
      "strategyParams": { "priorityCluster": "gcp" }
    }
  ]
}
```

---

## 9. Project Structure

```
data-reconciliation-service/
│
├── pom.xml                                  # Parent POM (dependency management)
├── DESIGN.md                                # This document
│
├── reconciliation-common/                   # Shared models, enums, SPI
│   ├── pom.xml
│   └── src/main/java/com/recon/common/
│       ├── model/
│       │   ├── ChangeEvent.java
│       │   ├── ConflictContext.java
│       │   └── ConflictLogEntry.java
│       ├── enums/
│       │   ├── DbType.java
│       │   ├── OperationType.java
│       │   └── ResolutionStatus.java
│       └── spi/
│           ├── ConflictStrategy.java
│           ├── CdcSourceFactory.java
│           └── SinkFactory.java
│
├── reconciliation-management/               # Spring Boot management plane
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/recon/management/
│       │   ├── ReconciliationApplication.java
│       │   ├── controller/
│       │   │   ├── ConnectionController.java
│       │   │   ├── SyncJobController.java
│       │   │   └── ConflictController.java
│       │   ├── service/
│       │   │   ├── ConnectionService.java
│       │   │   ├── SyncJobService.java
│       │   │   ├── FlinkDeployService.java
│       │   │   └── ConflictQueryService.java
│       │   ├── repository/
│       │   │   ├── DbConnectionRepository.java
│       │   │   ├── SyncJobRepository.java
│       │   │   ├── CollectionMappingRepository.java
│       │   │   └── ConflictLogRepository.java
│       │   ├── entity/
│       │   │   ├── DbConnectionEntity.java
│       │   │   ├── SyncJobEntity.java
│       │   │   ├── CollectionMappingEntity.java
│       │   │   └── ConflictLogEntity.java
│       │   ├── dto/
│       │   │   ├── ConnectionRequest.java
│       │   │   ├── SyncJobRequest.java
│       │   │   ├── CollectionMappingRequest.java
│       │   │   ├── ManualResolveRequest.java
│       │   │   └── PagedResponse.java
│       │   └── config/
│       │       ├── FlinkClientConfig.java
│       │       └── JacksonConfig.java
│       └── resources/
│           ├── application.yml
│           └── db/migration/
│               └── V1__init_schema.sql
│
├── reconciliation-engine/                   # Flink CDC job components
│   ├── pom.xml
│   └── src/main/java/com/recon/engine/
│       ├── flink/
│       │   ├── SyncJobBuilder.java
│       │   ├── source/
│       │   │   ├── MongoCdcSourceFactory.java
│       │   │   └── PostgresCdcSourceFactory.java
│       │   ├── sink/
│       │   │   ├── MongoSinkFactory.java
│       │   │   └── PostgresSinkFactory.java
│       │   ├── process/
│       │   │   ├── LoopPreventionFilter.java
│       │   │   ├── WindowedConflictResolver.java
│       │   │   └── SourceClusterInjector.java
│       │   └── serialization/
│       │       └── ChangeEventDeserializer.java
│       └── strategy/
│           ├── LastWriteWinsStrategy.java
│           ├── SourcePriorityStrategy.java
│           ├── TargetPriorityStrategy.java
│           └── ManualStrategy.java
│
└── reconciliation-flink-runner/             # Fat JAR for Flink submission
    ├── pom.xml
    └── src/main/java/com/recon/runner/
        └── SyncJobRunner.java
```

---

## 10. Technology Stack

| Component | Version | Purpose |
|---|---|---|
| Java | 21 | Runtime (virtual threads, pattern matching) |
| Spring Boot | 4.1.0-RC1 | Management plane, REST API, DI |
| Spring Data JPA | 4.1.0-RC1 | PostgreSQL access |
| Spring WebFlux | 4.1.0-RC1 | Reactive REST controllers |
| Flyway | 10.x | Database migrations |
| PostgreSQL | 16 | Management metadata + conflict audit log |
| Apache Flink | 1.19.1 | CDC stream processing engine |
| Flink MongoDB CDC | 3.2.1 | MongoDB change stream connector |
| Flink MongoDB Connector | 1.2.0 | MongoDB sink connector |
| Flink PostgreSQL CDC | 3.2.1 | PostgreSQL WAL connector (future) |
| Lombok | 1.18.36 | Boilerplate reduction |
| Jackson | 2.18.x | JSON serialization |
| Maven | 3.9+ | Build and dependency management |

---

## 11. Implementation Roadmap

### Milestone 1 — Foundation (Week 1-2)
- Spring Boot 4.1 scaffolding with multi-module Maven
- PostgreSQL schema + Flyway migrations
- Connection CRUD API
- Sync job + collection mapping CRUD API

### Milestone 2 — CDC Engine, Single Direction (Week 3-4)
- `ChangeEvent` model + deserialization
- MongoDB CDC source factory
- `LoopPreventionFilter` + `SourceClusterInjector`
- MongoDB sink factory
- Flink job assembly (`SyncJobBuilder`)
- Integration test: GCP → HIC one-way sync

### Milestone 3 — Conflict Resolution (Week 5-6)
- `ConflictStrategy` SPI + 4 strategies
- `WindowedConflictResolver` with state & timers
- Per-collection strategy dispatch
- Conflict audit logging to PostgreSQL (side-output → JDBC sink)

### Milestone 4 — Bidirectional & Operations (Week 7-8)
- Second direction: HIC → GCP
- Flink deployment via REST API
- Savepoint management (stop/restart)
- Conflict query API with pagination & filtering

### Milestone 5 — PostgreSQL CDC Support (Week 9-10)
- `PostgresCdcSourceFactory` + `PostgresSinkFactory`
- DDL handling / schema evolution
- `sourceCluster` column auto-injection

### Milestone 6 — Production Hardening (Week 11-12)
- Metrics: Micrometer + Prometheus + Grafana
- Alerting: lag threshold, conflict rate spike
- Dead-letter queue for failed writes
- TLS for all connections
- API authentication

---

## 12. Key Design Decisions

| Decision | Rationale |
|---|---|
| Two independent Flink jobs (not one centralized) | Fault isolation; GCP→HIC failure doesn't block HIC→GCP |
| `sourceCluster` as loop prevention | Deterministic; no distributed consensus needed |
| Sliding window per document key | Handles out-of-order CDC events without blocking the stream |
| Strategy as SPI interface | New strategies are a JAR drop-in; hot-reloadable |
| Conflict log as PostgreSQL side-output | Decoupled from Flink state; survives restarts; queryable via REST |
| `ChangeEvent` as canonical model | MongoDB docs and PG rows normalized to same event; strategy layer DB-agnostic |
| Flyway for migrations | Repeatable, versioned schema evolution |
| WebFlux (reactive) | Non-blocking I/O for management APIs under load |
