# Data Reconciliation Service

> Bidirectional multi-master data reconciliation with Flink CDC and Spring Boot  
> **Stack**: Java 21 · Spring Boot 3.4.6 · Apache Flink 1.19 · PostgreSQL 16 / H2

---

## Overview

A config-driven system that synchronizes MongoDB clusters (GCP ↔ HIC) using Flink CDC for change data capture, with pluggable conflict resolution strategies and full audit logging. PostgreSQL support via SPI abstraction.

**Core features:**
- Bidirectional CDC sync with deterministic loop prevention (`sourceCluster`)
- Sliding-window conflict resolution with 4 strategies (LAST_WRITE_WINS, SOURCE_PRIORITY, TARGET_PRIORITY, MANUAL)
- REST API for connection/sync-job/conflict management
- Conflict audit log stored in PostgreSQL
- PostgreSQL CDC support via SPI extension point

---

## Quick Start

### Prerequisites
- Java 21 (`/Users/kaishui/workspace/path/jdk-21.0.1.jdk`)
- Maven 3.8+
- (Optional) PostgreSQL 16 for production mode

### 1. Build
```bash
export JAVA_HOME=/Users/kaishui/workspace/path/jdk-21.0.1.jdk/Contents/Home
mvn clean install -DskipTests
```

### 2. Run (H2 in-memory — no PostgreSQL needed)
```bash
cd reconciliation-management
mvn spring-boot:run -Dspring-boot.run.profiles=h2
```
App starts at **http://localhost:8080**.  
H2 console at **http://localhost:8080/h2-console** (JDBC URL: `jdbc:h2:mem:reconciliation`).

### 3. Run (PostgreSQL production mode)
```bash
# First create the database:
# createdb -h localhost -U recon reconciliation

cd reconciliation-management
mvn spring-boot:run
```
Flyway auto-migrates the schema on startup.

### 4. Run tests
```bash
# Unit tests (all modules)
mvn test

# Cucumber BDD (management module only)
mvn verify -pl reconciliation-management

# Full build with tests
mvn clean verify
```

### 5. HTTP API testing
Open `api-test.http` in IntelliJ or VSCode (REST Client extension).  
Contains 20+ pre-configured requests for all endpoints.

---

## Project Structure

```
data-reconciliation-service/
├── DESIGN.md                       # Full architecture document
├── README.md                       # This file
├── api-test.http                   # HTTP request collection
├── pom.xml                         # Parent POM (dependency management)
│
├── reconciliation-common/          # Shared models, enums, SPI
│   ├── enums/   DbType, OperationType, ResolutionStatus
│   ├── model/   ChangeEvent, ConflictContext, ConflictLogEntry
│   └── spi/     ConflictStrategy, CdcSourceFactory, SinkFactory
│
├── reconciliation-management/      # Spring Boot REST API
│   ├── entity/       JPA entities
│   ├── dto/          Request/Response records
│   ├── repository/   Spring Data repos
│   ├── service/      Business logic
│   ├── controller/   REST controllers
│   └── resources/
│       ├── application.yml         # PostgreSQL config
│       ├── application-h2.yml      # H2 profile
│       ├── schema-h2.sql           # H2 DDL
│       └── db/migration/           # Flyway (PostgreSQL)
│
├── reconciliation-engine/          # Flink CDC pipeline
│   ├── flink/
│   │   ├── source/   MongoCdcSourceFactory, PostgresCdcSourceFactory
│   │   ├── sink/     MongoSinkFactory, PostgresSinkFactory
│   │   ├── process/  LoopPreventionFilter, WindowedConflictResolver,
│   │   │             SourceClusterInjector
│   │   └── SyncJobBuilder
│   └── strategy/     LastWriteWins, SourcePriority, TargetPriority, Manual
│
└── reconciliation-flink-runner/    # Fat JAR for Flink submission
    └── SyncJobRunner
```

---

## REST API

### Connections
```
POST   /api/v1/connections              Register DB connection
GET    /api/v1/connections              List all
GET    /api/v1/connections/{id}         Get by ID
PUT    /api/v1/connections/{id}         Update
DELETE /api/v1/connections/{id}         Delete
```

### Sync Jobs
```
POST   /api/v1/sync-jobs                Create job with mappings
GET    /api/v1/sync-jobs                List all
GET    /api/v1/sync-jobs/{id}           Get detail
PUT    /api/v1/sync-jobs/{id}           Update
DELETE /api/v1/sync-jobs/{id}           Delete (stopped only)
POST   /api/v1/sync-jobs/{id}/deploy    Deploy to Flink
POST   /api/v1/sync-jobs/{id}/stop      Stop with savepoint
POST   /api/v1/sync-jobs/{id}/restart   Restart from savepoint
```

### Conflicts
```
GET    /api/v1/conflicts                Search (paginated, filterable)
GET    /api/v1/conflicts/{id}           Detail
POST   /api/v1/conflicts/{id}/resolve   Manual resolution
```

### Health
```
GET    /actuator/health
GET    /actuator/prometheus
```

---

## Conflict Resolution Strategies

| Strategy | Behavior |
|---|---|
| `LAST_WRITE_WINS` | Compare timestamp field; newest wins |
| `SOURCE_PRIORITY` | Configured source cluster wins |
| `TARGET_PRIORITY` | Target cluster wins |
| `MANUAL` | Log conflict, skip write, human review |

Strategies are assigned per-collection in the sync job mapping.

---

## Architecture

```
Management Plane (Spring Boot) → PostgreSQL (config/audit)
         │
    deploys / monitors
         │
Flink Cluster
  ├── Sync Job: GCP → HIC  (MongoDB CDC → Filter → Resolver → Sink)
  └── Sync Job: HIC → GCP  (mirror)
```

Loop prevention: `sourceCluster` field injected on sink, filtered on source.  
Conflict resolution: sliding window per document key, strategy applied on window expiry.  
Audit log: conflicts side-output to PostgreSQL via JDBC sink.

---

## Test Coverage

- **Unit tests**: 30+ test files across all modules (JUnit 5 + AssertJ)
- **Cucumber BDD**: 21 scenarios across 3 feature files (REST API integration)
- **WebTestClient**: Controller-level integration tests
- **Flink test harness**: Process function unit tests

Run: `mvn test` (unit) | `mvn verify` (unit + Cucumber)

---

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `DB_PASSWORD` | `recon123` | PostgreSQL password |
| `FLINK_REST_URL` | `http://localhost:8081` | Flink JobManager URL |
| `FLINK_JAR_DIR` | `/opt/flink/jars` | Flink JAR directory |

---

## Implementation Milestones

1. ✅ Foundation — Spring Boot scaffolding, POM, entities, REST API
2. ✅ CDC Engine — MongoDB source/sink, filters, serialization
3. ✅ Conflict Resolution — 4 strategies, windowed resolver
4. ✅ Bidirectional + Operations — deploy/stop/restart, conflict query
5. 🔲 PostgreSQL CDC — PostgresCdcSource/Sink (stubbed)
6. 🔲 Production Hardening — metrics, alerting, TLS, auth

See `DESIGN.md` for full architecture details.
