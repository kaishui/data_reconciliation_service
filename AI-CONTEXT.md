# Data Reconciliation Service — AI Context

> Load this file into any AI assistant to instantly understand the project.
> Covers: architecture, module map, conventions, build commands, key files, and common tasks.

---

## 1. Project Identity

**What**: Bidirectional multi-master data reconciliation system using Flink CDC.
**Why**: Sync MongoDB clusters (GCP ↔ HIC) with eventual consistency, using `sourceCluster` field for loop prevention and a sliding-window conflict resolver with pluggable strategies.
**Stack**: Java 21 · Spring Boot 3.4.6 · Apache Flink 1.19.1 · PostgreSQL 16 / H2 · Maven 3.8+

---

## 2. Architecture (30-second mental model)

```
Spring Boot Management Plane (REST API)
    │  CRUD: connections, sync-jobs, collection-mappings, conflicts
    │  Deploys Flink jobs via REST API
    ▼
PostgreSQL 16 (config + conflict audit log)
    │
    ▼
Flink Cluster
    ├── Job: GCP → HIC   [MongoDB CDC Source] → [LoopFilter] → [WindowedResolver] → [Mongo Sink]
    └── Job: HIC → GCP   (mirror, reverse direction)
```

**Loop prevention**: `sourceCluster` field set on sink, filtered on source. Local changes pass through; remote-origin changes are dropped.

**Conflict resolution**: `WindowedConflictResolver` (KeyedProcessFunction) buffers events per document key within a configurable window. On timer expiry, picks the winner by `dataTimestamp` and applies the per-collection strategy (LAST_WRITE_WINS, SOURCE_PRIORITY, TARGET_PRIORITY, MANUAL).

**Audit log**: Conflicts side-output to PostgreSQL `conflict_log` table via JDBC sink.

---

## 3. Module Map

| Module | Purpose | # Java files | Dependencies |
|---|---|---|---|
| `reconciliation-common` | Shared models, enums, SPI | 9 main + 6 test | Jackson, Flink (provided) |
| `reconciliation-management` | Spring Boot REST API + JPA | 19 main + 13 test + 3 cucumber | Spring WebFlux, Data JPA, Flyway, PostgreSQL, H2 |
| `reconciliation-engine` | Flink CDC pipeline + strategies | 14 main + 8 test | Flink Streaming, Flink CDC (provided), Flink MongoDB (provided) |
| `reconciliation-flink-runner` | Fat JAR entrypoint | 1 main + 1 test | All Flink/Mongo deps (compile scope, shaded) |

---

## 4. Build & Run (quick reference)

```bash
# Prerequisites
export JAVA_HOME=/Users/kaishui/workspace/path/jdk-21.0.1.jdk/Contents/Home

# Full build (skip tests for speed)
mvn clean install -DskipTests

# Build + unit tests
mvn clean test

# Build + Cucumber BDD
mvn clean verify -pl reconciliation-management

# Run management service (H2 in-memory, no PostgreSQL needed)
cd reconciliation-management
mvn spring-boot:run -Dspring-boot.run.profiles=h2
# → http://localhost:8080
# → H2 console: http://localhost:8080/h2-console

# Run individual module tests
mvn test -pl reconciliation-common
mvn test -pl reconciliation-engine

# Build Flink fat JAR (for deployment)
mvn clean package -pl reconciliation-flink-runner -am -DskipTests
# Output: reconciliation-flink-runner/target/reconciliation-flink-runner-1.0.0-SNAPSHOT.jar
# Main class: com.recon.runner.SyncJobRunner

# Submit to local Flink cluster
flink run -c com.recon.runner.SyncJobRunner \
  reconciliation-flink-runner/target/reconciliation-flink-runner-1.0.0-SNAPSHOT.jar \
  --config /path/to/job-config.json
```

---

## 5. Key Files by Role

### Configuration
| File | Role |
|---|---|
| `pom.xml` | Parent POM — version management for ALL dependencies |
| `reconciliation-management/src/main/resources/application.yml` | PostgreSQL config |
| `reconciliation-management/src/main/resources/application-h2.yml` | H2 profile (local dev) |
| `reconciliation-management/src/main/resources/db/migration/V1__init_schema.sql` | Flyway migration (PostgreSQL) |
| `reconciliation-management/src/main/resources/schema-h2.sql` | H2 DDL (local dev) |

### Core Domain Model (reconciliation-common)
| File | Role |
|---|---|
| `model/ChangeEvent.java` | Canonical CDC event — DB-agnostic, used across all modules |
| `model/ConflictContext.java` | Input to ConflictStrategy.resolve() |
| `model/ConflictLogEntry.java` | Audit log entry — serialized to PostgreSQL `conflict_log` |
| `spi/ConflictStrategy.java` | Strategy interface — `name()` + `resolve(ConflictContext)` |
| `spi/CdcSourceFactory.java` | CDC source SPI — `supports(DbType)` + `createSource(...)` |
| `spi/SinkFactory.java` | Sink SPI — `supports(DbType)` + `createSink(...)` |

### Management API (reconciliation-management)
| File | Role |
|---|---|
| `entity/DbConnectionEntity.java` | DB connection config (manual builder, no Lombok) |
| `entity/SyncJobEntity.java` | Sync job with OneToMany CollectionMappingEntity |
| `entity/CollectionMappingEntity.java` | Per-collection mapping with strategy + timestamp field |
| `entity/ConflictLogEntry.java` | Conflict audit log — JSONB via hypersistence-utils |
| `service/ConnectionService.java` | CRUD for connections |
| `service/SyncJobService.java` | CRUD for sync jobs + mappings, validates connection references |
| `service/FlinkDeployService.java` | Deploy/stop/restart Flink jobs (stub — updates DB status) |
| `service/ConflictQueryService.java` | Search + manual resolve conflicts |
| `controller/ConnectionController.java` | REST: `POST/GET/PUT/DELETE /api/v1/connections` |
| `controller/SyncJobController.java` | REST: sync-jobs + deploy/stop/restart |
| `controller/ConflictController.java` | REST: conflict search + resolve |

### Flink Engine (reconciliation-engine)
| File | Role |
|---|---|
| `flink/SyncJobBuilder.java` | Assembles the full pipeline: source → filter → keyBy → resolver → sink + conflict log |
| `flink/process/LoopPreventionFilter.java` | Drops events where `sourceCluster == remoteCluster` |
| `flink/process/WindowedConflictResolver.java` | KeyedProcessFunction: buffers events, fires on timer, resolves conflicts |
| `flink/process/SourceClusterInjector.java` | Sets `sourceCluster` on BSON Document before MongoDB write |
| `flink/source/MongoCdcSourceFactory.java` | MongoDB CDC source via Flink CDC connector |
| `flink/source/PostgresCdcSourceFactory.java` | PostgreSQL CDC source (stub — throws UnsupportedOperationException) |
| `flink/sink/MongoSinkFactory.java` | Placeholder sink (provided scope — real impl uses MongoSink API) |
| `flink/sink/PostgresSinkFactory.java` | PostgreSQL sink (stub) |
| `strategy/LastWriteWinsStrategy.java` | Compares timestamps, newest wins (equal → source wins) |
| `strategy/SourcePriorityStrategy.java` | Source cluster wins if matches `priorityCluster` in metadata |
| `strategy/TargetPriorityStrategy.java` | Target cluster always wins |
| `strategy/ManualStrategy.java` | Returns null → skip write, log for human review |

### Tests
| File | What it covers |
|---|---|
| `ChangeEventTest.java` | Serialization, null safety, builder |
| `LastWriteWinsStrategyTest.java` | Nested: timestamp comparison + null handling |
| `LoopPreventionFilterTest.java` | Local/remote/null/empty sourceCluster, reversed direction |
| `WindowedConflictResolverTest.java` | Winner selection by timestamp, null-after skip, strategy resolution |
| `ConnectionServiceTest.java` | Mocked CRUD operations |
| `ConnectionControllerTest.java` | WebTestClient-based REST endpoint testing |
| `features/connection-management.feature` | 8 Gherkin scenarios |
| `features/sync-job-management.feature` | 8 Gherkin scenarios |
| `features/conflict-resolution.feature` | 6 Gherkin scenarios |

---

## 6. Design Decisions & Conventions

1. **No Lombok in management entities** — Manual getters/setters/builders to avoid Java 21 annotation processing issues with Spring Boot 3.4.6. Common module still uses Lombok.
2. **DTOs are Java records** — `ConnectionRequest`, `SyncJobRequest`, etc. are all `record` types.
3. **JSONB via hypersistence-utils** — `@Type(JsonBinaryType.class)` for PostgreSQL JSONB columns.
4. **Flink dependencies are `provided`** in engine module — resolved at runtime by the Flink cluster or fat JAR.
5. **MongoSinkFactory is a placeholder** — The Flink MongoDB connector API changed between versions. Real implementation uses `MongoSink.<T>builder().setUri(...)`.
6. **H2 for local dev** — `application-h2.yml` profile disables Flyway, uses H2 with PostgreSQL compatibility mode, and loads `schema-h2.sql` via `spring.sql.init`.
7. **Maven repo workaround** — The default `~/.m2/repository` is not writable in this environment. Use `-Dmaven.repo.local=/tmp/m2-recon` or set up a writable path.

---

## 7. Common Tasks (prompt templates)

### Add a new conflict strategy
```
Create a new strategy class in reconciliation-engine/strategy/ implementing ConflictStrategy.
Register it in SyncJobBuilder.buildStrategyRegistry().
Add unit test following LastWriteWinsStrategyTest pattern.
```

### Add a new REST endpoint
```
Add a route in the appropriate controller under reconciliation-management/controller/.
Follow the existing pattern: @RestController, constructor injection, validate with @Valid.
Add a WebTestClient-based test in the test controller package.
```

### Add PostgreSQL CDC support
```
Implement PostgresCdcSourceFactory and PostgresSinkFactory (currently stubs).
Add integration test with Testcontainers.
Update SyncJobBuilder to handle DbType.POSTGRESQL sink.
```

### Fix a compilation error
```
Ensure JAVA_HOME points to JDK 21.
Run mvn compile -pl <module> to isolate the error.
Check POM for dependency scope (provided vs compile).
Common issues: Lombok not generating code (use manual getters), Flink CDC group changed to org.apache.flink.
```

### Deploy Flink JAR
```
# 1. Build the fat JAR (shade plugin packages all deps into one JAR)
mvn clean package -pl reconciliation-flink-runner -am -DskipTests

# Output: reconciliation-flink-runner/target/reconciliation-flink-runner-1.0.0-SNAPSHOT.jar
# Main class: com.recon.runner.SyncJobRunner

# 2a. Submit to Flink cluster via CLI
flink run \
  -c com.recon.runner.SyncJobRunner \
  -p 4 \
  reconciliation-flink-runner-1.0.0-SNAPSHOT.jar \
  --config /path/to/job-config.json

# 2b. Submit to Flink cluster via REST API
# Step 1: Upload JAR
curl -X POST http://localhost:8081/jars/upload \
  -H "Expect:" \
  -F "jarfile=@reconciliation-flink-runner-1.0.0-SNAPSHOT.jar"
# Returns: {"filename":"/tmp/flink-web-.../flink-web-upload/UUID.jar"}

# Step 2: Run the uploaded JAR with job config
curl -X POST http://localhost:8081/jars/<jar-id>/run \
  -H "Content-Type: application/json" \
  -d '{
    "entryClass": "com.recon.runner.SyncJobRunner",
    "parallelism": 4,
    "programArgs": "--config /opt/flink/jars/job-config.json"
  }'
# Returns: {"jobid": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}

# 3. Manage running jobs via REST API
# List jobs:    GET  http://localhost:8081/jobs
# Job status:   GET  http://localhost:8081/jobs/<job-id>
# Stop + savepoint: POST http://localhost:8081/jobs/<job-id>/stop  {"targetDirectory":"/savepoints/"}
# Cancel job:   PATCH http://localhost:8081/jobs/<job-id>?mode=cancel

# 4. Job config JSON format (passed to SyncJobRunner)
# See SyncJobRunner.printUsage() for full schema:
{
  "job_name": "orders-gcp-to-hic",
  "sync_job_id": "job-uuid",
  "source_cluster_label": "gcp",
  "remote_cluster_label": "hic",
  "parallelism": 4,
  "checkpoint_interval_ms": 10000,
  "window_size_ms": 30000,
  "source_config": {
    "db_type": "MONGODB",
    "connection_string": "mongodb://gcp-host:27017",
    "database": "mydb",
    "cluster_label": "gcp"
  },
  "target_config": {
    "db_type": "MONGODB",
    "connection_string": "mongodb://hic-host:27017",
    "database": "mydb",
    "cluster_label": "hic"
  },
  "mappings": [{
    "source_collection": "orders",
    "target_collection": "orders",
    "timestamp_field": "updatedAt",
    "strategy": "LAST_WRITE_WINS",
    "window_size_ms": 30000
  }],
  "audit_db_url": "jdbc:postgresql://audit-host:5432/reconciliation",
  "audit_db_user": "recon",
  "audit_db_password": "secret"
}

# 5. Management API (Spring Boot) deployment endpoints
# These currently update DB status only (FlinkDeployService is a stub)
POST /api/v1/sync-jobs/{id}/deploy    → sets status=RUNNING
POST /api/v1/sync-jobs/{id}/stop      → sets status=STOPPED
POST /api/v1/sync-jobs/{id}/restart   → sets status=RUNNING

# TODO: FlinkDeployService needs real Flink REST API integration
# - Generate job-config.json from SyncJobEntity + DbConnectionEntity
# - Upload fat JAR to FLINK_REST_URL/jars/upload
# - Submit job via FLINK_REST_URL/jars/{id}/run
# - Store returned flinkJobId in SyncJobEntity
# - Stop: trigger savepoint then cancel
# - Restart: resubmit from last savepoint
```

---

## 8. Environment Variables

| Variable | Default | Where used |
|---|---|---|
| `DB_PASSWORD` | `recon123` | PostgreSQL connection |
| `FLINK_REST_URL` | `http://localhost:8081` | Flink job deployment |
| `FLINK_JAR_DIR` | `/opt/flink/jars` | Flink JAR storage |

---

## 9. File Extension Guide

| Extension | What |
|---|---|
| `.java` | Java source (main or test) |
| `.xml` | Maven POM |
| `.yml` | Spring Boot config |
| `.sql` | Database migration / schema |
| `.feature` | Cucumber Gherkin scenario |
| `.http` | IntelliJ/VSCode REST Client requests |
| `.md` | Documentation |

---

## 10. Quick Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `cannot find symbol: method getXxx()` | Lombok not processing | Entity classes in management module use manual getters |
| `flink-connector-mongodb-cdc` not found | Group changed from `com.ververica` to `org.apache.flink` | Check POM group ID |
| `BindableType not found` | Hibernate 6.x annotation processing | Use manual enum handling or explicit hibernate-core dep |
| `JsonProcessingException` | Jackson `writeValueAsString` | Wrap in try-catch or use `toJson()` helper |
| PostgreSQL connection refused | No PostgreSQL running | Use `-Dspring-boot.run.profiles=h2` |
| Maven repo write error | `~/.m2` not writable | Use `-Dmaven.repo.local=/tmp/m2-recon` |
