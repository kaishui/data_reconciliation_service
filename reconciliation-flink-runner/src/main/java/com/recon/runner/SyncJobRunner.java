package com.recon.runner;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import com.recon.engine.flink.SyncJobBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * CLI entrypoint for Flink job submission.
 *
 * Reads job configuration from a JSON file and builds/submits the Flink CDC
 * sync job.
 *
 * Usage:
 * java -jar reconciliation-flink-runner.jar job-config.json
 *
 * Or submit to a Flink cluster:
 * flink run -c com.recon.runner.SyncJobRunner reconciliation-flink-runner.jar
 * --config job-config.json
 */
public class SyncJobRunner {

  private static final Logger log = LoggerFactory.getLogger(SyncJobRunner.class);

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      System.exit(1);
    }

    String configPath = args[0];

    // Support --config flag
    if ("--config".equals(configPath) && args.length > 1) {
      configPath = args[1];
    }

    File configFile = new File(configPath);
    if (!configFile.exists()) {
      log.error("Config file not found: {}", configPath);
      System.exit(1);
    }

    ObjectMapper mapper = JsonMapper.builder().build();
    @SuppressWarnings("unchecked")
    Map<String, Object> jobConfig = mapper.readValue(configFile, Map.class);

    log.info("Starting sync job: {}", jobConfig.get("job_name"));
    log.info("Source cluster: {} → Target cluster: {}",
        jobConfig.get("source_cluster_label"), jobConfig.get("remote_cluster_label"));

    SyncJobBuilder builder = new SyncJobBuilder(jobConfig);
    builder.buildAndExecute();
  }

  private static void printUsage() {
    System.out.println("""
        Data Reconciliation Flink Runner

        Usage:
          java -jar reconciliation-flink-runner.jar <config-file.json>

        Or on a Flink cluster:
          flink run -c com.recon.runner.SyncJobRunner reconciliation-flink-runner.jar \\
                    --config job-config.json

        Config file format (JSON):
        {
          "job_name": "orders-gcp-to-hic",
          "sync_job_id": "job-uuid-here",
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
          "mappings": [
            {
              "source_collection": "orders",
              "target_collection": "orders",
              "timestamp_field": "updatedAt",
              "strategy": "LAST_WRITE_WINS",
              "window_size_ms": 30000
            }
          ],
          "audit_db_url": "jdbc:postgresql://audit-host:5432/reconciliation",
          "audit_db_user": "recon",
          "audit_db_password": "secret"
        }
        """);
  }
}
