package com.recon.management.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "recon.flink")
public class FlinkClientConfig {

    /**
     * Flink cluster REST API base URL.
     * Example: http://flink-jobmanager:8081
     */
    private String restUrl = "http://localhost:8081";

    /**
     * Timeout in seconds for Flink API calls.
     */
    private int timeoutSeconds = 30;

    /**
     * Directory where Flink job JARs are stored.
     */
    private String jarDirectory = "/opt/flink/jars";

    public String getRestUrl() {
        return restUrl;
    }

    public void setRestUrl(String restUrl) {
        this.restUrl = restUrl;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public String getJarDirectory() {
        return jarDirectory;
    }

    public void setJarDirectory(String jarDirectory) {
        this.jarDirectory = jarDirectory;
    }
}
