package com.recon.management.cucumber;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.List;
import java.util.Map;

/**
 * Step definitions for Sync Job Management features.
 */
public class SyncJobStepDefinitions {

    private final WebTestClient webClient;
    private WebTestClient.ResponseSpec lastResponse;
    private Map<String, Object> requestBody;

    public SyncJobStepDefinitions() {
        this.webClient = WebTestClient.bindToServer().baseUrl("http://localhost:8080").build();
    }

    @Given("a sync job request with name {string} and {int} mappings")
    public void syncJobRequestWithNameAndMappings(String name, int mappingCount) {
        requestBody = Map.of(
                "name", name,
                "source_connection_id", "src-gcp",
                "target_connection_id", "tgt-hic",
                "parallelism", 1,
                "checkpoint_interval_ms", 10000,
                "window_size_ms", 30000,
                "mappings", List.of(
                        Map.of(
                                "source_collection", "orders",
                                "target_collection", "orders",
                                "timestamp_field", "updatedAt",
                                "strategy", "LAST_WRITE_WINS",
                                "window_size_ms", 30000
                        ),
                        Map.of(
                                "source_collection", "customers",
                                "target_collection", "customers",
                                "timestamp_field", "lastModified",
                                "strategy", "SOURCE_PRIORITY",
                                "strategy_params", Map.of("priorityCluster", "gcp"),
                                "window_size_ms", 60000
                        )
                )
        );
    }

    @Given("a sync job request referencing source {string}")
    public void syncJobRequestReferencingSource(String sourceId) {
        requestBody = Map.of(
                "name", "test-job",
                "source_connection_id", sourceId,
                "target_connection_id", "tgt-hic",
                "parallelism", 1,
                "checkpoint_interval_ms", 10000,
                "window_size_ms", 30000,
                "mappings", List.of()
        );
    }

    @Given("a sync job with id {string} and status {string}")
    public void syncJobWithIdAndStatus(String id, String status) {
        // Pre-condition — in integration test, create via repository
    }

    @When("I POST the sync job to {string}")
    public void postSyncJob(String path) {
        lastResponse = webClient.post().uri(path)
                .bodyValue(requestBody != null ? requestBody : Map.of())
                .exchange();
    }

    @When("I POST to {string}")
    public void postTo(String path) {
        lastResponse = webClient.post().uri(path)
                .bodyValue(Map.of())
                .exchange();
    }

    @When("I GET {string}")
    public void get(String path) {
        lastResponse = webClient.get().uri(path).exchange();
    }

    @When("I DELETE {string}")
    public void delete(String path) {
        lastResponse = webClient.delete().uri(path).exchange();
    }

    @Then("the response status is {int} {string}")
    public void responseStatusIs(int code, String ignored) {
        lastResponse.expectStatus().isEqualTo(code);
    }

    @Then("the response status is {int}")
    public void responseStatusIs(int code) {
        lastResponse.expectStatus().isEqualTo(code);
    }

    @Then("the response status is {int} or {int}")
    public void responseStatusIsEither(int code1, int code2) {
        try {
            lastResponse.expectStatus().isEqualTo(code1);
        } catch (AssertionError e) {
            lastResponse.expectStatus().isEqualTo(code2);
        }
    }

    @Then("the response contains {string} equal to {string}")
    public void responseContainsFieldEqualTo(String field, String value) {
        lastResponse.expectBody()
                .jsonPath("$." + field).isEqualTo(value);
    }

    @Then("the response contains {string} array of length {int}")
    public void responseContainsArrayOfLength(String field, int length) {
        lastResponse.expectBody()
                .jsonPath("$." + field + ".length()").isEqualTo(length);
    }
}
