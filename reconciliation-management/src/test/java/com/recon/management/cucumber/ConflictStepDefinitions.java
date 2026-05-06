package com.recon.management.cucumber;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.Map;

/**
 * Step definitions for Conflict Resolution features.
 */
public class ConflictStepDefinitions {

    private final WebTestClient webClient;
    private WebTestClient.ResponseSpec lastResponse;

    public ConflictStepDefinitions() {
        this.webClient = WebTestClient.bindToServer().baseUrl("http://localhost:8080").build();
    }

    @Given("a conflict entry exists for sync job {string} with status {string}")
    public void conflictEntryExists(String jobId, String status) {
        // Pre-condition — in integration test, insert via repository
    }

    @Given("{int} conflict entries with status {string} exist")
    public void conflictEntriesWithStatusExist(int count, String status) {
        // Pre-condition
    }

    @Given("a conflict entry with id {string} exists")
    public void conflictEntryWithIdExists(String id) {
        // Pre-condition
    }

    @Given("a conflict entry with id {string} and status {string}")
    public void conflictEntryWithIdAndStatus(String id, String status) {
        // Pre-condition
    }

    @Given("conflict entries exist for collection {string}")
    public void conflictEntriesExistForCollection(String collection) {
        // Pre-condition
    }

    @When("I GET {string}")
    public void get(String path) {
        lastResponse = webClient.get().uri(path).exchange();
    }

    @When("I POST to {string} with resolution data")
    public void postResolve(String path) {
        var resolveBody = Map.of(
                "conflict_id", "cf-resolve",
                "resolved_version", Map.of("key", "value"),
                "resolution_detail", "Admin resolved manually"
        );
        lastResponse = webClient.post().uri(path)
                .bodyValue(resolveBody)
                .exchange();
    }

    @Then("the response status is {int} {string}")
    public void responseStatusIs(int code, String ignored) {
        lastResponse.expectStatus().isEqualTo(code);
    }

    @Then("the response status is {int}")
    public void responseStatusIs(int code) {
        lastResponse.expectStatus().isEqualTo(code);
    }

    @Then("the response contains {string} array")
    public void responseContainsArray(String field) {
        lastResponse.expectBody()
                .jsonPath("$." + field).isArray();
    }

    @Then("the response contains {string}")
    public void responseContains(String field) {
        lastResponse.expectBody()
                .jsonPath("$." + field).isNotEmpty();
    }

    @Then("the response contains {string} array of length {int}")
    public void responseContainsArrayOfLength(String field, int length) {
        lastResponse.expectBody()
                .jsonPath("$." + field + ".length()").isEqualTo(length);
    }

    @Then("the response contains {string} equal to {string}")
    public void responseContainsFieldEqualTo(String field, String value) {
        lastResponse.expectBody()
                .jsonPath("$." + field).isEqualTo(value);
    }

    @Then("the response {string} array contains only entries with {string} equal to {string}")
    public void responseArrayContainsOnlyEntriesWith(String arrayField, String field, String value) {
        lastResponse.expectBody()
                .jsonPath("$." + arrayField + "[*]." + field)
                .value(values -> {
                    @SuppressWarnings("unchecked")
                    var list = (java.util.List<String>) values;
                    for (String v : list) {
                        assert v != null && v.equals(value) : "Expected " + value + " but got " + v;
                    }
                });
    }
}
