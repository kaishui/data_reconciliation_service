package com.recon.management.cucumber;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Step definitions for Connection Management features.
 */
public class ConnectionStepDefinitions {

    private final WebTestClient webClient;
    private WebTestClient.ResponseSpec lastResponse;
    private Map<String, Object> requestBody;

    public ConnectionStepDefinitions() {
        this.webClient = WebTestClient.bindToServer().baseUrl("http://localhost:8080").build();
    }

    @Given("a connection request with name {string} and type {string}")
    public void connectionRequestWithNameAndType(String name, String type) {
        requestBody = Map.of(
                "name", name,
                "db_type", type,
                "connection_string", "mongodb://localhost:27017",
                "cluster_label", "test"
        );
    }

    @Given("cluster label {string}")
    public void clusterLabel(String label) {
        requestBody = new java.util.HashMap<>(requestBody);
        requestBody.put("cluster_label", label);
    }

    @Given("a connection with id {string} exists")
    public void connectionWithIdExists(String id) {
        // Pre-condition: connection should exist in DB
        // In a real integration test, insert via repository
    }

    @Given("a connection {string} exists")
    public void connectionExists(String name) {
        // Pre-condition
    }

    @Given("a source connection {string} exists")
    public void sourceConnectionExists(String name) {
        // Pre-condition
    }

    @Given("a target connection {string} exists")
    public void targetConnectionExists(String name) {
        // Pre-condition
    }

    @Given("a connection request with name {string} and type {string}")
    public void connectionRequestWithEmptyName(String name, String type) {
        requestBody = Map.of(
                "name", name,
                "db_type", type,
                "connection_string", "",
                "cluster_label", ""
        );
    }

    @When("I POST the connection to {string}")
    public void postConnection(String path) {
        lastResponse = webClient.post().uri(path)
                .bodyValue(requestBody != null ? requestBody : Map.of())
                .exchange();
    }

    @When("I GET {string}")
    public void get(String path) {
        lastResponse = webClient.get().uri(path).exchange();
    }

    @When("I PUT to {string} with updated name {string}")
    public void putWithUpdatedName(String path, String name) {
        var updateBody = Map.of(
                "name", name,
                "db_type", "MONGODB",
                "connection_string", "mongodb://localhost:27017",
                "cluster_label", "test"
        );
        lastResponse = webClient.put().uri(path)
                .bodyValue(updateBody)
                .exchange();
    }

    @When("I DELETE {string}")
    public void delete(String path) {
        lastResponse = webClient.delete().uri(path).exchange();
    }

    @Then("the response status is {int} {string}")
    public void responseStatusIs(int code, String ignored) {
        lastResponse.expectStatus().isEqualTo(code);
    }

    @Then("the response status is {int} or {int}")
    public void responseStatusIsEither(int code1, int code2) {
        // Allow either status code
        try {
            lastResponse.expectStatus().isEqualTo(code1);
        } catch (AssertionError e) {
            lastResponse.expectStatus().isEqualTo(code2);
        }
    }

    @Then("the response status is {int}")
    public void responseStatusIs(int code) {
        lastResponse.expectStatus().isEqualTo(code);
    }

    @Then("the response contains a UUID {string}")
    public void responseContainsUUID(String field) {
        lastResponse.expectBody()
                .jsonPath("$." + field).isNotEmpty();
    }

    @Then("the response contains {string} equal to {string}")
    public void responseContainsFieldEqualTo(String field, String value) {
        lastResponse.expectBody()
                .jsonPath("$." + field).isEqualTo(value);
    }

    @Then("the response is a JSON array of length {int}")
    public void responseIsJsonArrayOfLength(int length) {
        lastResponse.expectBody()
                .jsonPath("$.length()").isEqualTo(length);
    }
}
