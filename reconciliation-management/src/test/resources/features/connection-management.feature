Feature: Connection Management
  As a platform operator
  I want to manage database connections
  So that I can configure source and target clusters for reconciliation

  Scenario: Register a new MongoDB connection
    Given a connection request with name "gcp-mongo-prod" and type "MONGODB"
    And cluster label "gcp"
    When I POST the connection to "/api/v1/connections"
    Then the response status is 201 CREATED
    And the response contains a UUID "id"
    And the response contains "name" equal to "gcp-mongo-prod"
    And the response contains "db_type" equal to "MONGODB"

  Scenario: Register a new PostgreSQL connection
    Given a connection request with name "audit-pg-prod" and type "POSTGRESQL"
    And cluster label "audit"
    When I POST the connection to "/api/v1/connections"
    Then the response status is 201 CREATED
    And the response contains "db_type" equal to "POSTGRESQL"

  Scenario: List all connections
    Given a connection "gcp-prod" exists
    And a connection "hic-prod" exists
    When I GET "/api/v1/connections"
    Then the response status is 200 OK
    And the response is a JSON array of length 2

  Scenario: Get connection by ID
    Given a connection with id "conn-123" exists
    When I GET "/api/v1/connections/conn-123"
    Then the response status is 200 OK
    And the response contains "id" equal to "conn-123"

  Scenario: Get non-existent connection returns 404
    When I GET "/api/v1/connections/nonexistent"
    Then the response status is 500 or 404

  Scenario: Update connection details
    Given a connection with id "conn-456" exists
    When I PUT to "/api/v1/connections/conn-456" with updated name "gcp-updated"
    Then the response status is 200 OK
    And the response contains "name" equal to "gcp-updated"

  Scenario: Delete a connection
    Given a connection with id "conn-del" exists
    When I DELETE "/api/v1/connections/conn-del"
    Then the response status is 204 NO CONTENT

  Scenario: Validation error on empty connection name
    Given a connection request with name "" and type "MONGODB"
    When I POST the connection to "/api/v1/connections"
    Then the response status is 400 BAD REQUEST
