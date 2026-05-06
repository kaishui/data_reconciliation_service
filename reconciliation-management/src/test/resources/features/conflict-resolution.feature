Feature: Conflict Resolution
  As a data steward
  I want to view and resolve data conflicts
  So that I can ensure data consistency across clusters

  Scenario: Query conflict log for a specific job
    Given a conflict entry exists for sync job "job-1" with status "AUTO_RESOLVED"
    When I GET "/api/v1/conflicts?syncJobId=job-1&page=0&size=20"
    Then the response status is 200 OK
    And the response contains "content" array
    And the response contains "totalElements"

  Scenario: Query unresolved conflicts for manual review
    Given 3 conflict entries with status "MANUAL_REQUIRED" exist
    When I GET "/api/v1/conflicts?status=MANUAL_REQUIRED&page=0&size=10"
    Then the response status is 200 OK
    And the response contains "content" array of length 3

  Scenario: Get conflict detail by ID
    Given a conflict entry with id "cf-detail" exists
    When I GET "/api/v1/conflicts/cf-detail"
    Then the response status is 200 OK
    And the response contains "id" equal to "cf-detail"
    And the response contains "resolution_status"

  Scenario: Manually resolve a conflict
    Given a conflict entry with id "cf-resolve" and status "MANUAL_REQUIRED"
    When I POST to "/api/v1/conflicts/cf-resolve/resolve" with resolution data
    Then the response status is 200 OK
    And the response contains "resolution_status" equal to "AUTO_RESOLVED"

  Scenario: Cannot resolve an already resolved conflict
    Given a conflict entry with id "cf-done" and status "AUTO_RESOLVED"
    When I POST to "/api/v1/conflicts/cf-done/resolve" with resolution data
    Then the response status is 500

  Scenario: Query conflicts by collection name
    Given conflict entries exist for collection "orders"
    When I GET "/api/v1/conflicts?collectionName=orders&page=0&size=20"
    Then the response status is 200 OK
    And the response "content" array contains only entries with "collection_name" equal to "orders"
