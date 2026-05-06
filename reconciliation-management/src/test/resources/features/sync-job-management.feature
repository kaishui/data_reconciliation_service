Feature: Sync Job Management
  As a platform operator
  I want to create and manage sync jobs
  So that I can configure bidirectional reconciliation between clusters

  Scenario: Create a sync job with collection mappings
    Given a source connection "src-gcp" exists
    And a target connection "tgt-hic" exists
    And a sync job request with name "orders-recon" and 2 mappings
    When I POST the sync job to "/api/v1/sync-jobs"
    Then the response status is 201 CREATED
    And the response contains "name" equal to "orders-recon"
    And the response contains "mappings" array of length 2

  Scenario: Create sync job fails when source connection missing
    Given a sync job request referencing source "nonexistent-src"
    When I POST the sync job to "/api/v1/sync-jobs"
    Then the response status is 500 or 400

  Scenario: Deploy a stopped sync job
    Given a sync job with id "job-deploy" and status "STOPPED"
    When I POST to "/api/v1/sync-jobs/job-deploy/deploy"
    Then the response status is 200 OK
    And the response contains "status" equal to "RUNNING"

  Scenario: Cannot deploy an already running job
    Given a sync job with id "job-running" and status "RUNNING"
    When I POST to "/api/v1/sync-jobs/job-running/deploy"
    Then the response status is 500

  Scenario: Stop a running sync job
    Given a sync job with id "job-stop" and status "RUNNING"
    When I POST to "/api/v1/sync-jobs/job-stop/stop"
    Then the response status is 200 OK
    And the response contains "status" equal to "STOPPED"

  Scenario: Restart a stopped sync job
    Given a sync job with id "job-restart" and status "STOPPED"
    When I POST to "/api/v1/sync-jobs/job-restart/restart"
    Then the response status is 200 OK
    And the response contains "status" equal to "RUNNING"

  Scenario: Delete a stopped sync job
    Given a sync job with id "job-del" and status "STOPPED"
    When I DELETE "/api/v1/sync-jobs/job-del"
    Then the response status is 204 NO CONTENT

  Scenario: Cannot delete a running sync job
    Given a sync job with id "job-del-running" and status "RUNNING"
    When I DELETE "/api/v1/sync-jobs/job-del-running"
    Then the response status is 500
