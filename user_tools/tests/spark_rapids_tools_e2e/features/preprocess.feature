Feature: Testing preprocessing functionality
  As a user of the preprocessing module
  I want to ensure the preprocessing functions work correctly
  So that I can reliably process Spark event logs

  Background:
    Given SPARK_RAPIDS_TOOLS_JAR environment variable is set
    And SPARK_HOME environment variable is set
    And QUALX_DATA_DIR environment variable is set
    And QUALX_CACHE_DIR environment variable is set
    And sample event logs in the QUALX_DATA_DIR
    And dataset JSON files in the datasets directory

  Scenario Outline: Test preprocessing with different QUALX_LABEL settings
    Given QUALX_LABEL environment variable is set to "<label>"
    When preprocessing the event logs
    Then preprocessing should complete successfully
    And preprocessed data should contain the expected features for label "<label>"

    Examples:
      | label        |
      | Duration     |
      | duration_sum |