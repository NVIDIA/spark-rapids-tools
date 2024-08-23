# Copyright (c) 2024, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Feature: Spark Rapids Tools End-to-End Behavior
# Notes:
# Use semi-colon to separate multiple expected outputs

  @test_id_0001
  Scenario Outline: Environment has missing CLI and spark_rapids tool runs with single eventlog
    Given platform is "<platform>"
    And "<cli>" is not installed
    When spark-rapids tool is executed with "test_event_log_1,test_event_log_2" eventlogs
    Then stdout contains the following
      """
      <expected_stdout>
      """
    And processed applications is "2"
    And return code is "0"

    Examples:
      | platform         | cli    | expected_stdout           |
      | dataproc         | gcloud | 1 x n1-standard-8         |
      | emr              | aws    | 1 x g5.2xlarge            |
      | databricks-aws   | aws    | 1 x g5.2xlarge            |
      | databricks-azure | az     | 1 x Standard_NC8as_T4_v3  |

  @test_id_0002
  Scenario Outline: Tool spark_rapids runs with different types of event logs
    When spark-rapids tool is executed with "<event_logs>" eventlogs
    Then stderr contains the following
      """
      <expected_stderr>
      """
    And processed applications is "0"
    And return code is "0"

    Examples:
      | event_logs                              | expected_stderr                                                                                                       |
      | test_id_0005_incorrect_event_logs       | process.failure.count = 1;test_id_0005_incorrect_event_logs not found, skipping!                                      |
      | test_id_0005_gpu                        | process.skipped.count = 1;GpuEventLogException: Cannot parse event logs from GPU run: skipping this file              |
      | test_id_0005_photon                     | process.skipped.count = 1;PhotonEventLogException: Encountered Databricks Photon event log: skipping this file!       |
      | test_id_0005_streaming                  | process.skipped.count = 1;StreamingEventLogException: Encountered Spark Structured Streaming Job: skipping this file! |
      | test_id_0005_incorrect_app_status       | process.NA.count = 1;IncorrectAppStatusException: Application status is incorrect. Missing AppInfo                    |

  @test_id_0002
  Scenario: Environment has missing java
    Given "java" is not installed
    When spark-rapids tool is executed with "test_event_log_1" eventlogs
    Then stderr contains the following
      """
      RuntimeError: Error invoking CMD
      """
    And return code is "1"

  @test_id_0004
  Scenario: Qualification tool JAR crashes
    Given thread to crash qualification tool has started
    When spark-rapids tool is executed with "test_event_log_1" eventlogs
    Then stderr contains the following
      """
      Qualification. Raised an error in phase [Execution]
      """
    And return code is "1"

  @test_id_0005_1 @long_running
  Scenario Outline: Eventlogs are stored in HDFS - Platform specified
    Given platform is "<platform>"
    And HDFS is "running"
    And HDFS has "<eventlog>" eventlogs
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then processed applications is "1"
    And return code is "0"

    Examples:
      | platform | eventlog         |
      | onprem   | test_event_log_1 |
      | dataproc | test_event_log_1 |


  @test_id_0005_2 @long_running
  Scenario Outline: Eventlogs are stored in HDFS - Platform not specified
    Given HDFS is "running"
    And HDFS has "<eventlog>" eventlogs
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then processed applications is "1"
    And return code is "0"

    Examples:
      | eventlog         |
      | test_event_log_1 |

  @test_id_0005_3 @long_running
  Scenario Outline: Eventlogs are stored in HDFS - HDFS installed but not running
    Given HDFS is "not running"
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then stderr contains the following
      """
      EventLogPathProcessor: Unexpected exception occurred reading hdfs:///test_event_log_1, skipping!
      """
    And processed applications is "0"
    And return code is "0"

    Examples:
      | eventlog         |
      | test_event_log_1 |

  @test_id_0005_4 @long_running
  Scenario Outline: Eventlogs are stored in HDFS - HDFS not installed, Platform specified
    Given platform is "onprem"
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then stderr contains the following
      """
      EventLogPathProcessor: Unexpected exception occurred reading hdfs:///test_event_log_1, skipping!;Incomplete HDFS URI
      """
    And processed applications is "0"
    And return code is "0"

    Examples:
      | eventlog         |
      | test_event_log_1 |

  @test_id_0005_5 @long_running
  Scenario Outline: Eventlogs are stored in HDFS - HDFS not installed, Platform not specified
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then stderr contains the following
      """
      EventLogPathProcessor: Unexpected exception occurred reading hdfs:///test_event_log_1, skipping!;Incomplete HDFS URI
      """
    And processed applications is "0"
    And return code is "0"

    Examples:
      | eventlog         |
      | test_event_log_1 |
