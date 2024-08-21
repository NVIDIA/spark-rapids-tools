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

  @test_id_0002
  Scenario Outline: Environment has missing CLI and spark_rapids tool runs with single eventlog
    Given platform is "<platform>"
    And "<cli>" is not installed
    When spark-rapids tool is executed with "test_event_log_1,test_event_log_2" eventlogs
    Then stdout contains the following
      """
      <expected_cluster>
      """
    And return code is "0"

    Examples:
      | platform         | cli    | expected_cluster         |
      | dataproc         | gcloud | 1 x n1-standard-8        |
      | emr              | aws    | 1 x g5.2xlarge           |
      | databricks-aws   | aws    | 1 x g5.2xlarge           |
      | databricks-azure | az     | 1 x Standard_NC8as_T4_v3 |

  @test_id_0004
  Scenario: Environment has missing java
    Given "java" is not installed
    When spark-rapids tool is executed with "test_event_log_1" eventlogs
    Then stderr contains the following
      """
      java: command not found
      """
    And return code is "1"

  @test_id_0005
  Scenario Outline: Tool spark_rapids runs with different types of event logs
    When spark-rapids tool is executed with "<event_logs>" eventlogs
    Then stderr contains the following
      """
      <expected_stderr>
      """
    And return code is "<return_code>"

    Examples:
      | event_logs                              | expected_stderr                                                                                                       | return_code |
      | test_id_0005_incorrect_event_logs       | process.failure.count = 1;test_id_0005_incorrect_event_logs not found, skipping!                                      | 0           |
      | test_id_0005_gpu                        | process.skipped.count = 1;GpuEventLogException: Cannot parse event logs from GPU run: skipping this file              | 0           |
      | test_id_0005_photon                     | process.skipped.count = 1;PhotonEventLogException: Encountered Databricks Photon event log: skipping this file!       | 0           |
      | test_id_0005_streaming                  | process.skipped.count = 1;StreamingEventLogException: Encountered Spark Structured Streaming Job: skipping this file! | 0           |
      | test_id_0005_incorrect_app_status       | process.NA.count = 1;IncorrectAppStatusException: Application status is incorrect. Missing AppInfo                    | 0           |

  @test_id_0009
  Scenario: Qualification tool JAR crashes
    Given thread to crash qualification tool has started
    When spark-rapids tool is executed with "test_event_log_1" eventlogs
    Then stderr contains the following
      """
      Qualification. Raised an error in phase [Execution]
      """
    And return code is "1"


  @test_id_0010
  Scenario Outline: Eventlogs are stored in HDFS - Platform specified
    Given platform is "<platform>"
    And HDFS is "running"
    And HDFS has "<eventlog>" eventlogs
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then stdout contains the following
      """
      Processed applications  1
      """
    And return code is "0"

    Examples:
      | platform | eventlog         |
      | onprem   | test_event_log_1 |
      | dataproc | test_event_log_1 |


  @test_id_0011
  Scenario Outline: Eventlogs are stored in HDFS - Platform not specified
    Given HDFS is "running"
    And HDFS has "<eventlog>" eventlogs
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then stdout contains the following
      """
      Processed applications  1
      """
    And return code is "0"

    Examples:
      | eventlog         |
      | test_event_log_1 |

  @test_id_0012
  Scenario Outline: Eventlogs are stored in HDFS - HDFS installed but not running
    Given HDFS is "not running"
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then stdout contains the following
      """
      Processed applications  0
      """
    And return code is "0"

    Examples:
      | eventlog         |
      | test_event_log_1 |

  @test_id_0013
  Scenario Outline: Eventlogs are stored in HDFS - HDFS not installed
    Given platform is "onprem"
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then stderr contains the following
      """
      Failed to create HadoopFileSystem handler
      """
    And return code is "1"

    Examples:
      | eventlog         |
      | test_event_log_1 |
