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

Feature: Event Log Processing

  @test_id_ELP_0001
  Scenario Outline: Tool spark_rapids runs with different types of event logs
    When spark-rapids tool is executed with "<event_logs>" eventlogs
    Then stderr contains the following
      """
      <expected_stderr>
      """
    And processed applications is "<processed_apps_count>"
    And return code is "0"

    Examples:
      | event_logs                         | expected_stderr                                                                                                       | processed_apps_count |
      | invalid_path_eventlog              | process.failure.count = 1;invalid_path_eventlog not found, skipping!                                                  | 0                    |
      | gpu_eventlog.zstd                  | process.skipped.count = 1;GpuEventLogException: Cannot parse event logs from GPU run: skipping this file              | 0                    |
      | photon_eventlog.zstd               | process.success.count = 1;                                                                                            | 1                    |
      | streaming_eventlog.zstd            | process.skipped.count = 1;StreamingEventLogException: Encountered Spark Structured Streaming Job: skipping this file! | 0                    |
      | incorrect_app_status_eventlog.zstd | process.NA.count = 1;IncorrectAppStatusException: Application status is incorrect. Missing AppInfo                    | 0                    |

  @test_id_ELP_0002
  Scenario: Qualification tool JAR crashes
    Given thread to crash qualification tool has started
    When spark-rapids tool is executed with "join_agg_on_yarn_eventlog.zstd" eventlogs
    Then stderr contains the following
      """
      Qualification. Raised an error in phase [Execution]
      """
    And return code is "1"
