# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
    Given platform is "<platform>"
    When spark-rapids tool is executed with "<event_logs>" eventlogs
    Then stderr contains the following
      """
      <expected_stderr>
      """
    And processed applications is "<processed_apps_count>"
    And return code is "0"

    Examples:
      | platform 		| event_logs                         | expected_stderr                                                                                                       | processed_apps_count |
      | onprem   		| invalid_path_eventlog              | process.failure.count = 1;invalid_path_eventlog not found, skipping!                                                  | 0                    |
      | onprem   		| gpu_eventlog.zstd                  | process.skipped.count = 1;GpuEventLogException: Cannot parse event logs from GPU run: skipping this file              | 0                    |
      | onprem   		| streaming_eventlog.zstd            | process.skipped.count = 1;StreamingEventLogException: Encountered Spark Structured Streaming Job: skipping this file! | 0                    |
      | onprem   		| incorrect_app_status_eventlog.zstd | process.NA.count = 1;IncorrectAppStatusException: Application status is incorrect. Missing AppInfo                    | 0                    |
      | onprem  		| photon_eventlog.zstd               | process.skipped.count = 1;UnsupportedSparkRuntimeException: Platform 'onprem' does not support the runtime 'PHOTON'   | 0                    |
      | databricks-aws  | photon_eventlog.zstd               | process.success.count = 1;                                                                                            | 1                    |

  @test_id_ELP_0002
  Scenario: Qualification tool JAR crashes
    Given thread to crash qualification tool has started
    When spark-rapids tool is executed with "join_agg_on_yarn_eventlog.zstd" eventlogs
    Then stderr contains the following
      """
      Qualification. Raised an error in phase [Execution]
      """
    And return code is "1"

  @test_id_ELP_0003
  Scenario Outline: Check for expected output in generated files
    Given platform is "<platform>"
    Given environment variable "<variable>" is set to "<value>"
    When spark-rapids tool is executed with "<eventlog>" eventlogs
    When "<file>" file is generated
    When "<app_id>" app is not qualified
    Then not qualified reason is "<expected_reason>"
    And return code is "0"

    Examples:
      | platform 	| variable                                                  | value           |eventlog                                                    | file                          | app_id                  | expected_reason                                              |
      | onprem  	| RAPIDS_USER_TOOLS_SPILL_BYTES_THRESHOLD                   |   -1            |onprem/nds/power/eventlogs/cpu/app-20231122005806-0064.zstd | qualification_summary.csv     | app-20231122005806-0064 | Skipping due to total data spill in stages exceeding -1.00 B.|
      | onprem  	| RAPIDS_USER_TOOLS_GPU_SPEEDUP_SPARK_LOWER_BOUND           |   20.0          |onprem/nds/power/eventlogs/cpu/app-20231122005806-0064.zstd | qualification_summary.csv     | app-20231122005806-0064 | Skipping due to total data spill in stages exceeding -1.00 B.; GPU SpeedUp < Qualifying Threshold (More is qualified) |
      | onprem  	| RAPIDS_USER_TOOLS_UNSUPPORTED_OPERATORS_SPARK_UPPER_BOUND |   -1            |onprem/nds/power/eventlogs/cpu/app-20231122005806-0064.zstd | qualification_summary.csv     | app-20231122005806-0064 | Skipping due to total data spill in stages exceeding -1.00 B.; GPU SpeedUp < Qualifying Threshold (More is qualified); Unsupported Operators Stage Duration Percent > Qualifying Threshold (Less is qualified) |
