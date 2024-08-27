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

Feature: Tool Installation Checks

  @test_id_IC_0001
  Scenario Outline: Environment has missing CLI and spark_rapids tool processes eventlogs
    Given platform is "<platform>"
    And "<cli>" is not installed
    When spark-rapids tool is executed with "valid_eventlog_1.zstd,valid_eventlog_2.zstd" eventlogs
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

  @test_id_IC_0002
  Scenario: Environment has missing java
    Given "java" is not installed
    When spark-rapids tool is executed with "valid_eventlog_1.zstd" eventlogs
    Then stderr contains the following
      """
      RuntimeError: Error invoking CMD
      """
    And return code is "1"
