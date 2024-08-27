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

@long_running
Feature: HDFS Event Log Storage

  @test_id_HDFS_0001
  Scenario Outline: Eventlogs are stored in HDFS - Platform specified
    Given platform is "<platform>"
    And HDFS is "running"
    And HDFS has "<eventlog>" eventlogs
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then processed applications is "1"
    And return code is "0"

    Examples:
      | platform | eventlog              |
      | onprem   | valid_eventlog_1.zstd |
      | dataproc | valid_eventlog_1.zstd |

  @test_id_HDFS_0002
  Scenario Outline: Eventlogs are stored in HDFS - Platform not specified
    Given HDFS is "running"
    And HDFS has "<eventlog>" eventlogs
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then processed applications is "1"
    And return code is "0"

    Examples:
      | eventlog              |
      | valid_eventlog_1.zstd |

  @test_id_HDFS_0003
  Scenario Outline: Eventlogs are stored in HDFS - HDFS installed but not running
    Given HDFS is "not running"
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then stderr contains the following
      """
      EventLogPathProcessor: Unexpected exception occurred reading hdfs:///<eventlog>, skipping!
      """
    And processed applications is "0"
    And return code is "0"

    Examples:
      | eventlog              |
      | valid_eventlog_1.zstd |

  @test_id_HDFS_0004
  Scenario Outline: Eventlogs are stored in HDFS - HDFS not installed, Platform specified
    Given platform is "onprem"
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then stderr contains the following
      """
      EventLogPathProcessor: Unexpected exception occurred reading hdfs:///<eventlog>, skipping!;Incomplete HDFS URI
      """
    And processed applications is "0"
    And return code is "0"

    Examples:
      | eventlog              |
      | valid_eventlog_1.zstd |

  @test_id_HDFS_0005
  Scenario Outline: Eventlogs are stored in HDFS - HDFS not installed, Platform not specified
    When spark-rapids tool is executed with "hdfs:///<eventlog>" eventlogs
    Then stderr contains the following
      """
      EventLogPathProcessor: Unexpected exception occurred reading hdfs:///<eventlog>, skipping!;Incomplete HDFS URI
      """
    And processed applications is "0"
    And return code is "0"

    Examples:
      | eventlog              |
      | valid_eventlog_1.zstd |
