# Copyright (c) 2025, NVIDIA CORPORATION.
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

Feature: Testing preprocessing functionality
  As a user of the preprocessing module
  I want to ensure the preprocessing functions work correctly
  So that I can reliably process Spark event logs

  Background:
    Given SPARK_HOME environment variable is set
    And QUALX_DATA_DIR environment variable is set
    And QUALX_CACHE_DIR environment variable is set
    And sample event logs in the QUALX_DATA_DIR
    And dataset JSON files in the datasets directory

  Scenario Outline: Test preprocessing with different QUALX_LABEL settings
    Given QUALX_LABEL environment variable is set to "<label>"
    When preprocessing the event logs
    Then preprocessing should complete successfully
    And preprocessed data should contain the expected features for label "<label>"
    And sqlID hashes should align between CPU and GPU runs

    Examples:
      | label        |
      | Duration     |
      | duration_sum |
