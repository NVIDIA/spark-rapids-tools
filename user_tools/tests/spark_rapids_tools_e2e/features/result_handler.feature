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


Feature: Tools Result Handler

  @test_id_API_0001
  Scenario Outline: Initialize a result handler on an empty directory
    Given an empty tools_out_dir "<tools_out_dir>"
    When I build a wrapper with report_id "<report_id>"
    Then the result handler should be empty
    And CSV report result of "coreRawApplicationInformationCSV" should be an empty dictionary
    And CSV report result of status-report should fail with exception "FileNotFoundError"
    And CSV report result of "coreRawApplicationInformationCSV" on appID "invalid-app-id" should fail with "ValueError"
    And combined results of CSV report "coreRawApplicationInformationCSV" should be empty
    Examples:
        | tools_out_dir   | report_id             |
        | .               | qualWrapperOutput     |
        | .               | profWrapperOutput     |
        | .               | qualCoreOutput        |
        | .               | profCoreOutput        |
        | ./subfolder     | qualWrapperOutput     |
        | ./subfolder     | profWrapperOutput     |
        | ./subfolder     | qualCoreOutput        |
        | ./subfolder     | profCoreOutput        |
