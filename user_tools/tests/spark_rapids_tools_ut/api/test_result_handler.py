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

"""
Test the main behavioral expectations from the ToolsAPI. For example, the behavior when loading
empty folders, or non-existing reports...etc.
"""

import pytest

from spark_rapids_tools.api_v1 import APIHelpers, CSVReport, CSVReportCombiner
from ..conftest import SparkRapidsToolsUT


class TestResultHandler(SparkRapidsToolsUT):
    """
    Unit tests for the QualWrapper result handler utilities.
    These tests validate the behavior of the result handler when dealing with empty directories and
    missing files, ensuring robust handling of edge cases and error conditions.
    """

    @pytest.mark.parametrize('dir_name', ['', 'sub_folder'])
    def test_behavior_with_empty_folders(self, tmp_path, dir_name):
        """
        Test that the result handler does not fail when the folder is empty/non-existing.
        Steps:
        1. Create a test directory (empty or subfolder) using `tmp_path`.
        2. Build a QualWrapper handler for the test directory.
        3. Assert the handler is empty.
        4. Attempt to load a global CSV report and verify a `FileNotFoundError` is raised.
        5. Attempt to load a per-app CSV report and assert the result is an empty dictionary.
        6. Attempt to load a specific appId CSV report and verify the result is a failed
           `LoadDFResult` with the correct error.
        7. Combine a CSV report from the empty handler and verify the result is empty and successful.
        """
        test_dir = tmp_path / dir_name if dir_name else tmp_path
        handler = APIHelpers.build_qual_wrapper_handler(dir_path=str(test_dir))
        assert handler.is_empty()

        # Attempt to access a global CSV report that does not exist.
        # This should raise a FileNotFoundError.
        # Why this is correct? Because a global CSV report expected to load a specific file.
        csv_report = CSVReport(handler).table('qualCoreCSVSummary').load()
        with pytest.raises(FileNotFoundError):
            raise csv_report.get_fail_cause()

        # Attempt to access a per-app CSV report that does not exist.
        # In that case, the returned result is a dictionary. It should be empty dictionary.
        # Why this is correct? Because there are no-apps to form the dictionary keys
        per_app_csv_rep = CSVReport(handler).table('coreRawApplicationInformationCSV').load()
        assert per_app_csv_rep == {}

        # Attempt to access a specific appId CSV report that does not exist.
        # In that case, the returned result is failed LoadDFResult
        # Why this is correct? Because there are no-apps to form the dictionary keys
        invalid_app_id_csv_rep = (
            CSVReport(handler)
            .table('coreRawApplicationInformationCSV')
            .app('invalid-app-id')
            .load()
        )
        assert invalid_app_id_csv_rep.data is None
        assert invalid_app_id_csv_rep.success is False
        assert invalid_app_id_csv_rep.fallen_back is False
        with pytest.raises(
                ValueError,
                match=r'Application \[invalid-app-id\] not found in report \[coreRawMetrics\]\.'
        ):
            raise invalid_app_id_csv_rep.get_fail_cause()

        # Attempt to combine a CSV report from the empty handler.
        # This should return an empty DataFrame.
        # Why this is correct? Because the handler has no defined app_handlers.
        csv_combined = CSVReportCombiner([
            CSVReport(handler).table('coreRawApplicationInformationCSV')
        ]).build()
        assert csv_combined.success
        assert csv_combined.fallen_back is False
        assert csv_combined.data.empty
