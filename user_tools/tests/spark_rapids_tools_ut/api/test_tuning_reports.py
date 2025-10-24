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

"""Unit tests for tuning reports API."""

from spark_rapids_tools.api_v1 import QualCore
from .test_api_base import TuningAPITestBase


class TestTuningReportsAPI(TuningAPITestBase):
    """
    Verifies that tuning files can be loaded via the API.
    """

    def test_load_bootstrap_conf(self):
        """Test loading bootstrap.conf for a single application."""
        self.create_tuning_files(self.sample_app_id_1)

        handler = QualCore(self.core_output)

        result = handler.txt('tuningBootstrapConf').app(self.sample_app_id_1).load()

        # Verify successful load
        self.assertTrue(result.success)

        # Get text content (handles both str and bytes)
        text = result.data if isinstance(result.data, str) else result.decode_txt()
        self.assertIsNotNone(text)

        # Verify Spark CLI format
        self.assertIn('--conf spark.rapids.sql.enabled=true', text)
        self.assertIn('--conf spark.executor.cores=16', text)

    def test_load_recommendations_log(self):
        """Test loading recommendations.log with properties and comments."""
        self.create_tuning_files(self.sample_app_id_1)

        handler = QualCore(self.core_output)

        result = handler.txt('tuningRecommendationsLog').app(self.sample_app_id_1).load()

        self.assertTrue(result.success)

        # Get text content
        text = result.data if isinstance(result.data, str) else result.decode_txt()

        # Verify format sections exist
        self.assertIn('Spark Properties:', text)
        self.assertIn('Comments:', text)

    def test_load_combined_conf(self):
        """Test loading optional combined.conf file."""
        self.create_tuning_files(self.sample_app_id_1, include_combined=True)

        handler = QualCore(self.core_output)

        result = handler.txt('tuningCombinedConf').app(self.sample_app_id_1).load()

        # Should succeed when file exists
        self.assertTrue(result.success)

        # Get text content
        text = result.data if isinstance(result.data, str) else result.decode_txt()
        self.assertIn(f'spark.app.id={self.sample_app_id_1}', text)

    def test_load_multiple_apps(self):
        """Test loading tuning files for multiple applications."""
        self.create_tuning_files(self.sample_app_id_1)
        self.create_tuning_files(self.sample_app_id_2)

        handler = QualCore(self.core_output)

        results = handler.txt('tuningBootstrapConf').apps([self.sample_app_id_1, self.sample_app_id_2]).load()

        # Should return dictionary with both apps
        self.assertIsInstance(results, dict)
        self.assertEqual(len(results), 2)
        self.assertTrue(results[self.sample_app_id_1].success)
        self.assertTrue(results[self.sample_app_id_2].success)
