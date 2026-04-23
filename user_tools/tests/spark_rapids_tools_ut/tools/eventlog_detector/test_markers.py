# Copyright (c) 2026, NVIDIA CORPORATION.
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

"""Unit tests for ``eventlog_detector.markers``."""
# pylint: disable=too-few-public-methods  # test classes naturally have few methods

import re

from spark_rapids_tools.tools.eventlog_detector import markers as m


class TestGpuMarkers:
    """Test GPU plugin marker constants."""

    def test_plugin_substring_exact(self):
        assert m.GPU_PLUGIN_CLASS_SUBSTRING == "com.nvidia.spark.SQLPlugin"

    def test_gpu_toggle_key_exact(self):
        assert m.GPU_ENABLED_KEY == "spark.rapids.sql.enabled"


class TestAuronMarkers:
    """Test Auron extension and enabled marker constants."""

    def test_extension_regex_fullmatches_expected_value(self):
        # Mirrors AuronParseHelper.extensionRegxMap.
        pat = re.compile(m.AURON_EXTENSION_REGEX)
        assert pat.fullmatch("org.apache.spark.sql.AuronSparkSessionExtension")
        assert pat.fullmatch("whatever.AuronSparkSessionExtension.more")
        assert not pat.fullmatch("org.apache.spark.sql.SomeOtherExtension")

    def test_auron_enabled_defaults_to_true(self):
        assert m.AURON_ENABLED_DEFAULT == "true"
        assert m.AURON_ENABLED_KEY == "spark.auron.enabled"
        assert m.AURON_SPARK_EXTENSIONS_KEY == "spark.sql.extensions"


class TestDatabricksPrecondition:
    """Test Databricks precondition key constants."""

    def test_all_three_tag_keys_present(self):
        assert m.DB_PRECONDITION_KEYS == (
            "spark.databricks.clusterUsageTags.clusterAllTags",
            "spark.databricks.clusterUsageTags.clusterId",
            "spark.databricks.clusterUsageTags.clusterName",
        )


class TestPhotonMarkers:
    """Test Photon marker regex constants."""

    def test_marker_map_fullmatches_expected(self):
        pats = {k: re.compile(v) for k, v in m.PHOTON_MARKER_REGEX.items()}
        assert pats[
            "spark.databricks.clusterUsageTags.sparkVersion"
        ].fullmatch("11.3.x-photon-scala2.12")
        assert pats[
            "spark.databricks.clusterUsageTags.runtimeEngine"
        ].fullmatch("PHOTON")
        assert not pats[
            "spark.databricks.clusterUsageTags.runtimeEngine"
        ].fullmatch("STANDARD")

    def test_all_four_photon_keys(self):
        assert set(m.PHOTON_MARKER_REGEX) == {
            "spark.databricks.clusterUsageTags.sparkVersion",
            "spark.databricks.clusterUsageTags.effectiveSparkVersion",
            "spark.databricks.clusterUsageTags.sparkImageLabel",
            "spark.databricks.clusterUsageTags.runtimeEngine",
        }


class TestDatabricksRollingFileName:
    """Test Databricks rolling log file name pattern constants."""

    def test_prefix_is_eventlog(self):
        assert m.DB_EVENT_LOG_FILE_PREFIX == "eventlog"

    def test_date_pattern_parses_scala_format(self):
        pat = re.compile(m.DB_EVENT_LOG_DATE_REGEX)
        # Scala's getDBEventLogFileDate splits on '--' and parses
        # 'eventlog-YYYY-MM-DD--HH-MM[.codec]'.
        assert pat.search("eventlog-2021-06-14--20-00.gz")
        assert pat.search("eventlog-2021-06-14--20-00")
        assert not pat.search("eventlog")  # bare eventlog has no date
