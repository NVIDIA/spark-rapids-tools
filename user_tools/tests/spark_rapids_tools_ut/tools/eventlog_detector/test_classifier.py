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

"""Unit tests for ``eventlog_detector.classifier``."""

import pytest

from spark_rapids_tools.tools.eventlog_detector.classifier import _classify_runtime
from spark_rapids_tools.tools.eventlog_detector.types import SparkRuntime


class TestEmptyProperties:
    def test_empty_props_is_spark(self):
        assert _classify_runtime({}) is SparkRuntime.SPARK


class TestSparkRapids:
    def test_plugin_and_default_enabled(self):
        props = {"spark.plugins": "foo,com.nvidia.spark.SQLPlugin,bar"}
        assert _classify_runtime(props) is SparkRuntime.SPARK_RAPIDS

    def test_plugin_with_enabled_true(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": "true",
        }
        assert _classify_runtime(props) is SparkRuntime.SPARK_RAPIDS

    def test_plugin_with_enabled_false_demotes_to_spark(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": "false",
        }
        assert _classify_runtime(props) is SparkRuntime.SPARK

    def test_enabled_true_without_plugin_is_still_spark(self):
        props = {"spark.rapids.sql.enabled": "true"}
        assert _classify_runtime(props) is SparkRuntime.SPARK

    def test_unparseable_enabled_defaults_to_true(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": "not-a-bool",
        }
        assert _classify_runtime(props) is SparkRuntime.SPARK_RAPIDS


class TestAuron:
    def test_extension_and_default_enabled(self):
        props = {"spark.sql.extensions": "com.bytedance.auron.AuronSparkSessionExtension"}
        assert _classify_runtime(props) is SparkRuntime.AURON

    def test_extension_and_enabled_false_demotes_to_spark(self):
        props = {
            "spark.sql.extensions": "com.bytedance.auron.AuronSparkSessionExtension",
            "spark.auron.enabled": "FALSE",
        }
        assert _classify_runtime(props) is SparkRuntime.SPARK

    def test_auron_enabled_case_insensitive(self):
        props = {
            "spark.sql.extensions": "AuronSparkSessionExtension",
            "spark.auron.enabled": " TrUe ",
        }
        assert _classify_runtime(props) is SparkRuntime.AURON


class TestDatabricksPhoton:
    @pytest.fixture
    def db_precond_props(self):
        return {
            "spark.databricks.clusterUsageTags.clusterAllTags": "[{...}]",
            "spark.databricks.clusterUsageTags.clusterId": "1234",
            "spark.databricks.clusterUsageTags.clusterName": "dev-cluster",
        }

    def test_precondition_only_is_spark(self, db_precond_props):
        assert _classify_runtime(db_precond_props) is SparkRuntime.SPARK

    def test_precondition_plus_photon_version(self, db_precond_props):
        props = {
            **db_precond_props,
            "spark.databricks.clusterUsageTags.sparkVersion": "11.3.x-photon-scala2.12",
        }
        assert _classify_runtime(props) is SparkRuntime.PHOTON

    def test_precondition_plus_photon_engine(self, db_precond_props):
        props = {**db_precond_props, "spark.databricks.clusterUsageTags.runtimeEngine": "PHOTON"}
        assert _classify_runtime(props) is SparkRuntime.PHOTON

    def test_photon_marker_without_precondition_is_spark(self):
        props = {"spark.databricks.clusterUsageTags.runtimeEngine": "PHOTON"}
        assert _classify_runtime(props) is SparkRuntime.SPARK

    def test_photon_engine_other_value_is_spark(self, db_precond_props):
        props = {**db_precond_props, "spark.databricks.clusterUsageTags.runtimeEngine": "STANDARD"}
        assert _classify_runtime(props) is SparkRuntime.SPARK


class TestPriority:
    """PHOTON > AURON > SPARK_RAPIDS > SPARK when markers coexist."""

    def test_photon_beats_spark_rapids(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.databricks.clusterUsageTags.clusterAllTags": "[{...}]",
            "spark.databricks.clusterUsageTags.clusterId": "1",
            "spark.databricks.clusterUsageTags.clusterName": "c",
            "spark.databricks.clusterUsageTags.runtimeEngine": "PHOTON",
        }
        assert _classify_runtime(props) is SparkRuntime.PHOTON

    def test_auron_beats_spark_rapids(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.sql.extensions": "AuronSparkSessionExtension",
        }
        assert _classify_runtime(props) is SparkRuntime.AURON
