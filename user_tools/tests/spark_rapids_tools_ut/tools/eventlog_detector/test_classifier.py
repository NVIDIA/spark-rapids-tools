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
# pylint: disable=too-few-public-methods  # test classes naturally have few methods

import pytest

from spark_rapids_tools.tools.eventlog_detector.classifier import (
    _classify_runtime,
    _has_rapids_conf_markers,
)
from spark_rapids_tools.tools.eventlog_detector.types import SparkRuntime


class TestEmptyProperties:
    """Test classification with an empty properties dict."""

    def test_empty_props_is_spark(self):
        assert _classify_runtime({}) is SparkRuntime.SPARK


class TestRapidsConfigMarkers:
    """Test marker presence checks used by the CPU fast path."""

    def test_plain_spark_props_have_no_rapids_markers(self):
        assert _has_rapids_conf_markers({"spark.master": "local"}) is False

    def test_disabled_rapids_plugin_still_counts_as_marker(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": "false",
        }
        assert _classify_runtime(props) is SparkRuntime.SPARK
        assert _has_rapids_conf_markers(props) is True

    def test_rapids_enabled_key_alone_counts_as_marker(self):
        assert _has_rapids_conf_markers({"spark.rapids.sql.enabled": "true"}) is True


class TestSparkRapids:
    """Test SPARK_RAPIDS classification logic."""

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

    @pytest.mark.parametrize("bogus_value", ["no", "0", "yes", "1", "", "maybe"])
    def test_non_toboolean_values_default_to_true_matching_scala(self, bogus_value):
        # Scala: Try { "no".toBoolean }.getOrElse(true) == true because
        # "no" is not parseable. The Python classifier must do the same.
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": bogus_value,
        }
        assert _classify_runtime(props) is SparkRuntime.SPARK_RAPIDS
