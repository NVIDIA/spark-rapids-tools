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
    classify_runtime,
    has_rapids_conf_markers,
)
from spark_rapids_tools.tools.eventlog_detector.types import SparkRuntime


class TestEmptyProperties:
    """Test classification with an empty properties dict."""

    def test_empty_props_is_spark(self):
        assert classify_runtime({}) is SparkRuntime.SPARK


class TestRapidsConfigMarkers:
    """Test marker presence checks used by the CPU fast path."""

    @pytest.mark.parametrize(
        "props,expected",
        [
            ({"spark.master": "local"}, False),
            ({"spark.rapids.sql.enabled": "true"}, True),
            ({"spark.plugins": "com.nvidia.spark.SQLPlugin"}, True),
            ({"spark.plugins": "foo,com.nvidia.spark.SQLPlugin"}, True),
            ({
                "spark.plugins": "com.nvidia.spark.SQLPlugin",
                "spark.rapids.sql.enabled": "true",
            }, True),
            ({"spark.plugins": "foo.nvidia.spark.SQLPlugin"}, False),
        ],
    )
    def test_detects_rapids_marker_configs(self, props, expected):
        assert has_rapids_conf_markers(props) is expected


class TestSparkRapids:
    """Test SPARK_RAPIDS classification logic."""

    def test_plugin_and_default_enabled(self):
        props = {"spark.plugins": "foo,com.nvidia.spark.SQLPlugin,bar"}
        assert classify_runtime(props) is SparkRuntime.SPARK_RAPIDS

    def test_plugin_with_enabled_true(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": "true",
        }
        assert classify_runtime(props) is SparkRuntime.SPARK_RAPIDS

    def test_plugin_with_enabled_false_demotes_to_spark(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": "false",
        }
        assert classify_runtime(props) is SparkRuntime.SPARK

    def test_enabled_true_without_plugin_is_still_spark(self):
        props = {"spark.rapids.sql.enabled": "true"}
        assert classify_runtime(props) is SparkRuntime.SPARK

    @pytest.mark.parametrize("bogus_value", ["no", "0", "yes", "1", "", "maybe", "not-a-bool"])
    def test_non_toboolean_values_default_to_true_matching_scala(self, bogus_value):
        # Scala: Try { "no".toBoolean }.getOrElse(true) == true because
        # "no" is not parseable. The Python classifier must do the same.
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": bogus_value,
        }
        assert classify_runtime(props) is SparkRuntime.SPARK_RAPIDS
