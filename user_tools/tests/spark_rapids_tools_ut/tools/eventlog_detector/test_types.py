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

"""Unit tests for ``eventlog_detector.types``."""
# pylint: disable=too-few-public-methods  # test classes naturally have few methods

from spark_rapids_tools.tools.eventlog_detector.types import (
    EventLogDetectionError,
    EventLogReadError,
    SparkRuntime,
    ToolExecution,
    UnsupportedCompressionError,
    UnsupportedInputError,
)


class TestToolExecution:
    """Test the ToolExecution string enum."""

    def test_has_expected_values(self):
        assert {r.value for r in ToolExecution} == {
            "QUALIFICATION",
            "PROFILING",
            "UNKNOWN",
        }

    def test_is_string_enum(self):
        # str subclass means aether can compare against plain strings.
        assert ToolExecution.PROFILING == "PROFILING"


class TestSparkRuntime:
    """Test the reduced SparkRuntime string enum."""

    def test_values_cover_spark_and_rapids_only(self):
        assert {r.value for r in SparkRuntime} == {
            "SPARK",
            "SPARK_RAPIDS",
        }

    def test_is_string_enum(self):
        assert SparkRuntime.SPARK_RAPIDS == "SPARK_RAPIDS"


class TestExceptionHierarchy:
    """Test that all detector exceptions form a coherent hierarchy."""

    def test_all_errors_subclass_base(self):
        for cls in (
            UnsupportedInputError,
            UnsupportedCompressionError,
            EventLogReadError,
        ):
            assert issubclass(cls, EventLogDetectionError)
