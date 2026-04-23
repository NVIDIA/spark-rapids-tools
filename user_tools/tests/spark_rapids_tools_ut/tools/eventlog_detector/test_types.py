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

import pytest

from spark_rapids_tools.tools.eventlog_detector.types import (
    DetectionResult,
    EventLogDetectionError,
    EventLogReadError,
    Route,
    SparkRuntime,
    Termination,
    UnsupportedCompressionError,
    UnsupportedInputError,
)


class TestRoute:
    def test_has_three_values(self):
        assert {r.value for r in Route} == {"QUALIFICATION", "PROFILING", "UNKNOWN"}

    def test_is_string_enum(self):
        # str subclass means aether can compare against plain strings.
        assert Route.PROFILING == "PROFILING"


class TestSparkRuntime:
    def test_values_match_scala_enum_exactly(self):
        # These strings match org.apache.spark.sql.rapids.tool.util.SparkRuntime
        # which aether already persists in JobRun.spark_runtime.
        assert {r.value for r in SparkRuntime} == {
            "SPARK",
            "SPARK_RAPIDS",
            "PHOTON",
            "AURON",
        }

    def test_is_string_enum(self):
        assert SparkRuntime.SPARK_RAPIDS == "SPARK_RAPIDS"


class TestTermination:
    def test_has_three_modes(self):
        assert {t.name for t in Termination} == {"DECISIVE", "EXHAUSTED", "CAP_HIT"}


class TestDetectionResult:
    def test_frozen_dataclass(self):
        result = DetectionResult(
            route=Route.PROFILING,
            spark_runtime=SparkRuntime.SPARK_RAPIDS,
            app_id="app-1",
            spark_version="3.5.1",
            event_log_path="/tmp/x",
            source_path="/tmp/x",
            reason="decisive: classified as SPARK_RAPIDS",
        )
        # Python raises FrozenInstanceError (a subclass of AttributeError)
        # when you try to assign to a field on a frozen dataclass.
        with pytest.raises(AttributeError):
            result.route = Route.UNKNOWN  # type: ignore[misc]

    def test_structural_equality(self):
        kwargs = dict(
            route=Route.QUALIFICATION,
            spark_runtime=SparkRuntime.SPARK,
            app_id="a",
            spark_version="3.5.1",
            event_log_path="/tmp/a",
            source_path="/tmp/a",
            reason="walked full log, no GPU-family signal",
        )
        assert DetectionResult(**kwargs) == DetectionResult(**kwargs)
        assert hash(DetectionResult(**kwargs)) == hash(DetectionResult(**kwargs))
        # Distinct payloads compare unequal.
        other = DetectionResult(**{**kwargs, "app_id": "b"})
        assert DetectionResult(**kwargs) != other

    def test_accepts_optional_fields_as_none(self):
        result = DetectionResult(
            route=Route.UNKNOWN,
            spark_runtime=None,
            app_id=None,
            spark_version=None,
            event_log_path="/tmp/x",
            source_path="/tmp/x",
            reason="no decisive signal within bounded scan",
        )
        assert result.route is Route.UNKNOWN
        assert result.spark_runtime is None


class TestExceptionHierarchy:
    def test_all_errors_subclass_base(self):
        for cls in (
            UnsupportedInputError,
            UnsupportedCompressionError,
            EventLogReadError,
        ):
            assert issubclass(cls, EventLogDetectionError)

    def test_base_is_exception_not_value_error(self):
        # We deliberately do NOT inherit ValueError: EventLogReadError wraps
        # I/O failures (missing file, permissions), which are not bad-input
        # errors. Keeping the base at Exception avoids that semantic mismatch.
        assert issubclass(EventLogDetectionError, Exception)
        assert not issubclass(EventLogDetectionError, ValueError)
