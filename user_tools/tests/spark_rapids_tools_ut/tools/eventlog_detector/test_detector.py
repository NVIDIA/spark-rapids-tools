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

"""Integration tests for ``eventlog_detector.detect_spark_runtime``."""
# pylint: disable=too-few-public-methods  # test classes naturally have few methods

import json
from pathlib import Path

import pytest

from spark_rapids_tools.storagelib import CspPath
from spark_rapids_tools.tools.eventlog_detector import detect_spark_runtime
from spark_rapids_tools.tools.eventlog_detector.types import (
    Route,
    SparkRuntime,
    UnsupportedInputError,
)


def env_update(props: dict) -> dict:
    return {
        "Event": "SparkListenerEnvironmentUpdate",
        "Spark Properties": props,
        "System Properties": {},
        "Classpath Entries": {},
        "JVM Information": {},
    }


def _write_plain_log(path: Path, events: list) -> None:
    path.write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )


class TestAcceptsStringPath:
    """Test that detect_spark_runtime accepts plain string paths."""

    def test_str_input_resolves(self, tmp_path):
        log = tmp_path / "eventlog"
        _write_plain_log(
            log,
            [
                {"Event": "SparkListenerLogStart", "Spark Version": "3.5.1"},
                {"Event": "SparkListenerApplicationStart", "App ID": "a", "App Name": "A"},
                env_update({"spark.master": "local"}),
            ],
        )
        result = detect_spark_runtime(str(log))
        assert result.route is Route.QUALIFICATION
        assert result.spark_runtime is SparkRuntime.SPARK


class TestGpuLog:
    """Test detection on GPU event logs."""

    def test_env_update_with_plugin_classifies_as_profiling(self, tmp_path):
        log = tmp_path / "eventlog"
        _write_plain_log(
            log,
            [
                {"Event": "SparkListenerLogStart", "Spark Version": "3.5.1"},
                {"Event": "SparkListenerApplicationStart", "App ID": "g", "App Name": "G"},
                env_update({"spark.plugins": "com.nvidia.spark.SQLPlugin"}),
            ],
        )
        result = detect_spark_runtime(CspPath(str(log)))
        assert result.route is Route.PROFILING
        assert result.spark_runtime is SparkRuntime.SPARK_RAPIDS
        assert result.app_id == "g"
        assert result.spark_version == "3.5.1"


class TestCapHit:
    """Test detection when the event budget is exhausted before env-update."""

    def test_no_env_update_before_cap_is_unknown(self, tmp_path):
        log = tmp_path / "eventlog"
        # Many LogStart events, no env-update. Cap hits first.
        _write_plain_log(
            log,
            [{"Event": "SparkListenerLogStart", "Spark Version": "3.5.1"}] * 10,
        )
        result = detect_spark_runtime(str(log), max_events_scanned=5)
        assert result.route is Route.UNKNOWN
        assert result.spark_runtime is None
        reason = result.reason.lower()
        assert "no decisive signal" in reason or "no sparklistenerenvironmentupdate" in reason


class TestDatabricksRolling:
    """Test detection on Databricks rolling event log directories."""

    def test_gpu_in_later_rolled_file(self, tmp_path):
        d = tmp_path / "dbrolling"
        d.mkdir()
        _write_plain_log(
            d / "eventlog-2021-06-14--18-00",
            [
                {"Event": "SparkListenerLogStart", "Spark Version": "3.5.1"},
                {"Event": "SparkListenerApplicationStart", "App ID": "d", "App Name": "D"},
                env_update({"spark.master": "local"}),
            ],
        )
        _write_plain_log(
            d / "eventlog",
            [
                {
                    "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
                    "executionId": 0,
                    "description": "",
                    "details": "",
                    "physicalPlanDescription": "",
                    "sparkPlanInfo": {},
                    "time": 0,
                    "modifiedConfigs": {"spark.plugins": "com.nvidia.spark.SQLPlugin"},
                }
            ],
        )
        result = detect_spark_runtime(CspPath(str(d)))
        assert result.route is Route.PROFILING
        assert result.spark_runtime is SparkRuntime.SPARK_RAPIDS
        # The decisive signal came from the bare `eventlog` file (the latest,
        # sorted last after the dated chunk). event_log_path should reflect
        # that, not the first-opened file.
        assert result.event_log_path.endswith("/eventlog")


class TestUnsupportedInput:
    """Test that unsupported input shapes raise the expected error."""

    def test_spark_native_rolling_dir_raises(self, tmp_path):
        d = tmp_path / "eventlog_v2_local-1623876083964"
        d.mkdir()
        (d / "events_1_local-1623876083964").write_bytes(b"")
        with pytest.raises(UnsupportedInputError):
            detect_spark_runtime(CspPath(str(d)))


class TestReasonStrings:
    """Test the human-readable reason field on DetectionResult."""

    def test_reason_mentions_runtime_on_profiling(self, tmp_path):
        log = tmp_path / "eventlog"
        _write_plain_log(
            log,
            [
                env_update({"spark.plugins": "com.nvidia.spark.SQLPlugin"}),
            ],
        )
        result = detect_spark_runtime(str(log))
        assert "SPARK_RAPIDS" in result.reason

    def test_reason_mentions_full_log_on_qualification(self, tmp_path):
        log = tmp_path / "eventlog"
        _write_plain_log(log, [env_update({"spark.master": "local"})])
        result = detect_spark_runtime(str(log))
        assert result.route is Route.QUALIFICATION
        assert "walked full log" in result.reason.lower()


class TestSourcePathPreserved:
    """Test that source_path echoes the original input string."""

    def test_source_path_equals_input_string(self, tmp_path):
        log = tmp_path / "eventlog"
        _write_plain_log(log, [env_update({"spark.master": "local"})])
        input_str = str(log)
        result = detect_spark_runtime(input_str)
        assert result.source_path == input_str
