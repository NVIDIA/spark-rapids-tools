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

"""Unit tests for ``eventlog_detector.resolver``."""
# pylint: disable=too-few-public-methods  # test classes naturally have few methods

from datetime import datetime
from pathlib import Path

import pytest

from spark_rapids_tools.storagelib import CspPath
from spark_rapids_tools.tools.eventlog_detector.resolver import (
    _parse_databricks_file_datetime,
    _resolve_event_log_files,
)
from spark_rapids_tools.tools.eventlog_detector.types import UnsupportedInputError


class TestDatabricksDateParse:
    """Test Databricks rolling log file date parsing."""

    def test_bare_eventlog_is_latest_sentinel(self):
        # Returns None; caller treats None as "sort last".
        assert _parse_databricks_file_datetime("eventlog") is None

    def test_dated_file_parses(self):
        dt = _parse_databricks_file_datetime("eventlog-2021-06-14--20-00.gz")
        assert dt == datetime(2021, 6, 14, 20, 0)

    def test_dated_file_without_codec_parses(self):
        dt = _parse_databricks_file_datetime("eventlog-2022-01-02--03-04")
        assert dt == datetime(2022, 1, 2, 3, 4)

    def test_non_eventlog_prefix_returns_none(self):
        assert _parse_databricks_file_datetime("application_1234.log") is None

    def test_out_of_range_components_return_none(self):
        # month=13 is syntactically 2 digits but not a valid month;
        # datetime() would raise ValueError, we want None instead.
        assert _parse_databricks_file_datetime("eventlog-2021-13-01--00-00") is None
        assert _parse_databricks_file_datetime("eventlog-2021-02-30--25-99") is None


class TestResolveSingleFile:
    """Test resolving a single event log file."""

    def test_single_file_returns_single_element_list(self, tmp_path: Path):
        f = tmp_path / "eventlog.zstd"
        f.write_bytes(b"x")
        source, files = _resolve_event_log_files(CspPath(str(f)))
        assert source == str(f)
        assert [p.base_name() for p in files] == ["eventlog.zstd"]


class TestResolveDatabricksRollingDir:
    """Test resolving a Databricks rolling event log directory."""

    def test_orders_earliest_first_and_bare_eventlog_last(self, tmp_path: Path):
        d = tmp_path / "dbrolling"
        d.mkdir()
        (d / "eventlog").write_bytes(b"")
        (d / "eventlog-2021-06-14--20-00.gz").write_bytes(b"")
        (d / "eventlog-2021-06-14--18-00.gz").write_bytes(b"")
        source, files = _resolve_event_log_files(CspPath(str(d)))
        assert source == str(d)
        names = [p.base_name() for p in files]
        # Earliest dated file first; bare `eventlog` sorts last (treated as
        # "current/latest" per Scala).
        assert names == [
            "eventlog-2021-06-14--18-00.gz",
            "eventlog-2021-06-14--20-00.gz",
            "eventlog",
        ]

    def test_dir_with_no_eventlog_prefix_raises(self, tmp_path: Path):
        d = tmp_path / "empty"
        d.mkdir()
        (d / "application_1.log").write_bytes(b"")
        with pytest.raises(UnsupportedInputError):
            _resolve_event_log_files(CspPath(str(d)))

    def test_empty_dir_raises(self, tmp_path: Path):
        d = tmp_path / "blank"
        d.mkdir()
        with pytest.raises(UnsupportedInputError):
            _resolve_event_log_files(CspPath(str(d)))


class TestResolveUnsupportedShapes:
    """Test that unsupported directory shapes raise UnsupportedInputError."""

    def test_spark_native_rolling_dir_raises(self, tmp_path: Path):
        d = tmp_path / "eventlog_v2_local-1623876083964"
        d.mkdir()
        (d / "events_1_local-1623876083964").write_bytes(b"")
        with pytest.raises(UnsupportedInputError):
            _resolve_event_log_files(CspPath(str(d)))

    def test_generic_multi_app_dir_raises(self, tmp_path: Path):
        d = tmp_path / "multi"
        d.mkdir()
        (d / "app-1.zstd").write_bytes(b"")
        (d / "app-2.zstd").write_bytes(b"")
        with pytest.raises(UnsupportedInputError):
            _resolve_event_log_files(CspPath(str(d)))
