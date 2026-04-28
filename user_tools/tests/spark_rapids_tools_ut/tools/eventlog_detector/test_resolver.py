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

from pathlib import Path

import pytest

from spark_rapids_tools.storagelib import CspPath
from spark_rapids_tools.tools.eventlog_detector.resolver import (
    parse_oss_event_file_index,
    resolve_event_log_files,
)
from spark_rapids_tools.tools.eventlog_detector.types import UnsupportedInputError


class TestOssEventFileIndex:
    """Test Apache Spark rolling event file index parsing."""

    def test_events_file_index_parses(self):
        assert parse_oss_event_file_index("events_1_app-1.zstd") == 1
        assert parse_oss_event_file_index("events_10_app-1") == 10

    def test_non_events_file_returns_none(self):
        assert parse_oss_event_file_index("appstatus_app-1.inprogress") is None
        assert parse_oss_event_file_index("eventlog") is None


class TestResolveSingleFile:
    """Test resolving a single event log file."""

    def test_single_file_returns_single_element_list(self, tmp_path: Path):
        f = tmp_path / "eventlog.zstd"
        f.write_bytes(b"x")
        source, files = resolve_event_log_files(CspPath(str(f)))
        assert source == str(f)
        assert [p.base_name() for p in files] == ["eventlog.zstd"]


class TestResolveOssRollingDir:
    """Test resolving an Apache Spark rolling event-log directory."""

    def test_orders_event_chunks_by_numeric_index(self, tmp_path: Path):
        d = tmp_path / "eventlog_v2_app-1"
        d.mkdir()
        (d / "events_10_app-1.zstd").write_bytes(b"")
        (d / "events_2_app-1.zstd").write_bytes(b"")
        (d / "events_1_app-1.zstd").write_bytes(b"")
        (d / "appstatus_app-1.inprogress").write_bytes(b"")
        source, files = resolve_event_log_files(CspPath(str(d)))
        assert source == str(d)
        assert [p.base_name() for p in files] == [
            "events_1_app-1.zstd",
            "events_2_app-1.zstd",
            "events_10_app-1.zstd",
        ]

    def test_accepts_trailing_slash_on_rolling_dir(self, tmp_path: Path):
        d = tmp_path / "eventlog_v2_app-1"
        d.mkdir()
        (d / "events_1_app-1.zstd").write_bytes(b"")
        source, files = resolve_event_log_files(CspPath(f"{d}/"))
        assert source.rstrip("/") == str(d)
        assert [p.base_name() for p in files] == ["events_1_app-1.zstd"]

    def test_empty_oss_rolling_dir_raises(self, tmp_path: Path):
        d = tmp_path / "eventlog_v2_app-1"
        d.mkdir()
        (d / "appstatus_app-1.inprogress").write_bytes(b"")
        with pytest.raises(UnsupportedInputError):
            resolve_event_log_files(CspPath(str(d)))


class TestResolveUnsupportedShapes:
    """Test that unsupported directory shapes raise UnsupportedInputError."""

    def test_non_oss_rolling_dir_raises(self, tmp_path: Path):
        d = tmp_path / "non_oss_rolling"
        d.mkdir()
        (d / "eventlog-2021-06-14--18-00.gz").write_bytes(b"")
        (d / "eventlog").write_bytes(b"")
        with pytest.raises(UnsupportedInputError, match="eventlog_v2_\\*"):
            resolve_event_log_files(CspPath(str(d)))

    def test_generic_multi_app_dir_raises(self, tmp_path: Path):
        d = tmp_path / "multi"
        d.mkdir()
        (d / "app-1.zstd").write_bytes(b"")
        (d / "app-2.zstd").write_bytes(b"")
        with pytest.raises(UnsupportedInputError):
            resolve_event_log_files(CspPath(str(d)))
