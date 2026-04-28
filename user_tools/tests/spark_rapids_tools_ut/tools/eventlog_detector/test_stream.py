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

"""Unit tests for ``eventlog_detector.stream``."""
# pylint: disable=too-few-public-methods  # test classes naturally have few methods

import gzip
from pathlib import Path

import pytest
import zstandard as zstd

from spark_rapids_tools.storagelib import CspPath
from spark_rapids_tools.tools.eventlog_detector.stream import open_event_log_stream
from spark_rapids_tools.tools.eventlog_detector.types import (
    EventLogReadError,
    UnsupportedCompressionError,
)


SAMPLE_LINES = [
    '{"Event":"SparkListenerLogStart","Spark Version":"3.5.1"}',
    '{"Event":"SparkListenerApplicationStart","App ID":"app-1"}',
    '{"Event":"SparkListenerEnvironmentUpdate","Spark Properties":{}}',
]


def _write_plain(path: Path) -> None:
    path.write_text("\n".join(SAMPLE_LINES) + "\n", encoding="utf-8")


def _write_gz(path: Path) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("\n".join(SAMPLE_LINES) + "\n")


def _write_zstd(path: Path) -> None:
    cctx = zstd.ZstdCompressor()
    raw = ("\n".join(SAMPLE_LINES) + "\n").encode("utf-8")
    path.write_bytes(cctx.compress(raw))


@pytest.fixture
def plain_file(tmp_path: Path) -> CspPath:
    p = tmp_path / "eventlog.inprogress"
    _write_plain(p)
    return CspPath(str(p))


@pytest.fixture
def gz_file(tmp_path: Path) -> CspPath:
    p = tmp_path / "eventlog.gz"
    _write_gz(p)
    return CspPath(str(p))


@pytest.fixture
def zstd_file(tmp_path: Path) -> CspPath:
    p = tmp_path / "eventlog.zstd"
    _write_zstd(p)
    return CspPath(str(p))


class TestPlainStream:
    """Test streaming plain-text event logs."""

    def test_yields_all_lines(self, plain_file):  # pylint: disable=redefined-outer-name
        with open_event_log_stream(plain_file) as lines:
            collected = list(lines)
        assert collected == SAMPLE_LINES


class TestGzipStream:
    """Test streaming gzip-compressed event logs."""

    def test_yields_all_lines(self, gz_file):  # pylint: disable=redefined-outer-name
        with open_event_log_stream(gz_file) as lines:
            collected = list(lines)
        assert collected == SAMPLE_LINES


class TestZstdStream:
    """Test streaming zstd-compressed event logs."""

    def test_yields_all_lines(self, zstd_file):  # pylint: disable=redefined-outer-name
        with open_event_log_stream(zstd_file) as lines:
            collected = list(lines)
        assert collected == SAMPLE_LINES

    def test_zst_short_suffix_also_works(self, tmp_path):
        p = tmp_path / "eventlog.zst"
        _write_zstd(p)
        with open_event_log_stream(CspPath(str(p))) as lines:
            collected = list(lines)
        assert collected == SAMPLE_LINES


class TestUnsupportedCompression:
    """Test that unsupported compression formats raise UnsupportedCompressionError."""

    @pytest.mark.parametrize("suffix", [".lz4", ".snappy", ".lzf", ".weirdcodec"])
    def test_unsupported_suffix_raises(self, tmp_path, suffix):
        p = tmp_path / f"eventlog{suffix}"
        p.write_bytes(b"some-bytes")
        with pytest.raises(UnsupportedCompressionError):
            with open_event_log_stream(CspPath(str(p))) as _:
                pass


class TestIoFailure:
    """Test that I/O errors raise EventLogReadError."""

    def test_missing_file_raises_read_error(self, tmp_path):
        p = tmp_path / "does-not-exist"
        with pytest.raises(EventLogReadError):
            with open_event_log_stream(CspPath(str(p))) as lines:
                next(iter(lines))

    def test_caller_side_exception_is_not_reclassified(self, plain_file):  # pylint: disable=redefined-outer-name
        # Caller-raised exceptions must propagate untouched, not be
        # reclassified as EventLogReadError.
        class _MarkerError(RuntimeError):
            pass

        with pytest.raises(_MarkerError):
            with open_event_log_stream(plain_file):
                raise _MarkerError("not an I/O failure")
