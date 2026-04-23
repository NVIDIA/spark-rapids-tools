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

"""Codec-aware context-managed line streamer for Spark event logs.

Opens the file through ``CspPath.open_input_stream()``, wraps it with
the right decompression and text layers, and yields an
``Iterator[str]``. Streaming only — the full file is never buffered.

``CspPath.open_input_stream()`` delegates to PyArrow, which auto-detects
and decompresses ``.gz`` and ``.zst`` transparently. ``.zstd`` is not
recognised by PyArrow, so we decompress it manually via ``zstandard``.
Revisit this mapping if the upstream codec detection changes.
"""

import contextlib
import io
from typing import Iterator

import zstandard as zstd

from spark_rapids_tools.storagelib import CspPath
from spark_rapids_tools.tools.eventlog_detector.types import (
    EventLogReadError,
    UnsupportedCompressionError,
)


# Suffixes PyArrow already decompresses for us.
_PYARROW_AUTO_DECOMP_SUFFIXES = {".gz", ".zst"}
# Suffixes we must decompress manually via zstandard.
_ZSTD_MANUAL_SUFFIXES = {".zstd"}
# Suffixes treated as plain text.
_PLAIN_SUFFIXES = {"", ".inprogress"}
# Whitelist of accepted suffixes; anything else raises
# ``UnsupportedCompressionError``.
_SUPPORTED_SUFFIXES = (
    _PYARROW_AUTO_DECOMP_SUFFIXES | _ZSTD_MANUAL_SUFFIXES | _PLAIN_SUFFIXES
)


def _classify_suffix(path: CspPath) -> str:
    name = path.base_name().lower()
    dot = name.rfind(".")
    if dot < 0:
        return ""
    return name[dot:]


@contextlib.contextmanager
def _open_event_log_stream(path: CspPath) -> Iterator[Iterator[str]]:
    suffix = _classify_suffix(path)
    if suffix not in _SUPPORTED_SUFFIXES:
        raise UnsupportedCompressionError(
            f"File suffix '{suffix}' is not supported. "
            "Supported: plain, .inprogress, .gz, .zstd, .zst."
        )

    try:
        byte_stream = path.open_input_stream()
    except Exception as exc:
        raise EventLogReadError(f"Failed to open event log {path}: {exc}") from exc

    close_stack = contextlib.ExitStack()
    close_stack.callback(byte_stream.close)
    try:
        if suffix in _ZSTD_MANUAL_SUFFIXES:
            # Decompress ``.zstd`` ourselves; PyArrow does not handle it.
            dctx = zstd.ZstdDecompressor()
            decompressed: io.RawIOBase = dctx.stream_reader(byte_stream)
            close_stack.callback(decompressed.close)
        else:
            # Plain text, or already decompressed by PyArrow.
            decompressed = byte_stream

        text = io.TextIOWrapper(decompressed, encoding="utf-8", errors="replace", newline="")
        close_stack.callback(text.close)

        def line_iter() -> Iterator[str]:
            # One event per line; strip the trailing newline and leave
            # empty lines for the caller to skip.
            for raw in text:
                yield raw.rstrip("\r\n")

        try:
            yield line_iter()
        except Exception as exc:
            raise EventLogReadError(f"Error reading event log {path}: {exc}") from exc
    finally:
        close_stack.close()
