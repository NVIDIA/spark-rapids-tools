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

Opens the file via ``CspPath.open_input_stream()``, applies the right
decompression layer based on extension, wraps in a text decoder, and
yields an ``Iterator[str]``. On exit the context manager closes every
layer in reverse order. Streaming only — no buffering of the full file.

PyArrow coupling: ``CspPath.open_input_stream()`` delegates to PyArrow's
filesystem API, which auto-detects and decompresses ``.gz`` and ``.zst``
files transparently. ``.zstd`` is not recognised by PyArrow, so this
module decompresses it manually via ``zstandard``. If a future PyArrow
release changes its codec detection, this suffix mapping must be
re-verified.
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


# PyArrow's ``open_input_stream()`` transparently decompresses files whose
# extension matches a codec it recognises (including ``.gz`` and ``.zst``).
# For these we can read the already-decompressed byte stream directly.
_PYARROW_AUTO_DECOMP_SUFFIXES = {".gz", ".zst"}
# PyArrow does NOT recognise ``.zstd`` as a codec suffix, so the byte stream
# is raw compressed data that we must decompress ourselves.
_ZSTD_MANUAL_SUFFIXES = {".zstd"}
# Suffixes that unambiguously indicate a codec we do not support in V1.
_UNSUPPORTED_CODEC_SUFFIXES = {".lz4", ".lzf", ".snappy"}

def _classify_suffix(path: CspPath) -> str:
    name = path.base_name().lower()
    dot = name.rfind(".")
    if dot < 0:
        return ""
    return name[dot:]


@contextlib.contextmanager
def _open_event_log_stream(path: CspPath) -> Iterator[Iterator[str]]:
    suffix = _classify_suffix(path)
    if suffix in _UNSUPPORTED_CODEC_SUFFIXES:
        raise UnsupportedCompressionError(
            f"Compression codec '{suffix}' is not supported by the lightweight "
            "event log detector. Fall back to the full qualification/profiling "
            "pipeline for this log."
        )

    try:
        byte_stream = path.open_input_stream()
    except Exception as exc:
        raise EventLogReadError(f"Failed to open event log {path}: {exc}") from exc

    close_stack = contextlib.ExitStack()
    close_stack.callback(byte_stream.close)
    try:
        if suffix in _ZSTD_MANUAL_SUFFIXES:
            # PyArrow does not recognise ``.zstd``, so the byte stream holds
            # raw compressed frames — decompress them with the zstandard library.
            dctx = zstd.ZstdDecompressor()
            decompressed: io.RawIOBase = dctx.stream_reader(byte_stream)
            close_stack.callback(decompressed.close)
        else:
            # For ``.gz``, ``.zst``, ``.inprogress``, and unknown/plain
            # suffixes, PyArrow already handles decompression (or there is
            # nothing to decompress). Pass the byte stream straight through.
            # If the file is actually compressed with an unknown codec the
            # scanner will see garbled lines that don't parse as JSON; those
            # will be skipped and the caller will see Route.UNKNOWN — the
            # correct failure mode for this lightweight path.
            decompressed = byte_stream

        text = io.TextIOWrapper(decompressed, encoding="utf-8", errors="replace", newline="")
        close_stack.callback(text.close)

        def line_iter() -> Iterator[str]:
            for raw in text:
                # Strip the trailing newline to match the "one event per line"
                # contract. Empty lines are legal and skipped by the caller.
                yield raw.rstrip("\r\n")

        try:
            yield line_iter()
        except Exception as exc:
            # Convert any read-time I/O error into a typed domain error.
            raise EventLogReadError(f"Error reading event log {path}: {exc}") from exc
    finally:
        close_stack.close()
