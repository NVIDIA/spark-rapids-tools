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

"""Input-path resolver for single files and Apache Spark rolling event logs."""

import re
from typing import List, Optional, Tuple

from spark_rapids_tools.storagelib import CspFs, CspPath
from spark_rapids_tools.tools.eventlog_detector import markers as m
from spark_rapids_tools.tools.eventlog_detector.types import UnsupportedInputError


_OSS_EVENT_FILE_PATTERN = re.compile(r"^events_(\d+)_.*")


def parse_oss_event_file_index(name: str) -> Optional[int]:
    """Return the numeric chunk index from ``events_<n>_...`` files."""
    match = _OSS_EVENT_FILE_PATTERN.match(name)
    if match is None:
        return None
    return int(match.group(1))


def _is_oss_event_log_file(path: CspPath) -> bool:
    return parse_oss_event_file_index(path.base_name()) is not None


def _base_name_from_source(source: str) -> str:
    """Return the final path component, ignoring trailing separators."""
    return source.rstrip("/").rsplit("/", 1)[-1]


def resolve_event_log_files(path: CspPath) -> Tuple[str, List[CspPath]]:
    """Resolve ``path`` to an ordered list of files to scan.

    Supported inputs are a single concrete file or an Apache Spark rolling
    event-log directory named ``eventlog_v2_*``. Other directory layouts are
    rejected so callers can use the full tools pipeline.
    """
    source = path.no_scheme

    if path.is_file():
        return source, [path]

    if not path.is_dir():
        raise UnsupportedInputError(
            f"Path is neither a file nor a directory: {source}"
        )

    if not _base_name_from_source(source).startswith(m.OSS_EVENT_LOG_DIR_PREFIX):
        raise UnsupportedInputError(
            f"Directory {source} is not a supported input shape. Only single "
            f"files and Apache Spark rolling event-log directories named "
            f"{m.OSS_EVENT_LOG_DIR_PREFIX}* are handled "
            "here; use the full pipeline for other shapes."
        )

    event_files = [c for c in CspFs.list_all_files(path) if _is_oss_event_log_file(c)]
    if not event_files:
        raise UnsupportedInputError(f"Directory {source} does not contain Spark event chunks")

    event_files.sort(
        key=lambda f: (
            parse_oss_event_file_index(f.base_name()) or 0,
            f.base_name(),
        )
    )
    return source, event_files
