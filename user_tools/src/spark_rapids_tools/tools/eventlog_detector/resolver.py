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

"""Input-path resolution for the event log detector.

Resolves one user-supplied path into a list of one or more concrete files
to read, in scan order. Supports a single file or a Databricks rolling
directory. Spark-native rolling, generic multi-app directories, wildcards,
and comma lists raise ``UnsupportedInputError`` and are expected to fall
back to the full Scala pipeline.
"""

import re
from datetime import datetime
from typing import List, Optional, Tuple

from spark_rapids_tools.storagelib import CspFs, CspPath
from spark_rapids_tools.tools.eventlog_detector import markers as m
from spark_rapids_tools.tools.eventlog_detector.types import UnsupportedInputError


_DB_DATE_PATTERN = re.compile(m.DB_EVENT_LOG_DATE_REGEX)


def _parse_databricks_file_datetime(name: str) -> Optional[datetime]:
    """Parse ``eventlog-YYYY-MM-DD--HH-MM[.codec]`` to a datetime.

    Returns ``None`` for bare ``eventlog`` and any name that does not match
    the dated pattern. The caller sorts ``None`` as "latest/current" to
    mirror Scala's ``getDBEventLogFileDate`` which defaults to ``now()``.
    """
    if not name.startswith(m.DB_EVENT_LOG_FILE_PREFIX):
        return None
    match = _DB_DATE_PATTERN.match(name)
    if match is None:
        return None
    year, month, day, hour, minute = (int(g) for g in match.groups())
    return datetime(year, month, day, hour, minute)


def _is_databricks_event_log_filename(name: str) -> bool:
    return name.startswith(m.DB_EVENT_LOG_FILE_PREFIX)


def _resolve_event_log_files(path: CspPath) -> Tuple[str, List[CspPath]]:
    """Resolve ``path`` to an ordered list of files to scan.

    Returns ``(source, files)`` where ``source`` is the original input
    rendered as a string (preserved for the ``DetectionResult``) and
    ``files`` is the scan order.
    """
    source = path.no_scheme

    if path.is_file():
        return source, [path]

    if not path.is_dir():
        raise UnsupportedInputError(
            f"Path is neither a file nor a directory: {source}"
        )

    # Directory: must be a Databricks rolling dir. Spark-native rolling
    # (eventlog_v2_*) and generic multi-app directories are out of scope.
    children = CspFs.list_all_files(path)
    db_files = [c for c in children if _is_databricks_event_log_filename(c.base_name())]
    if not db_files:
        raise UnsupportedInputError(
            f"Directory {source} is not a supported input shape. The detector "
            "handles single files or Databricks rolling directories only; fall "
            "back to the full pipeline for Spark-native rolling, multi-app "
            "directories, wildcards, or comma-separated inputs."
        )

    # Sort mirroring DatabricksRollingEventLogFilesFileReader: dated files
    # ascending by parsed datetime, bare `eventlog` last (treated as
    # "latest/current"). Stable sort on filename first to keep ordering
    # deterministic among equal-date files (extremely unlikely in practice
    # but cheap insurance for tests).
    db_files.sort(key=lambda f: f.base_name())
    db_files.sort(
        key=lambda f: (
            _parse_databricks_file_datetime(f.base_name()) or datetime.max,
        )
    )
    return source, db_files
