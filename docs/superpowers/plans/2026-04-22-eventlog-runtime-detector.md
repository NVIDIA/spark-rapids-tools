# Event Log Runtime Detector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a lightweight Python function `detect_spark_runtime(path)` that returns a `Route` (`QUALIFICATION` / `PROFILING` / `UNKNOWN`) and best-effort runtime metadata by scanning a bounded prefix of a Spark event log — single file or Databricks rolling directory — without invoking the Scala tools pipeline.

**Architecture:** Four cohesive units inside a single module under `user_tools/src/spark_rapids_tools/tools/`: path resolver, stream opener (context manager, codec-aware), bounded event scanner (streaming only, no accumulation), pure-function classifier. A single-source-of-truth markers file pins the key strings/regexes to Scala source references. Public surface is the function plus three dataclasses/enums (`Route`, `SparkRuntime`, `DetectionResult`) plus a small exception hierarchy. No CLI. No output files.

**Tech Stack:** Python 3.10+ (matches `user_tools/pyproject.toml`), stdlib (`gzip`, `json`, `io`, `re`, `contextlib`, `dataclasses`, `enum`, `pathlib`, `datetime`), `zstandard` (new hard dep), `spark_rapids_tools.storagelib.csppath.CspPath` + `spark_rapids_tools.storagelib.cspfs.CspFs` for cloud/local I/O, `pytest` for tests.

**Spec:** `docs/superpowers/specs/2026-04-22-eventlog-runtime-detector-design.md`

---

## File Structure

**New files (all under `user_tools/`):**

- `src/spark_rapids_tools/tools/eventlog_detector/__init__.py` — re-exports public API.
- `src/spark_rapids_tools/tools/eventlog_detector/markers.py` — single source of truth for property keys, regexes, substrings. Each constant carries a `# Scala source: <file>:<line>` comment.
- `src/spark_rapids_tools/tools/eventlog_detector/types.py` — `Route`, `SparkRuntime`, `DetectionResult`, `Termination` enum, exception hierarchy.
- `src/spark_rapids_tools/tools/eventlog_detector/resolver.py` — `_resolve_event_log_files(path) -> tuple[str, list[CspPath]]`.
- `src/spark_rapids_tools/tools/eventlog_detector/stream.py` — `_open_event_log_stream(path)` context manager yielding `Iterator[str]`.
- `src/spark_rapids_tools/tools/eventlog_detector/scanner.py` — `_scan_events(lines, budget) -> _ScanResult` and `_scan_events_across(paths, budget) -> _ScanResult`.
- `src/spark_rapids_tools/tools/eventlog_detector/classifier.py` — `_classify_runtime(spark_properties) -> SparkRuntime`.
- `src/spark_rapids_tools/tools/eventlog_detector/detector.py` — top-level `detect_spark_runtime()` that ties everything together.

**New tests (under `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/`):**

- `__init__.py`
- `conftest.py` — shared fixtures (synthesized event log files in `tmp_path`).
- `test_markers.py`
- `test_types.py`
- `test_classifier.py`
- `test_resolver.py`
- `test_stream.py`
- `test_scanner.py`
- `test_detector.py` — integration tests covering the public API end-to-end.
- `test_detector_fixtures.py` — runs the detector against the existing `core/src/test/resources/spark-events-*` fixtures with expected `Route` labels.

**Modified files:**

- `user_tools/pyproject.toml` — add `zstandard` to `dependencies`.
- `user_tools/src/spark_rapids_tools/tools/__init__.py` — add re-export line so `from spark_rapids_tools.tools import detect_spark_runtime` works (if the existing `__init__.py` follows that pattern — verify in Task 2).

**Rationale for splitting into many small files:** the spec defines four independently testable components plus a shared types/markers layer. Putting each in its own file keeps files small, lets each test file focus on one component, and makes reviewer diffs surgical. This matches existing layout in `user_tools/src/spark_rapids_tools/tools/` (several single-purpose files there already).

---

## Task 1: Add `zstandard` dependency

**Files:**

- Modify: `user_tools/pyproject.toml`

- [ ] **Step 1: Read current dependencies block**

Run: `sed -n '/^dependencies = \[/,/^\]/p' user_tools/pyproject.toml`
Expected: prints the current `dependencies = [ ... ]` block. `zstandard` must not be present.

- [ ] **Step 2: Add `zstandard` to dependencies**

Use Edit to insert a new line after the existing `"pyYAML>=6.0.2",` line (or similar stable anchor — whichever comes last alphabetically near the Z range). Add:

```
    # Decompresses Spark event logs written with --conf spark.eventLog.compress=true
    # spark.io.compression.codec=zstd (the common default). Used by
    # spark_rapids_tools.tools.eventlog_detector.
    "zstandard>=0.22.0",
```

The exact Edit call (using a unique anchor from the current file — pick the existing `# used for retrieving available memory on the host` comment block, which precedes `"psutil==7.0.0"`):

```python
Edit(
  file_path="user_tools/pyproject.toml",
  old_string='    # used for retrieving available memory on the host\n    "psutil==7.0.0",',
  new_string='    # used for retrieving available memory on the host\n    "psutil==7.0.0",\n    # Decompresses Spark event logs with zstd codec. Used by\n    # spark_rapids_tools.tools.eventlog_detector.\n    "zstandard>=0.22.0",'
)
```

- [ ] **Step 3: Install the updated package locally**

Run: `pip install -e user_tools/`
Expected: `zstandard-<version>` appears in the output; install completes successfully.

- [ ] **Step 4: Verify `zstandard` is importable**

Run: `python -c "import zstandard; print(zstandard.__version__)"`
Expected: prints a version string, no `ModuleNotFoundError`.

- [ ] **Step 5: Commit**

```bash
git add user_tools/pyproject.toml
git commit -m "build(user_tools): add zstandard dep for event log detector"
```

---

## Task 2: Create module skeleton and `__init__.py`

**Files:**

- Create: `user_tools/src/spark_rapids_tools/tools/eventlog_detector/__init__.py`
- Read (do not modify yet): `user_tools/src/spark_rapids_tools/tools/__init__.py`

- [ ] **Step 1: Read the existing `tools/__init__.py` to learn the re-export convention**

Run: `cat user_tools/src/spark_rapids_tools/tools/__init__.py`
Record: whether the file has explicit re-exports, `__all__`, or is empty. This determines whether Task 9 needs to append anything there.

- [ ] **Step 2: Create the package skeleton**

Create `user_tools/src/spark_rapids_tools/tools/eventlog_detector/__init__.py`:

```python
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

"""Lightweight event log runtime detector.

Public entry point: ``detect_spark_runtime``. Returns a ``DetectionResult``
carrying a routing decision (``QUALIFICATION`` / ``PROFILING`` / ``UNKNOWN``)
and best-effort metadata, by scanning a bounded prefix of a Spark event log.

See docs/superpowers/specs/2026-04-22-eventlog-runtime-detector-design.md
for the full contract and the Scala sources this mirrors.
"""

from spark_rapids_tools.tools.eventlog_detector.detector import detect_spark_runtime
from spark_rapids_tools.tools.eventlog_detector.types import (
    DetectionResult,
    EventLogDetectionError,
    EventLogReadError,
    Route,
    SparkRuntime,
    UnsupportedCompressionError,
    UnsupportedInputError,
)

__all__ = [
    "DetectionResult",
    "EventLogDetectionError",
    "EventLogReadError",
    "Route",
    "SparkRuntime",
    "UnsupportedCompressionError",
    "UnsupportedInputError",
    "detect_spark_runtime",
]
```

This will `ImportError` until the submodules exist. That's fine; subsequent tasks fill them in. We create it first so each later task's "does it import?" smoke test covers the integration path.

- [ ] **Step 3: Create empty test package**

Create `user_tools/tests/spark_rapids_tools_ut/tools/__init__.py` (empty) and `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/__init__.py` (empty). First check whether `tools/__init__.py` already exists:

Run: `test -f user_tools/tests/spark_rapids_tools_ut/tools/__init__.py && echo EXISTS || echo MISSING`

If `MISSING`, create it with the Apache 2 header as a docstring module:

```python
# Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Tests for ``spark_rapids_tools.tools``."""
```

Then create `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/__init__.py` with the same header.

- [ ] **Step 4: Commit**

```bash
git add user_tools/src/spark_rapids_tools/tools/eventlog_detector/__init__.py \
        user_tools/tests/spark_rapids_tools_ut/tools/__init__.py \
        user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/__init__.py
git commit -m "feat(eventlog_detector): package skeleton and public re-exports"
```

Note: the package re-exports will fail to resolve until Task 3/9, but the commit is self-contained as a new empty package. Do not run the imports yet.

---

## Task 3: Types — `Route`, `SparkRuntime`, `DetectionResult`, exceptions

**Files:**

- Create: `user_tools/src/spark_rapids_tools/tools/eventlog_detector/types.py`
- Test: `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_types.py`

- [ ] **Step 1: Write the failing test**

Create `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_types.py`:

```python
# Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
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
        with pytest.raises((AttributeError, Exception)):
            result.route = Route.UNKNOWN  # type: ignore[misc]

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

    def test_base_is_value_error(self):
        # Using ValueError as the common ancestor so callers who catch
        # ValueError (a reasonable default for bad input) still see these.
        assert issubclass(EventLogDetectionError, ValueError)
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_types.py -v`
Expected: `ModuleNotFoundError: No module named 'spark_rapids_tools.tools.eventlog_detector.types'`.

- [ ] **Step 3: Implement `types.py`**

Create `user_tools/src/spark_rapids_tools/tools/eventlog_detector/types.py`:

```python
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

"""Types, enums, and exceptions for the event log runtime detector."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Route(str, Enum):
    """Routing decision returned to the caller."""

    QUALIFICATION = "QUALIFICATION"
    PROFILING = "PROFILING"
    UNKNOWN = "UNKNOWN"


class SparkRuntime(str, Enum):
    """Runtime taxonomy.

    Values match ``org.apache.spark.sql.rapids.tool.util.SparkRuntime`` in the
    Scala core so aether's ``JobRun.spark_runtime`` column stays compatible.
    """

    SPARK = "SPARK"
    SPARK_RAPIDS = "SPARK_RAPIDS"
    PHOTON = "PHOTON"
    AURON = "AURON"


class Termination(Enum):
    """How the scanner stopped. Used internally by the detector flow."""

    DECISIVE = "DECISIVE"      # classification returned non-SPARK
    EXHAUSTED = "EXHAUSTED"    # walked every file to EOF under the budget
    CAP_HIT = "CAP_HIT"        # hit max_events_scanned before exhausting files


@dataclass(frozen=True)
class DetectionResult:
    """Result returned by ``detect_spark_runtime``.

    ``spark_runtime`` is best-effort metadata. ``None`` is valid (e.g., when
    ``route`` is ``UNKNOWN`` because env-update was never seen).
    """

    route: Route
    spark_runtime: Optional[SparkRuntime]
    app_id: Optional[str]
    spark_version: Optional[str]
    event_log_path: str
    source_path: str
    reason: str


class EventLogDetectionError(ValueError):
    """Base class for detector errors."""


class UnsupportedInputError(EventLogDetectionError):
    """Input shape is outside V1 scope (multi-app dir, wildcard, comma list, ...)."""


class UnsupportedCompressionError(EventLogDetectionError):
    """File uses a compression codec the V1 detector does not handle."""


class EventLogReadError(EventLogDetectionError):
    """Wraps an underlying I/O failure when reading the event log."""
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_types.py -v`
Expected: all 7 test cases pass.

- [ ] **Step 5: Commit**

```bash
git add user_tools/src/spark_rapids_tools/tools/eventlog_detector/types.py \
        user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_types.py
git commit -m "feat(eventlog_detector): routing types and exception hierarchy"
```

---

## Task 4: Markers module (single source of truth pinned to Scala)

**Files:**

- Create: `user_tools/src/spark_rapids_tools/tools/eventlog_detector/markers.py`
- Test: `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_markers.py`

- [ ] **Step 1: Write the failing test**

Create `test_markers.py`:

```python
# Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``eventlog_detector.markers``."""

import re

from spark_rapids_tools.tools.eventlog_detector import markers as m


class TestGpuMarkers:
    def test_plugin_substring_exact(self):
        assert m.GPU_PLUGIN_CLASS_SUBSTRING == "com.nvidia.spark.SQLPlugin"

    def test_gpu_toggle_key_exact(self):
        assert m.GPU_ENABLED_KEY == "spark.rapids.sql.enabled"


class TestAuronMarkers:
    def test_extension_regex_fullmatches_expected_value(self):
        # Mirrors AuronParseHelper.extensionRegxMap.
        pat = re.compile(m.AURON_EXTENSION_REGEX)
        assert pat.fullmatch("org.apache.spark.sql.AuronSparkSessionExtension")
        assert pat.fullmatch("whatever.AuronSparkSessionExtension.more")
        assert not pat.fullmatch("org.apache.spark.sql.SomeOtherExtension")

    def test_auron_enabled_defaults_to_true(self):
        assert m.AURON_ENABLED_DEFAULT == "true"
        assert m.AURON_ENABLED_KEY == "spark.auron.enabled"
        assert m.AURON_SPARK_EXTENSIONS_KEY == "spark.sql.extensions"


class TestDatabricksPrecondition:
    def test_all_three_tag_keys_present(self):
        assert m.DB_PRECONDITION_KEYS == (
            "spark.databricks.clusterUsageTags.clusterAllTags",
            "spark.databricks.clusterUsageTags.clusterId",
            "spark.databricks.clusterUsageTags.clusterName",
        )


class TestPhotonMarkers:
    def test_marker_map_fullmatches_expected(self):
        pats = {k: re.compile(v) for k, v in m.PHOTON_MARKER_REGEX.items()}
        assert pats[
            "spark.databricks.clusterUsageTags.sparkVersion"
        ].fullmatch("11.3.x-photon-scala2.12")
        assert pats[
            "spark.databricks.clusterUsageTags.runtimeEngine"
        ].fullmatch("PHOTON")
        assert not pats[
            "spark.databricks.clusterUsageTags.runtimeEngine"
        ].fullmatch("STANDARD")

    def test_all_four_photon_keys(self):
        assert set(m.PHOTON_MARKER_REGEX) == {
            "spark.databricks.clusterUsageTags.sparkVersion",
            "spark.databricks.clusterUsageTags.effectiveSparkVersion",
            "spark.databricks.clusterUsageTags.sparkImageLabel",
            "spark.databricks.clusterUsageTags.runtimeEngine",
        }


class TestDatabricksRollingFileName:
    def test_prefix_is_eventlog(self):
        assert m.DB_EVENT_LOG_FILE_PREFIX == "eventlog"

    def test_date_pattern_parses_scala_format(self):
        pat = re.compile(m.DB_EVENT_LOG_DATE_REGEX)
        # Scala's getDBEventLogFileDate splits on '--' and parses
        # 'eventlog-YYYY-MM-DD--HH-MM[.codec]'.
        assert pat.search("eventlog-2021-06-14--20-00.gz")
        assert pat.search("eventlog-2021-06-14--20-00")
        assert not pat.search("eventlog")  # bare eventlog has no date
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_markers.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement `markers.py`**

Create `user_tools/src/spark_rapids_tools/tools/eventlog_detector/markers.py`:

```python
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

"""Single source of truth for runtime-detection markers.

Each constant below is pinned to a specific Scala source location. If
the Scala rule changes, update the constant here and the parity tests
under ``tests/spark_rapids_tools_ut/tools/eventlog_detector/`` will
catch any drift on a re-run.
"""

from typing import Mapping, Tuple

# ---------------------------------------------------------------------------
# SPARK_RAPIDS (GPU) markers
# Scala source: core/src/main/scala/org/apache/spark/sql/rapids/tool/ToolUtils.scala:114-121
# ---------------------------------------------------------------------------
GPU_PLUGIN_KEY: str = "spark.plugins"
GPU_PLUGIN_CLASS_SUBSTRING: str = "com.nvidia.spark.SQLPlugin"
GPU_ENABLED_KEY: str = "spark.rapids.sql.enabled"
# Default when GPU_ENABLED_KEY is missing or unparseable as bool. Matches
# Scala ``Try { ... }.getOrElse(true)`` in isPluginEnabled.
GPU_ENABLED_DEFAULT: bool = True

# ---------------------------------------------------------------------------
# AURON markers
# Scala source: core/src/main/scala/com/nvidia/spark/rapids/tool/planparser/auron/AuronParseHelper.scala:149-172
# ---------------------------------------------------------------------------
AURON_SPARK_EXTENSIONS_KEY: str = "spark.sql.extensions"
AURON_EXTENSION_REGEX: str = r".*AuronSparkSessionExtension.*"
AURON_ENABLED_KEY: str = "spark.auron.enabled"
AURON_ENABLED_DEFAULT: str = "true"

# ---------------------------------------------------------------------------
# Databricks precondition (all three keys must be non-empty)
# Scala source: core/src/main/scala/com/nvidia/spark/rapids/tool/planparser/db/DBPlugin.scala:45-58
# and DatabricksParseHelper.scala:188-190
# ---------------------------------------------------------------------------
DB_PRECONDITION_KEYS: Tuple[str, str, str] = (
    "spark.databricks.clusterUsageTags.clusterAllTags",
    "spark.databricks.clusterUsageTags.clusterId",
    "spark.databricks.clusterUsageTags.clusterName",
)

# ---------------------------------------------------------------------------
# PHOTON markers (any one fullmatches once Databricks precondition holds)
# Scala source: core/src/main/scala/com/nvidia/spark/rapids/tool/planparser/db/DatabricksParseHelper.scala:146-151
# ---------------------------------------------------------------------------
PHOTON_MARKER_REGEX: Mapping[str, str] = {
    "spark.databricks.clusterUsageTags.sparkVersion": r".*-photon-.*",
    "spark.databricks.clusterUsageTags.effectiveSparkVersion": r".*-photon-.*",
    "spark.databricks.clusterUsageTags.sparkImageLabel": r".*-photon-.*",
    "spark.databricks.clusterUsageTags.runtimeEngine": r"PHOTON",
}

# ---------------------------------------------------------------------------
# Databricks rolling event-log file layout
# Scala source: core/src/main/scala/com/nvidia/spark/rapids/tool/EventLogPathProcessor.scala:57
# and :458-478 (date parse in getDBEventLogFileDate)
# ---------------------------------------------------------------------------
DB_EVENT_LOG_FILE_PREFIX: str = "eventlog"
# Matches the dated form ``eventlog-YYYY-MM-DD--HH-MM[.codec]`` used by
# ``DatabricksRollingEventLogFilesFileReader``. Bare ``eventlog`` has no
# match and is treated as "latest" (sorted last) by the resolver.
DB_EVENT_LOG_DATE_REGEX: str = (
    r"^eventlog-(\d{4})-(\d{2})-(\d{2})--(\d{2})-(\d{2})(?:\.[A-Za-z0-9]+)?$"
)

# ---------------------------------------------------------------------------
# Supported Spark listener event names
# ---------------------------------------------------------------------------
EVENT_LOG_START: str = "SparkListenerLogStart"
EVENT_APPLICATION_START: str = "SparkListenerApplicationStart"
EVENT_ENVIRONMENT_UPDATE: str = "SparkListenerEnvironmentUpdate"
EVENT_SQL_EXECUTION_START: str = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
# Spark's actual SQLExecutionStart class name carries a package prefix in
# event logs. The unqualified shortname is sometimes used in test fixtures.
EVENT_SQL_EXECUTION_START_SHORTNAME: str = "SparkListenerSQLExecutionStart"
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_markers.py -v`
Expected: all assertions pass.

- [ ] **Step 5: Commit**

```bash
git add user_tools/src/spark_rapids_tools/tools/eventlog_detector/markers.py \
        user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_markers.py
git commit -m "feat(eventlog_detector): Scala-pinned markers module"
```

---

## Task 5: Classifier (pure function over a property dict)

**Files:**

- Create: `user_tools/src/spark_rapids_tools/tools/eventlog_detector/classifier.py`
- Test: `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_classifier.py`

- [ ] **Step 1: Write the failing test**

```python
# Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``eventlog_detector.classifier``."""

import pytest

from spark_rapids_tools.tools.eventlog_detector.classifier import _classify_runtime
from spark_rapids_tools.tools.eventlog_detector.types import SparkRuntime


class TestEmptyProperties:
    def test_empty_props_is_spark(self):
        assert _classify_runtime({}) is SparkRuntime.SPARK


class TestSparkRapids:
    def test_plugin_and_default_enabled(self):
        props = {"spark.plugins": "foo,com.nvidia.spark.SQLPlugin,bar"}
        assert _classify_runtime(props) is SparkRuntime.SPARK_RAPIDS

    def test_plugin_with_enabled_true(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": "true",
        }
        assert _classify_runtime(props) is SparkRuntime.SPARK_RAPIDS

    def test_plugin_with_enabled_false_demotes_to_spark(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": "false",
        }
        assert _classify_runtime(props) is SparkRuntime.SPARK

    def test_enabled_true_without_plugin_is_still_spark(self):
        props = {"spark.rapids.sql.enabled": "true"}
        assert _classify_runtime(props) is SparkRuntime.SPARK

    def test_unparseable_enabled_defaults_to_true(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": "not-a-bool",
        }
        assert _classify_runtime(props) is SparkRuntime.SPARK_RAPIDS


class TestAuron:
    def test_extension_and_default_enabled(self):
        props = {"spark.sql.extensions": "com.bytedance.auron.AuronSparkSessionExtension"}
        assert _classify_runtime(props) is SparkRuntime.AURON

    def test_extension_and_enabled_false_demotes_to_spark(self):
        props = {
            "spark.sql.extensions": "com.bytedance.auron.AuronSparkSessionExtension",
            "spark.auron.enabled": "FALSE",
        }
        assert _classify_runtime(props) is SparkRuntime.SPARK

    def test_auron_enabled_case_insensitive(self):
        props = {
            "spark.sql.extensions": "AuronSparkSessionExtension",
            "spark.auron.enabled": " TrUe ",
        }
        assert _classify_runtime(props) is SparkRuntime.AURON


class TestDatabricksPhoton:
    @pytest.fixture
    def db_precond_props(self):
        return {
            "spark.databricks.clusterUsageTags.clusterAllTags": "[{...}]",
            "spark.databricks.clusterUsageTags.clusterId": "1234",
            "spark.databricks.clusterUsageTags.clusterName": "dev-cluster",
        }

    def test_precondition_only_is_spark(self, db_precond_props):
        assert _classify_runtime(db_precond_props) is SparkRuntime.SPARK

    def test_precondition_plus_photon_version(self, db_precond_props):
        props = {
            **db_precond_props,
            "spark.databricks.clusterUsageTags.sparkVersion": "11.3.x-photon-scala2.12",
        }
        assert _classify_runtime(props) is SparkRuntime.PHOTON

    def test_precondition_plus_photon_engine(self, db_precond_props):
        props = {**db_precond_props, "spark.databricks.clusterUsageTags.runtimeEngine": "PHOTON"}
        assert _classify_runtime(props) is SparkRuntime.PHOTON

    def test_photon_marker_without_precondition_is_spark(self):
        props = {"spark.databricks.clusterUsageTags.runtimeEngine": "PHOTON"}
        assert _classify_runtime(props) is SparkRuntime.SPARK

    def test_photon_engine_other_value_is_spark(self, db_precond_props):
        props = {**db_precond_props, "spark.databricks.clusterUsageTags.runtimeEngine": "STANDARD"}
        assert _classify_runtime(props) is SparkRuntime.SPARK


class TestPriority:
    """PHOTON > AURON > SPARK_RAPIDS > SPARK when markers coexist."""

    def test_photon_beats_spark_rapids(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.databricks.clusterUsageTags.clusterAllTags": "[{...}]",
            "spark.databricks.clusterUsageTags.clusterId": "1",
            "spark.databricks.clusterUsageTags.clusterName": "c",
            "spark.databricks.clusterUsageTags.runtimeEngine": "PHOTON",
        }
        assert _classify_runtime(props) is SparkRuntime.PHOTON

    def test_auron_beats_spark_rapids(self):
        props = {
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.sql.extensions": "AuronSparkSessionExtension",
        }
        assert _classify_runtime(props) is SparkRuntime.AURON
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_classifier.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement the classifier**

Create `user_tools/src/spark_rapids_tools/tools/eventlog_detector/classifier.py`:

```python
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

"""Pure-function runtime classifier.

``_classify_runtime`` maps a merged Spark properties dict to a
``SparkRuntime``. Priority order when multiple markers are present:
``PHOTON > AURON > SPARK_RAPIDS > SPARK``. This is a deterministic Python
choice; Scala's plugin iteration order is undefined when multiple
plugins claim a runtime, but in practice markers do not overlap.
"""

import re
from typing import Mapping

from spark_rapids_tools.tools.eventlog_detector import markers as m
from spark_rapids_tools.tools.eventlog_detector.types import SparkRuntime


_TRUE_STRINGS = {"true", "1", "yes"}
_FALSE_STRINGS = {"false", "0", "no"}


def _parse_bool(raw: str, default: bool) -> bool:
    """Mirror Scala's ``Try { s.toBoolean }.getOrElse(default)``."""
    stripped = raw.strip().lower()
    if stripped in _TRUE_STRINGS:
        return True
    if stripped in _FALSE_STRINGS:
        return False
    return default


def _is_spark_rapids(props: Mapping[str, str]) -> bool:
    plugins = props.get(m.GPU_PLUGIN_KEY, "")
    if m.GPU_PLUGIN_CLASS_SUBSTRING not in plugins:
        return False
    raw = props.get(m.GPU_ENABLED_KEY)
    if raw is None:
        return m.GPU_ENABLED_DEFAULT
    return _parse_bool(raw, default=m.GPU_ENABLED_DEFAULT)


def _is_auron(props: Mapping[str, str]) -> bool:
    extensions = props.get(m.AURON_SPARK_EXTENSIONS_KEY)
    if extensions is None or not re.fullmatch(m.AURON_EXTENSION_REGEX, extensions):
        return False
    enabled_raw = props.get(m.AURON_ENABLED_KEY, m.AURON_ENABLED_DEFAULT)
    return enabled_raw.strip().lower() == m.AURON_ENABLED_DEFAULT


def _is_databricks(props: Mapping[str, str]) -> bool:
    return all(props.get(k, "").strip() for k in m.DB_PRECONDITION_KEYS)


def _is_photon(props: Mapping[str, str]) -> bool:
    if not _is_databricks(props):
        return False
    for key, pattern in m.PHOTON_MARKER_REGEX.items():
        value = props.get(key)
        if value is not None and re.fullmatch(pattern, value):
            return True
    return False


def _classify_runtime(props: Mapping[str, str]) -> SparkRuntime:
    # Priority: PHOTON > AURON > SPARK_RAPIDS > SPARK.
    if _is_photon(props):
        return SparkRuntime.PHOTON
    if _is_auron(props):
        return SparkRuntime.AURON
    if _is_spark_rapids(props):
        return SparkRuntime.SPARK_RAPIDS
    return SparkRuntime.SPARK
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_classifier.py -v`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add user_tools/src/spark_rapids_tools/tools/eventlog_detector/classifier.py \
        user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_classifier.py
git commit -m "feat(eventlog_detector): classifier mirroring Scala priority"
```

---

## Task 6: Stream opener (context-managed codec-aware line iterator)

**Files:**

- Create: `user_tools/src/spark_rapids_tools/tools/eventlog_detector/stream.py`
- Test: `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_stream.py`

- [ ] **Step 1: Write the failing test**

Create `test_stream.py`:

```python
# Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``eventlog_detector.stream``."""

import gzip
from pathlib import Path

import pytest
import zstandard as zstd

from spark_rapids_tools.storagelib import CspPath
from spark_rapids_tools.tools.eventlog_detector.stream import _open_event_log_stream
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
    def test_yields_all_lines(self, plain_file):
        with _open_event_log_stream(plain_file) as lines:
            collected = [ln for ln in lines]
        assert collected == SAMPLE_LINES


class TestGzipStream:
    def test_yields_all_lines(self, gz_file):
        with _open_event_log_stream(gz_file) as lines:
            collected = [ln for ln in lines]
        assert collected == SAMPLE_LINES


class TestZstdStream:
    def test_yields_all_lines(self, zstd_file):
        with _open_event_log_stream(zstd_file) as lines:
            collected = [ln for ln in lines]
        assert collected == SAMPLE_LINES

    def test_zst_short_suffix_also_works(self, tmp_path):
        p = tmp_path / "eventlog.zst"
        _write_zstd(p)
        with _open_event_log_stream(CspPath(str(p))) as lines:
            collected = [ln for ln in lines]
        assert collected == SAMPLE_LINES


class TestUnsupportedCompression:
    def test_lz4_raises(self, tmp_path):
        p = tmp_path / "eventlog.lz4"
        p.write_bytes(b"not-real-lz4")
        with pytest.raises(UnsupportedCompressionError):
            with _open_event_log_stream(CspPath(str(p))) as _:
                pass

    def test_snappy_raises(self, tmp_path):
        p = tmp_path / "eventlog.snappy"
        p.write_bytes(b"not-real-snappy")
        with pytest.raises(UnsupportedCompressionError):
            with _open_event_log_stream(CspPath(str(p))) as _:
                pass

    def test_lzf_raises(self, tmp_path):
        p = tmp_path / "eventlog.lzf"
        p.write_bytes(b"not-real-lzf")
        with pytest.raises(UnsupportedCompressionError):
            with _open_event_log_stream(CspPath(str(p))) as _:
                pass


class TestIoFailure:
    def test_missing_file_raises_read_error(self, tmp_path):
        p = tmp_path / "does-not-exist"
        with pytest.raises(EventLogReadError):
            with _open_event_log_stream(CspPath(str(p))) as lines:
                next(iter(lines))
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_stream.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement the stream opener**

Create `user_tools/src/spark_rapids_tools/tools/eventlog_detector/stream.py`:

```python
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
"""

import contextlib
import gzip
import io
from typing import Iterator

import zstandard as zstd

from spark_rapids_tools.storagelib import CspPath
from spark_rapids_tools.tools.eventlog_detector.types import (
    EventLogReadError,
    UnsupportedCompressionError,
)


_GZIP_SUFFIXES = {".gz"}
_ZSTD_SUFFIXES = {".zstd", ".zst"}
# Suffixes that unambiguously indicate a codec we do not support in V1.
_UNSUPPORTED_CODEC_SUFFIXES = {".lz4", ".lzf", ".snappy"}
# Treated as plain text.
_PLAIN_SUFFIXES = {".inprogress", ""}


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
        if suffix in _GZIP_SUFFIXES:
            decompressed: io.IOBase = gzip.GzipFile(fileobj=byte_stream, mode="rb")
            close_stack.callback(decompressed.close)
        elif suffix in _ZSTD_SUFFIXES:
            dctx = zstd.ZstdDecompressor()
            # stream_reader supports .read(); we need a readable binary layer
            # below TextIOWrapper. read1 emulation is good enough for line iter.
            decompressed = dctx.stream_reader(byte_stream)
            close_stack.callback(decompressed.close)
        elif suffix in _PLAIN_SUFFIXES or suffix not in _UNSUPPORTED_CODEC_SUFFIXES:
            # Unknown/empty suffix → best-effort treat as plain text. If the
            # file is actually compressed with an unknown codec the scanner
            # will simply see garbled lines that don't parse as JSON and be
            # skipped; env-update will never be reached and the caller will
            # see Route.UNKNOWN. That is the right failure mode here.
            decompressed = byte_stream
        else:  # pragma: no cover — every branch covered above
            raise UnsupportedCompressionError(f"Unsupported suffix: {suffix}")

        # Line-at-a-time text iterator over the decompressed stream.
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
```

Note on the suffix decision tree: `_PLAIN_SUFFIXES` intentionally includes `""` (no extension) so paths like `eventlog` or `foo/events_1_app-xyz` are read as plain text.

- [ ] **Step 4: Run the test and confirm it passes**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_stream.py -v`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add user_tools/src/spark_rapids_tools/tools/eventlog_detector/stream.py \
        user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_stream.py
git commit -m "feat(eventlog_detector): context-managed codec-aware line streamer"
```

---

## Task 7: Path resolver (single file + Databricks rolling dir)

**Files:**

- Create: `user_tools/src/spark_rapids_tools/tools/eventlog_detector/resolver.py`
- Test: `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_resolver.py`

- [ ] **Step 1: Write the failing test**

```python
# Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``eventlog_detector.resolver``."""

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


class TestResolveSingleFile:
    def test_single_file_returns_single_element_list(self, tmp_path: Path):
        f = tmp_path / "eventlog.zstd"
        f.write_bytes(b"x")
        source, files = _resolve_event_log_files(CspPath(str(f)))
        assert source == str(f)
        assert [p.base_name() for p in files] == ["eventlog.zstd"]


class TestResolveDatabricksRollingDir:
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
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_resolver.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement the resolver**

Create `user_tools/src/spark_rapids_tools/tools/eventlog_detector/resolver.py`:

```python
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
    source = str(path)

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
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_resolver.py -v`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add user_tools/src/spark_rapids_tools/tools/eventlog_detector/resolver.py \
        user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_resolver.py
git commit -m "feat(eventlog_detector): single-file and Databricks rolling-dir resolver"
```

---

## Task 8: Event scanner (streaming, bounded, multi-file)

**Files:**

- Create: `user_tools/src/spark_rapids_tools/tools/eventlog_detector/scanner.py`
- Test: `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_scanner.py`

- [ ] **Step 1: Write the failing test**

```python
# Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``eventlog_detector.scanner``."""

from pathlib import Path
from typing import List

import pytest

from spark_rapids_tools.storagelib import CspPath
from spark_rapids_tools.tools.eventlog_detector.scanner import (
    _scan_events,
    _scan_events_across,
)
from spark_rapids_tools.tools.eventlog_detector.types import (
    SparkRuntime,
    Termination,
)


# ---------- Line builders ----------

def env_update(props: dict) -> str:
    import json

    return json.dumps(
        {
            "Event": "SparkListenerEnvironmentUpdate",
            "Spark Properties": props,
            "System Properties": {},
            "Classpath Entries": {},
            "JVM Information": {},
        }
    )


def log_start(version: str = "3.5.1") -> str:
    import json

    return json.dumps({"Event": "SparkListenerLogStart", "Spark Version": version})


def app_start(app_id: str = "app-1", app_name: str = "App") -> str:
    import json

    return json.dumps(
        {
            "Event": "SparkListenerApplicationStart",
            "App ID": app_id,
            "App Name": app_name,
        }
    )


def sql_exec_start(modified_configs: dict) -> str:
    import json

    return json.dumps(
        {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": 0,
            "description": "x",
            "details": "",
            "physicalPlanDescription": "",
            "sparkPlanInfo": {},
            "time": 0,
            "modifiedConfigs": modified_configs,
        }
    )


# ---------- Tests for _scan_events (single stream) ----------

class TestScanEvents:
    def test_env_update_with_gpu_is_decisive(self):
        lines = iter(
            [
                log_start(),
                app_start(),
                env_update({"spark.plugins": "com.nvidia.spark.SQLPlugin"}),
            ]
        )
        result = _scan_events(lines, budget=100)
        assert result.env_update_seen is True
        assert result.app_id == "app-1"
        assert result.spark_version == "3.5.1"
        assert result.termination is Termination.DECISIVE

    def test_env_update_cpu_then_sql_start_flips_to_gpu(self):
        lines = iter(
            [
                log_start(),
                app_start(),
                env_update({"spark.master": "local"}),
                sql_exec_start({"spark.plugins": "com.nvidia.spark.SQLPlugin"}),
            ]
        )
        result = _scan_events(lines, budget=100)
        assert result.termination is Termination.DECISIVE

    def test_cpu_only_to_eof_is_exhausted(self):
        lines = iter([log_start(), app_start(), env_update({"spark.master": "local"})])
        result = _scan_events(lines, budget=100)
        assert result.env_update_seen is True
        assert result.termination is Termination.EXHAUSTED

    def test_no_env_update_within_budget_is_cap_hit(self):
        # Budget less than the number of events, none of them env-update.
        lines = iter([log_start()] * 5)
        result = _scan_events(lines, budget=2)
        assert result.env_update_seen is False
        assert result.termination is Termination.CAP_HIT

    def test_no_env_update_to_eof_is_exhausted_without_env(self):
        lines = iter([log_start(), app_start()])
        result = _scan_events(lines, budget=100)
        assert result.env_update_seen is False
        assert result.termination is Termination.EXHAUSTED

    def test_malformed_json_lines_are_skipped(self):
        lines = iter(
            [
                "not-json-at-all",
                log_start(),
                "",
                app_start(),
                env_update({"spark.master": "local"}),
            ]
        )
        result = _scan_events(lines, budget=100)
        assert result.env_update_seen is True
        assert result.app_id == "app-1"

    def test_later_sql_start_overwrites_earlier_property(self):
        # last-write-wins merge, matching CacheablePropsHandler.mergeModifiedConfigs.
        lines = iter(
            [
                env_update({"spark.rapids.sql.enabled": "false", "spark.plugins": "com.nvidia.spark.SQLPlugin"}),
                sql_exec_start({"spark.rapids.sql.enabled": "true"}),
            ]
        )
        result = _scan_events(lines, budget=100)
        assert result.termination is Termination.DECISIVE
        # Final accumulated props reflect the merge.
        assert result.spark_properties["spark.rapids.sql.enabled"] == "true"


# ---------- Tests for _scan_events_across (multi-file) ----------

def _write(path: Path, lines: List[str]) -> CspPath:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return CspPath(str(path))


class TestScanEventsAcross:
    def test_gpu_signal_in_second_file_is_decisive(self, tmp_path):
        f1 = _write(
            tmp_path / "eventlog-2021-06-14--18-00",
            [log_start(), app_start(), env_update({"spark.master": "local"})],
        )
        f2 = _write(
            tmp_path / "eventlog-2021-06-14--20-00",
            [sql_exec_start({"spark.plugins": "com.nvidia.spark.SQLPlugin"})],
        )
        result = _scan_events_across([f1, f2], budget=100)
        assert result.termination is Termination.DECISIVE

    def test_shared_budget_applied_across_files(self, tmp_path):
        # 3 events in first file, 3 in second. Budget = 4. Second file stops
        # after one event, before any GPU signal.
        f1 = _write(tmp_path / "a", [log_start(), app_start(), env_update({"spark.master": "local"})])
        f2 = _write(
            tmp_path / "b",
            [
                sql_exec_start({"spark.master": "still-cpu"}),
                sql_exec_start({"spark.plugins": "com.nvidia.spark.SQLPlugin"}),
                sql_exec_start({"x": "y"}),
            ],
        )
        result = _scan_events_across([f1, f2], budget=4)
        assert result.termination is Termination.CAP_HIT

    def test_all_files_exhausted_returns_exhausted(self, tmp_path):
        f1 = _write(tmp_path / "a", [env_update({"spark.master": "local"})])
        result = _scan_events_across([f1], budget=100)
        assert result.termination is Termination.EXHAUSTED
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_scanner.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement the scanner**

Create `user_tools/src/spark_rapids_tools/tools/eventlog_detector/scanner.py`:

```python
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

"""Bounded streaming event scanner.

Walks JSON-per-line event logs under a shared event budget, merges
properties from startup and per-SQL events into a single mutable dict,
and reports early-stop/exhausted/cap-hit termination. Strictly streaming:
no slurping, no accumulating raw events.
"""

import json
from dataclasses import dataclass, field
from typing import Dict, Iterable, Iterator, List, Optional

from spark_rapids_tools.storagelib import CspPath
from spark_rapids_tools.tools.eventlog_detector import markers as m
from spark_rapids_tools.tools.eventlog_detector.classifier import _classify_runtime
from spark_rapids_tools.tools.eventlog_detector.stream import _open_event_log_stream
from spark_rapids_tools.tools.eventlog_detector.types import SparkRuntime, Termination


@dataclass
class _ScanResult:
    spark_properties: Dict[str, str] = field(default_factory=dict)
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    spark_version: Optional[str] = None
    env_update_seen: bool = False
    events_scanned: int = 0
    termination: Termination = Termination.EXHAUSTED


def _scan_events(
    lines: Iterable[str],
    *,
    budget: int,
    state: Optional[_ScanResult] = None,
) -> _ScanResult:
    """Scan one stream of lines, optionally continuing from a prior state.

    Returns the updated ``_ScanResult``. Terminates as soon as classification
    turns non-SPARK (``DECISIVE``), or when the budget is exhausted
    (``CAP_HIT``), or when ``lines`` is fully consumed (``EXHAUSTED``).
    """
    result = state if state is not None else _ScanResult()

    for raw in lines:
        if result.events_scanned >= budget:
            result.termination = Termination.CAP_HIT
            return result

        if not raw:
            continue

        try:
            event = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            # Malformed JSON line (Spark tolerates trailing partials). Count
            # it against the budget so a pathological log can't stall us.
            result.events_scanned += 1
            continue

        result.events_scanned += 1
        name = event.get("Event")
        if name == m.EVENT_LOG_START:
            version = event.get("Spark Version")
            if isinstance(version, str):
                result.spark_version = version
        elif name == m.EVENT_APPLICATION_START:
            app_id = event.get("App ID")
            app_name = event.get("App Name")
            if isinstance(app_id, str):
                result.app_id = app_id
            if isinstance(app_name, str):
                result.app_name = app_name
        elif name == m.EVENT_ENVIRONMENT_UPDATE:
            props = event.get("Spark Properties") or {}
            if isinstance(props, dict):
                for k, v in props.items():
                    if isinstance(k, str) and isinstance(v, str):
                        result.spark_properties[k] = v
                result.env_update_seen = True
                if _classify_runtime(result.spark_properties) is not SparkRuntime.SPARK:
                    result.termination = Termination.DECISIVE
                    return result
        elif name in (m.EVENT_SQL_EXECUTION_START, m.EVENT_SQL_EXECUTION_START_SHORTNAME):
            modified = event.get("modifiedConfigs") or {}
            if isinstance(modified, dict) and modified:
                for k, v in modified.items():
                    if isinstance(k, str) and isinstance(v, str):
                        result.spark_properties[k] = v
                if result.env_update_seen and (
                    _classify_runtime(result.spark_properties) is not SparkRuntime.SPARK
                ):
                    result.termination = Termination.DECISIVE
                    return result

    # Fully consumed without early-stop or budget exhaustion.
    result.termination = Termination.EXHAUSTED
    return result


def _scan_events_across(files: List[CspPath], *, budget: int) -> _ScanResult:
    """Walk ``files`` in order under a single shared ``budget``."""
    state = _ScanResult()
    for path in files:
        if state.events_scanned >= budget:
            state.termination = Termination.CAP_HIT
            return state
        with _open_event_log_stream(path) as lines:
            state = _scan_events(lines, budget=budget, state=state)
        if state.termination in (Termination.DECISIVE, Termination.CAP_HIT):
            return state
    # All files consumed.
    state.termination = Termination.EXHAUSTED
    return state
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_scanner.py -v`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add user_tools/src/spark_rapids_tools/tools/eventlog_detector/scanner.py \
        user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_scanner.py
git commit -m "feat(eventlog_detector): bounded streaming scanner across files"
```

---

## Task 9: Top-level `detect_spark_runtime`

**Files:**

- Create: `user_tools/src/spark_rapids_tools/tools/eventlog_detector/detector.py`
- Test: `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_detector.py`

- [ ] **Step 1: Write the failing integration test**

```python
# Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Integration tests for ``eventlog_detector.detect_spark_runtime``."""

import gzip
import json
from pathlib import Path

import pytest
import zstandard as zstd

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
        assert "no decisive signal" in result.reason


class TestDatabricksRolling:
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


class TestUnsupportedInput:
    def test_spark_native_rolling_dir_raises(self, tmp_path):
        d = tmp_path / "eventlog_v2_local-1623876083964"
        d.mkdir()
        (d / "events_1_local-1623876083964").write_bytes(b"")
        with pytest.raises(UnsupportedInputError):
            detect_spark_runtime(CspPath(str(d)))


class TestReasonStrings:
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
        assert "walked full log" in result.reason
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_detector.py -v`
Expected: `ModuleNotFoundError` (detector module missing).

- [ ] **Step 3: Implement the detector**

Create `user_tools/src/spark_rapids_tools/tools/eventlog_detector/detector.py`:

```python
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

"""Top-level event log runtime detector.

``detect_spark_runtime(path)`` returns a ``DetectionResult`` carrying the
routing decision and best-effort metadata. On inconclusive input, returns
``Route.UNKNOWN`` rather than raising; callers fall back to the full
Scala pipeline in that case.
"""

from typing import Optional, Union

from spark_rapids_tools.storagelib import CspPath
from spark_rapids_tools.tools.eventlog_detector.classifier import _classify_runtime
from spark_rapids_tools.tools.eventlog_detector.resolver import _resolve_event_log_files
from spark_rapids_tools.tools.eventlog_detector.scanner import _scan_events_across
from spark_rapids_tools.tools.eventlog_detector.types import (
    DetectionResult,
    Route,
    SparkRuntime,
    Termination,
)


_GPU_FAMILY = frozenset({SparkRuntime.SPARK_RAPIDS, SparkRuntime.PHOTON, SparkRuntime.AURON})


def detect_spark_runtime(
    event_log: Union[str, CspPath],
    *,
    max_events_scanned: int = 500,
) -> DetectionResult:
    """Classify a single-app event log into a routing decision.

    Returns ``DetectionResult`` with:

    * ``route`` = ``PROFILING`` for any decisive non-SPARK classification,
    * ``QUALIFICATION`` only after the scanner walked the full log with
      no GPU-family signal,
    * ``UNKNOWN`` when the event budget was hit first or
      ``SparkListenerEnvironmentUpdate`` was never seen.

    ``max_events_scanned`` caps CPU/IO cost; large CPU logs routinely end
    as ``UNKNOWN`` at the cap. Raise the cap at the call site to trade
    cost for decisiveness.
    """
    path = event_log if isinstance(event_log, CspPath) else CspPath(str(event_log))
    source, files = _resolve_event_log_files(path)

    scan = _scan_events_across(files, budget=max_events_scanned)

    # Classify from whatever we accumulated.
    runtime: Optional[SparkRuntime]
    if scan.env_update_seen:
        runtime = _classify_runtime(scan.spark_properties)
    else:
        runtime = None

    # Apply the asymmetric decision rule.
    if runtime in _GPU_FAMILY:
        route = Route.PROFILING
        reason = f"decisive: classified as {runtime.value}"
    elif scan.termination is Termination.EXHAUSTED and scan.env_update_seen:
        route = Route.QUALIFICATION
        reason = "walked full log, no GPU-family signal"
    else:
        route = Route.UNKNOWN
        reason = (
            "no decisive signal within bounded scan"
            if scan.env_update_seen
            else "no SparkListenerEnvironmentUpdate reached"
        )

    resolved_path = str(files[0]) if files else source
    return DetectionResult(
        route=route,
        spark_runtime=runtime,
        app_id=scan.app_id,
        spark_version=scan.spark_version,
        event_log_path=resolved_path,
        source_path=source,
        reason=reason,
    )
```

- [ ] **Step 4: Run the tests**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_detector.py -v`
Expected: all tests pass.

- [ ] **Step 5: Sanity-check the package-level re-exports**

Run: `python -c "from spark_rapids_tools.tools.eventlog_detector import detect_spark_runtime, Route, DetectionResult; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 6: Commit**

```bash
git add user_tools/src/spark_rapids_tools/tools/eventlog_detector/detector.py \
        user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_detector.py
git commit -m "feat(eventlog_detector): top-level detect_spark_runtime entry point"
```

---

## Task 10: Fixture tests against existing Scala event logs

**Files:**

- Create: `user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_detector_fixtures.py`

- [ ] **Step 1: Confirm the fixtures exist and are readable**

Run these sanity checks:

```bash
ls core/src/test/resources/spark-events-profiling/eventlog-gpu-dsv2.zstd
ls core/src/test/resources/spark-events-profiling/eventlog_dsv2.zstd
ls core/src/test/resources/spark-events-qualification/eventlog_same_app_id_1.zstd
```

Expected: all three paths print. If any is missing, add it to the `pytest.mark.skip` decorator in the test below and note it in Section 12 of the spec (open items).

- [ ] **Step 2: Write the fixture tests**

Create `test_detector_fixtures.py`:

```python
# Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Anchor tests against fixtures already shipped in the Scala core.

These are not a full parity sweep. They catch regressions on a small
curated set covering each decisive route.
"""

from pathlib import Path

import pytest

from spark_rapids_tools.tools.eventlog_detector import detect_spark_runtime
from spark_rapids_tools.tools.eventlog_detector.types import Route, SparkRuntime


REPO_ROOT = Path(__file__).resolve().parents[5]
CORE_FIXTURES = REPO_ROOT / "core" / "src" / "test" / "resources"


@pytest.mark.parametrize(
    "relative_path,expected_route,expected_runtime",
    [
        (
            "spark-events-profiling/eventlog-gpu-dsv2.zstd",
            Route.PROFILING,
            SparkRuntime.SPARK_RAPIDS,
        ),
        (
            "spark-events-profiling/eventlog_dsv2.zstd",
            Route.QUALIFICATION,
            SparkRuntime.SPARK,
        ),
        (
            "spark-events-qualification/eventlog_same_app_id_1.zstd",
            Route.QUALIFICATION,
            SparkRuntime.SPARK,
        ),
    ],
)
def test_detector_matches_expected_route_on_scala_fixture(
    relative_path: str, expected_route: Route, expected_runtime: SparkRuntime
) -> None:
    fixture = CORE_FIXTURES / relative_path
    if not fixture.exists():
        pytest.skip(f"fixture not available: {fixture}")
    # Fixtures are ~small; a generous budget keeps this test decisive.
    result = detect_spark_runtime(str(fixture), max_events_scanned=5000)
    assert result.route is expected_route, result.reason
    assert result.spark_runtime is expected_runtime, result.reason
```

- [ ] **Step 3: Run the fixture tests**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_detector_fixtures.py -v`
Expected: all cases pass (or skip with a clear reason if a fixture is missing).

- [ ] **Step 4: Commit**

```bash
git add user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/test_detector_fixtures.py
git commit -m "test(eventlog_detector): anchor parity tests on Scala fixtures"
```

---

## Task 11: Run the full test suite and linter

**Files:** none modified.

- [ ] **Step 1: Run every detector test**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/ -v`
Expected: all tests pass (ignore tests that skipped with "fixture not available").

- [ ] **Step 2: Run the full user_tools test suite to catch unintended regressions**

Run: `pytest user_tools/tests/spark_rapids_tools_ut/ -x`
Expected: same pass/fail baseline as before this branch (nothing that was green turns red).

- [ ] **Step 3: Run the project's linter against the new files**

Run (adjust if the project uses a different runner):

```
pylint user_tools/src/spark_rapids_tools/tools/eventlog_detector/ \
       user_tools/tests/spark_rapids_tools_ut/tools/eventlog_detector/
```

Expected: no new errors or warnings above the project's existing baseline. If the project pins a specific pylint config in `pyproject.toml` or `.pylintrc`, read that file first and rerun with the matching options. If `pylint` isn't configured here, substitute the tool the repo actually uses (check `pyproject.toml` or `user_tools/dev/` for hooks).

- [ ] **Step 4: Commit any docstring/type-hint fixups surfaced by the linter**

If fixups were needed, stage only the files the linter touched and commit:

```bash
git add -u
git commit -m "chore(eventlog_detector): resolve lint findings from full-suite run"
```

If no changes were required, skip the commit.

---

## Task 12: Update plan doc with the realized inventory

**Files:**

- Modify: `docs/superpowers/specs/2026-04-22-eventlog-runtime-detector-design.md`

- [ ] **Step 1: Replace the placeholder in Section 12 (Open items)**

The spec currently says:

```
- Parity-test fixture inventory: enumerate every file under `core/src/test/resources/spark-events-*` and record the expected `SparkRuntime` label, derived from existing Scala test expectations. The plan step owns this list.
```

Replace with the actual list used in Task 10:

```
- Parity-test fixture inventory (realized):
  - `spark-events-profiling/eventlog-gpu-dsv2.zstd` → `PROFILING` / `SPARK_RAPIDS`
  - `spark-events-profiling/eventlog_dsv2.zstd` → `QUALIFICATION` / `SPARK`
  - `spark-events-qualification/eventlog_same_app_id_1.zstd` → `QUALIFICATION` / `SPARK`
```

Use Edit with the old text as `old_string`.

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/2026-04-22-eventlog-runtime-detector-design.md
git commit -m "docs(spec): record realized fixture inventory for event log detector"
```

---

## Self-Review

**Spec coverage:**

- Sections 1–4 (problem, goal, non-goals, consumers) → no code; covered by the plan's Goal/Architecture header.
- Section 5 (public API) → Tasks 2, 3, 9 (re-exports, types, entry point).
- Section 6 (input shapes, codecs) → Tasks 6, 7.
- Section 7.1 (resolver) → Task 7.
- Section 7.2 (stream opener) → Task 6.
- Section 7.3 (scanner + termination) → Task 8.
- Section 7.4 (classifier) → Task 5.
- Section 7.5 (top-level flow + asymmetric rule) → Task 9.
- Section 7.6 (markers) → Task 4.
- Section 8 (classification rules) → Task 5 (classifier) + Task 4 (markers).
- Section 9 (error model) → Task 3 (types) + all tasks that raise.
- Section 10 (testing) → every task's test plus Task 10 (fixture anchor) and Task 11 (full-suite run).
- Section 11 (rollout) → this plan produces one PR.
- Section 12 open item (`zstandard` RELEASE.md note) → not addressed; treat as a follow-up for the release engineer. Section 12 open item (fixture inventory) → Task 12.
- Sections 13 (evolution) → doc-only, no code.
- Memory contract (section 7 header) → Task 6 (no `read()`/`readlines()`; streaming only) + Task 8 (no raw-event accumulation).
- Decision rule (section 2) → Task 9.

**Placeholder scan:** every step shows exact paths, full file contents for new files, and specific commands. No "TODO", "fill in later", "similar to Task N", or bare "add appropriate handling" phrases.

**Type/signature consistency:**

- `Route`, `SparkRuntime`, `Termination`, `DetectionResult`, and the exception classes are defined in Task 3 and used verbatim in Tasks 5, 6, 7, 8, 9, 10 — same names and field orders throughout.
- `_resolve_event_log_files` (plural) is defined in Task 7 and called in Task 9.
- `_scan_events` / `_scan_events_across` signatures match between Task 8's implementation and Task 9's caller.
- `_open_event_log_stream` is a context manager in Task 6; Tasks 8 and 9 use it with `with`.
- `_ScanResult` fields (`spark_properties`, `app_id`, `app_name`, `spark_version`, `env_update_seen`, `events_scanned`, `termination`) are consistent between Task 8 and the read-sites in Task 9.

No gaps found.