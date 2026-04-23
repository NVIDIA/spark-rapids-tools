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

"""Top-level event log runtime detector."""

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

    Returns a :class:`DetectionResult` whose ``route`` is ``PROFILING`` on
    a decisive non-SPARK classification, ``QUALIFICATION`` only after the
    scanner walked the full log without seeing a GPU-family signal, and
    ``UNKNOWN`` otherwise (e.g., the budget was hit first or the log never
    emitted ``SparkListenerEnvironmentUpdate``).

    ``max_events_scanned`` caps CPU/IO cost. Large CPU logs routinely end
    as ``UNKNOWN`` at the cap; raise it at the call site to trade cost
    for decisiveness.
    """
    # Keep the caller's input verbatim in source_path (cloud URI schemes
    # would otherwise be stripped by CspPath normalisation).
    source_path = event_log if isinstance(event_log, str) else str(event_log)
    path = event_log if isinstance(event_log, CspPath) else CspPath(str(event_log))
    _, files = _resolve_event_log_files(path)

    scan = _scan_events_across(files, budget=max_events_scanned)

    runtime: Optional[SparkRuntime]
    if scan.env_update_seen:
        runtime = _classify_runtime(scan.spark_properties)
    else:
        runtime = None

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

    resolved_path = scan.last_scanned_path or (str(files[0]) if files else source_path)
    return DetectionResult(
        route=route,
        spark_runtime=runtime,
        app_id=scan.app_id,
        spark_version=scan.spark_version,
        event_log_path=resolved_path,
        source_path=source_path,
        reason=reason,
    )
