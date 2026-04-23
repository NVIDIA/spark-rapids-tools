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

Walks JSON-per-line event logs under a shared event budget, folding the
relevant startup and per-SQL properties into a single mutable dict so
the classifier can decide as soon as a decisive signal is seen.
"""

import json
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

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
    last_scanned_path: Optional[str] = None


def _scan_events(
    lines: Iterable[str],
    *,
    budget: int,
    state: Optional[_ScanResult] = None,
) -> _ScanResult:
    """Scan one stream of lines, optionally continuing from a prior state.

    Terminates as ``DECISIVE`` on the first non-SPARK classification,
    ``CAP_HIT`` when ``budget`` is exhausted, or ``EXHAUSTED`` when the
    iterator runs out.
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
            # Tolerate trailing partial lines in live logs; count them so
            # a pathological log can't keep us scanning forever.
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

    result.termination = Termination.EXHAUSTED
    return result


def _scan_events_across(files: List[CspPath], *, budget: int) -> _ScanResult:
    """Walk ``files`` in order under a single shared ``budget``."""
    state = _ScanResult()
    for path in files:
        if state.events_scanned >= budget:
            state.termination = Termination.CAP_HIT
            return state
        state.last_scanned_path = str(path)
        with _open_event_log_stream(path) as lines:
            state = _scan_events(lines, budget=budget, state=state)
        if state.termination in (Termination.DECISIVE, Termination.CAP_HIT):
            return state
    state.termination = Termination.EXHAUSTED
    return state
