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


class EventLogDetectionError(Exception):
    """Base class for detector errors."""


class UnsupportedInputError(EventLogDetectionError):
    """Input shape is outside V1 scope (multi-app dir, wildcard, comma list, ...)."""


class UnsupportedCompressionError(EventLogDetectionError):
    """File uses a compression codec the V1 detector does not handle."""


class EventLogReadError(EventLogDetectionError):
    """Wraps an underlying I/O failure when reading the event log."""
