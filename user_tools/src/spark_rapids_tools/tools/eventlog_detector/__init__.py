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

from .detector import detect_spark_runtime
from .types import (
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
