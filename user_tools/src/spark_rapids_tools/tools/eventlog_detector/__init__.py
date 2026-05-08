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

Scans a bounded prefix of a Spark event log and returns a tool execution
decision (``QUALIFICATION`` / ``PROFILING`` / ``UNKNOWN``) plus best-effort
runtime metadata, without invoking the full tools pipeline.

Public entry point: :func:`detect_spark_runtime`.
"""

from .detector import detect_spark_runtime
from .types import (
    DetectionResult,
    EventLogDetectionError,
    EventLogReadError,
    SparkRuntime,
    ToolExecution,
    UnsupportedCompressionError,
    UnsupportedInputError,
)

__all__ = [
    "DetectionResult",
    "EventLogDetectionError",
    "EventLogReadError",
    "SparkRuntime",
    "ToolExecution",
    "UnsupportedCompressionError",
    "UnsupportedInputError",
    "detect_spark_runtime",
]
