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

from typing import Any

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

# Lazy re-exports: the submodules that back these names are added across
# subsequent tasks. Using ``__getattr__`` defers the import until the name
# is actually accessed, which keeps intermediate test suites importable
# while the package is being built out.
_TYPES_NAMES = {
    "DetectionResult",
    "EventLogDetectionError",
    "EventLogReadError",
    "Route",
    "SparkRuntime",
    "UnsupportedCompressionError",
    "UnsupportedInputError",
}


def __getattr__(name: str) -> Any:
    if name == "detect_spark_runtime":
        from .detector import detect_spark_runtime as _fn
        return _fn
    if name in _TYPES_NAMES:
        from . import types as _types
        return getattr(_types, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
