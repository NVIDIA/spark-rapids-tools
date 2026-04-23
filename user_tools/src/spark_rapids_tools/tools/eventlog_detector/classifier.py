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


def _parse_bool(raw: str, default: bool) -> bool:
    """Mirror Scala's ``Try { s.toBoolean }.getOrElse(default)``.

    Scala's ``String.toBoolean`` accepts only ``"true"``/``"false"``
    case-insensitively. Everything else (including ``"yes"``, ``"no"``,
    ``"1"``, ``"0"``) falls back to ``default`` because the Scala call
    would throw ``IllegalArgumentException``.
    """
    stripped = raw.strip().lower()
    if stripped == "true":
        return True
    if stripped == "false":
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
