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
