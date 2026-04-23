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

"""Property keys, regexes, and constants used by the runtime classifier.

Each block carries a Scala source reference so the two implementations
can be kept in sync when the Scala detection rules change.
"""

from typing import Mapping, Tuple

# GPU (SPARK_RAPIDS) markers.
# Scala: org/apache/spark/sql/rapids/tool/ToolUtils.scala :: isPluginEnabled
GPU_PLUGIN_KEY: str = "spark.plugins"
GPU_PLUGIN_CLASS_SUBSTRING: str = "com.nvidia.spark.SQLPlugin"
GPU_ENABLED_KEY: str = "spark.rapids.sql.enabled"
# Defaults to true when missing or unparseable.
GPU_ENABLED_DEFAULT: bool = True

# AURON markers.
# Scala: com/nvidia/spark/rapids/tool/planparser/auron/AuronParseHelper.scala
AURON_SPARK_EXTENSIONS_KEY: str = "spark.sql.extensions"
AURON_EXTENSION_REGEX: str = r".*AuronSparkSessionExtension.*"
AURON_ENABLED_KEY: str = "spark.auron.enabled"
AURON_ENABLED_DEFAULT: str = "true"

# Databricks precondition — all three keys must be non-empty.
# Scala: com/nvidia/spark/rapids/tool/planparser/db/DBPlugin.scala :: DBConditionImpl
DB_PRECONDITION_KEYS: Tuple[str, str, str] = (
    "spark.databricks.clusterUsageTags.clusterAllTags",
    "spark.databricks.clusterUsageTags.clusterId",
    "spark.databricks.clusterUsageTags.clusterName",
)

# Photon markers — any one fullmatches once the Databricks precondition holds.
# Scala: com/nvidia/spark/rapids/tool/planparser/db/DatabricksParseHelper.scala :: PhotonParseHelper
PHOTON_MARKER_REGEX: Mapping[str, str] = {
    "spark.databricks.clusterUsageTags.sparkVersion": r".*-photon-.*",
    "spark.databricks.clusterUsageTags.effectiveSparkVersion": r".*-photon-.*",
    "spark.databricks.clusterUsageTags.sparkImageLabel": r".*-photon-.*",
    "spark.databricks.clusterUsageTags.runtimeEngine": r"PHOTON",
}

# Databricks rolling event-log file layout.
# Scala: com/nvidia/spark/rapids/tool/EventLogPathProcessor.scala :: getDBEventLogFileDate
DB_EVENT_LOG_FILE_PREFIX: str = "eventlog"
# ``eventlog-YYYY-MM-DD--HH-MM[.codec]``. Bare ``eventlog`` does not match
# and is treated as the latest chunk (sorted last) by the resolver.
DB_EVENT_LOG_DATE_REGEX: str = (
    r"^eventlog-(\d{4})-(\d{2})-(\d{2})--(\d{2})-(\d{2})(?:\.[A-Za-z0-9]+)?$"
)

# Spark listener event names consumed by the scanner.
EVENT_LOG_START: str = "SparkListenerLogStart"
EVENT_APPLICATION_START: str = "SparkListenerApplicationStart"
EVENT_ENVIRONMENT_UPDATE: str = "SparkListenerEnvironmentUpdate"
EVENT_SQL_EXECUTION_START: str = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
# Unqualified shortname; sometimes used in test fixtures.
EVENT_SQL_EXECUTION_START_SHORTNAME: str = "SparkListenerSQLExecutionStart"
