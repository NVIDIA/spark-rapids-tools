/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.tool.planparser

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode

case class HiveScanSerdeClasses(className: String, format: String) extends Logging {
  def parseReadNode(node: SparkPlanGraphNode): ReadMetaData = {
    logDebug(s"Parsing node as ScanHiveTable: ${node.desc}")
    // Schema, pushedFilters empty for now as we cannot extract them yet from eventlogs
    ReadMetaData("", "HiveTableRelation", "unknown", format)
  }
}

object HiveParseHelper extends Logging {
  val SCAN_HIVE_LABEL = "scan hive"
  val SCAN_HIVE_EXEC_NAME = "HiveTableScanExec"
  // Classes we can look for is SerDe
  private val LOADED_SERDE_CLASSES = Seq(
    HiveScanSerdeClasses("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "HiveText"),
    HiveScanSerdeClasses("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      "HiveParquet"),
    HiveScanSerdeClasses("org.apache.hadoop.hive.serde2.avro.AvroSerDe", "HiveAvro"),
    HiveScanSerdeClasses("org.apache.hadoop.hive.serde2.OpenCSVSerde", "HiveCSV")
  )

  def isHiveTableScanNode(node: SparkPlanGraphNode): Boolean = {
    node.name.toLowerCase.startsWith(SCAN_HIVE_LABEL)
  }

  def parseReadNode(node: SparkPlanGraphNode): ReadMetaData = {
    LOADED_SERDE_CLASSES.find(k => node.desc.contains(k.className)).map(
      _.parseReadNode(node)).getOrElse(ReadMetaData("", "HiveTableRelation", "unknown", "unknown"))
  }

}
