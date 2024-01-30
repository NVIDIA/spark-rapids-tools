/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
import org.apache.spark.sql.rapids.tool.util.EventUtils

// A wrapper class to map between
case class HiveScanSerdeClasses(className: String, format: String) extends Logging {
  def parseReadNode(node: SparkPlanGraphNode): ReadMetaData = {
    logDebug(s"Parsing node as ScanHiveTable: ${node.desc}")
    // Schema, pushedFilters empty for now as we cannot extract them yet from eventlogs
    ReadMetaData("", "HiveTableRelation", "unknown", format)
  }
  def accepts(str: String): Boolean = {
    str.equals(className)
  }
  def accepts(node: SparkPlanGraphNode): Boolean = {
    node.desc.contains(className)
  }
}

// Utilities used to handle Hive Ops.
object HiveParseHelper extends Logging {
  val SCAN_HIVE_LABEL = "scan hive"
  val SCAN_HIVE_EXEC_NAME = "HiveTableScanExec"
  val INSERT_INTO_HIVE_LABEL = "InsertIntoHiveTable"

  // The following is a list of Classes we can look for is SerDe.
  // We should maintain this table with custom classes as needed.
  // Note that we map each SerDe to a format "Hive*" because the hive formats are still different
  // compared to the native onces according to the documentation. For example, this is why the
  // the "supportedDataSource.csv" has a "HiveText" entry.
  private val LOADED_SERDE_CLASSES = Seq(
    HiveScanSerdeClasses("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "HiveText"),
    HiveScanSerdeClasses("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      "HiveParquet"),
    HiveScanSerdeClasses("org.apache.hadoop.hive.serde2.avro.AvroSerDe", "HiveAvro"),
    HiveScanSerdeClasses("org.apache.hadoop.hive.serde2.OpenCSVSerde", "HiveCSV"),
    HiveScanSerdeClasses("org.apache.hadoop.hive.ql.io.orc.OrcSerde", "HiveORC")
  )

  def isHiveTableInsertNode(nodeName: String): Boolean = {
    nodeName.contains(INSERT_INTO_HIVE_LABEL)
  }

  def isHiveTableScanNode(nodeName: String): Boolean = {
    nodeName.toLowerCase.startsWith(SCAN_HIVE_LABEL)
  }

  def isHiveTableScanNode(node: SparkPlanGraphNode): Boolean = {
    isHiveTableScanNode(node.name)
  }

  def getHiveFormatFromSimpleStr(str: String): String = {
    LOADED_SERDE_CLASSES.find(_.accepts(str)).map(_.format).getOrElse("unknown")
  }

  // Given a "scan hive" NodeGraph, construct the MetaData based on the SerDe class.
  // If the SerDe class does not match the lookups, it returns an "unknown" format.
  def parseReadNode(node: SparkPlanGraphNode): ReadMetaData = {
    LOADED_SERDE_CLASSES.find(k => node.desc.contains(k.className)).map(
      _.parseReadNode(node)).getOrElse(ReadMetaData("", "HiveTableRelation", "unknown", "unknown"))
  }

  // Given a "scan hive" NodeGraph, construct the MetaData of the write operation based on the
  // SerDe class. If the SerDe class does not match the lookups, it returns an "unknown" format.
  def getWriteFormat(node: SparkPlanGraphNode): String = {
    val readMetaData = parseReadNode(node)
    readMetaData.format
  }

  def isHiveEnabled(properties: collection.Map[String, String]): Boolean = {
    EventUtils.isPropertyMatch(properties, "spark.sql.catalogImplementation", "", "hive")
  }

  // Keep for future improvement as we can pass this information to the AutoTuner/user to suggest
  // recommendations regarding ORC optimizations.
  def isORCNativeEnabled(properties: collection.Map[String, String]): Boolean = {
    EventUtils.isPropertyMatch(properties, "spark.sql.orc.impl", "native", "native") ||
      EventUtils.isPropertyMatch(properties, "spark.sql.hive.convertMetastoreOrc", "true", "true")
  }

  // Keep for future improvement as we can pass this information to the AutoTuner/user to suggest
  // recommendations regarding Parquet optimizations.
  def isConvertParquetEnabled(properties: collection.Map[String, String]): Boolean = {
    EventUtils.isPropertyMatch(properties, "spark.sql.hive.convertMetastoreParquet", "true", "true")
  }

  // Keep for future improvement as we can pass this information to the AutoTuner/user to suggest
  // recommendations regarding Text optimizations for GPU.
  def isRAPIDSTextHiveEnabled(properties: collection.Map[String, String]): Boolean = {
    EventUtils.isPropertyMatch(properties,
      "spark.rapids.sql.format.hive.text.enabled", "true", "true")
  }
}
