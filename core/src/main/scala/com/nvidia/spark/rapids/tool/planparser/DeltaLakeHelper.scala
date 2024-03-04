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

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode

// A class used to handle the DL writeOps such as:
// - AppendDataExecV1
// - OverwriteByExpressionExecV1
// - SaveIntoDataSourceCommand
// This is not a Case class so later we can overwrite it.
class DLWriteWithFormatAndSchemaParser(node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long) extends ExecParser {
  // We use "DataWritingCommandExec" to get the speedupFactor of AppendDataExecV1
  val fullExecName: String = DataWritingCommandExecParser.dataWriteCMD
  override def parse: ExecInfo = {
    // The node description has information about the table schema and its format.
    // We do not want the op to me marked as RDD or UDF if the node description contains some
    // expressions that match UDF/RDD.
    val dataFormat = DeltaLakeHelper.getWriteFormat
    val writeSupported = checker.isWriteFormatSupported(dataFormat)
    val speedupFactor = if (writeSupported) {
      checker.getSpeedupFactor(fullExecName)
    } else {
      1.0
    }
    // execs like SaveIntoDataSourceCommand has prefix "Execute". So, we need to get rid of it.
    val nodeName = node.name.replace("Execute ", "")
    ExecInfo.createExecNoNode(sqlID, nodeName,
      s"Format: $dataFormat", speedupFactor, None, node.id, OpTypes.WriteExec,
      isSupported = writeSupported, children = None)
  }
}

object DeltaLakeHelper {
  // we look for the serdeLibrary which is part of the node description:
  // Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
  //private val serdeRegex = "Serde Library: ([\\w.]+)".r
  private val serdeRegex = "Serde Library: ([\\w.]+)".r
  // We look for the schema in the node description. It is in the following format
  // Schema: root
  //  |-- field_00: timestamp (nullable = true)
  //  |-- field_01: string (nullable = true)
  // the following regex matches lines that start with ( --|) and it will create two groups
  // [fieldName, type]
  private val schemaRegex =
    "\\s+\\|--\\s+([a-zA-Z]+(?:_[a-zA-Z]+)*):\\s+([a-zA-Z]+(?:_[a-zA-Z]+)*)\\s+\\(".r
  val saveIntoDataSrcCMD = "SaveIntoDataSourceCommand"
  private val atomicReplaceTableExec = "AtomicReplaceTableAsSelect"
  private val appendDataExecV1 = "AppendDataExecV1"
  private val overwriteByExprExecV1 = "OverwriteByExpressionExecV1"
  private val mergeIntoCommandEdgeExec = "MergeIntoCommandEdge"
  private val writeIntoDeltaCommandExec = "WriteIntoDeltaCommand"
  // Note that the SaveIntoDataSourceCommand node name appears as
  // "Execute SaveIntoDataSourceCommand"
  // Same for Execute MergeIntoCommandEdge
  private val exclusiveDeltaExecs = Set(
    saveIntoDataSrcCMD,
    mergeIntoCommandEdgeExec,
    writeIntoDeltaCommandExec)
  // define the list of writeExecs that also exist in Spark
  private val deltaExecsFromSpark = Set(
    appendDataExecV1,
    overwriteByExprExecV1,
    atomicReplaceTableExec)

  // keywords used to verify that the operator provider is DeltaLake
  private val nodeDescKeywords = Set(
    "DeltaTableV2",
    "WriteIntoDeltaBuilder")

  def accepts(nodeName: String): Boolean = {
    exclusiveDeltaExecs.exists(k => nodeName.contains(k)) || deltaExecsFromSpark.contains(nodeName)
  }

  def acceptsWriteOp(node: SparkPlanGraphNode): Boolean = {
    if (exclusiveDeltaExecs.exists(k => node.name.contains(k))) {
      true
    } else if (deltaExecsFromSpark.contains(node.name)) {
      if (node.name.contains(appendDataExecV1) || node.name.contains(overwriteByExprExecV1)) {
        nodeDescKeywords.forall(s => node.desc.contains(s))
      } else if (node.name.contains(atomicReplaceTableExec)) {
        // AtomicReplaceTableAsSelectExec has a different format
        // AtomicReplaceTableAsSelect [num_affected_rows#ID_0L, num_inserted_rows#ID_1L],
        // com.databricks.sql.managedcatalog.UnityCatalogV2Proxy@XXXXX,
        // DB.VAR_2, Union false, false,
        // TableSpec(Map(),Some(delta),Map(),None,None,None,false,Set(),None,None,None),
        // [replaceWhere=VAR_1 IN ('WHATEVER')], true,
        // org.apache.spark.sql.execution.datasources.
        // v2.DataSourceV2Strategy$$Lambda$XXXXX
        node.desc.matches("""(?i).*delta.*""")
      } else {
        false
      }
    } else {
      false
    }
  }

  def getWriteFormat: String = {
    // by default Delta Operators return write format
    "Delta"
  }

  def parseNode(node: SparkPlanGraphNode,
      checker: PluginTypeChecker,
      sqlID: Long): ExecInfo = {
    val opExec = new DLWriteWithFormatAndSchemaParser(node, checker, sqlID)
    opExec.parse
    node match {
      case n if acceptsWriteOp(n) =>
        val opExec = new DLWriteWithFormatAndSchemaParser(node, checker, sqlID)
        opExec.parse
      case _ => throw new IllegalArgumentException(s"Unhandled Exec for node ${node.name}")
    }
  }

  // Kept for future use if we find that SerDe library can be used to deduce any information to
  // reflect on the support of the Op
  def getSerdeLibrary(nodeDesc: String): Option[String] = {
    val matches = serdeRegex.findAllMatchIn(nodeDesc).toSeq
    if (matches.nonEmpty) {
      Option(matches.head.group(1))
    } else {
      None
    }
  }

  // Retrieve the schema of a DeltaLake table from some ops like Overwrite and Append
  def getSchema(nodeDesc: String): String = {
    schemaRegex.findAllMatchIn(nodeDesc).map { m =>
      s"${m.group(1)}:${m.group(2)}"
    }.mkString(",")
  }
}
