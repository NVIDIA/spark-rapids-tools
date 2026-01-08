/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.tool.planparser.delta.DeltaLakeHelper.{DELTALAKE_LOG_FILE_KEYWORD, DELTALAKE_META_KEYWORD, DELTALAKE_META_SCAN_RDD_KEYWORD}
import com.nvidia.spark.rapids.tool.planparser.hive.HiveParseHelper
import com.nvidia.spark.rapids.tool.planparser.ops.OpTypes
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.{AppBase, RDDCheckHelper}
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.util.CacheablePropsHandler


case class FileSourceScanExecParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    override val app: Option[AppBase]
) extends BaseSourceScanExecParser(
    node = node,
    checker = checker,
    sqlID = sqlID,
    app = app
) with Logging {
  // The node name for Scans is Scan <format> so here we hardcode
  override lazy val fullExecName: String = if (isHiveScan) {
    HiveParseHelper.SCAN_HIVE_EXEC_NAME
  } else {
    "FileSourceScanExec"
  }

  // Record containing the parsed information from the read node.
  override lazy val readInfo: ReadMetaData = if (isHiveScan) {
    HiveParseHelper.parseReadNode(node)
  } else {
    ReadParser.parseReadNode(node)
  }

  // 1- Set the exec name to nodeLabel + format
  // 2- If the format is not found, then put the entire node description to make it easy to
  // troubleshoot by reading the output files.
  override lazy val scanNodeToken: String = nodeNameRegeX.findFirstMatchIn(trimmedNodeName) match {
    case Some(m) => m.group(1)
    // in case not found, use the full exec name
    case None => fullExecName
  }

  /**
   * Check if the scan operation is supported.
   * Delta Lake metadata table scans are not supported.
   * @return true if supported, false otherwise
   */
  override def isScanOpSupported: Boolean = {
    if (isDeltaLakeMetaScan) {
      // Delta Lake metadata table scans are not supported
      // No need to set the reason because it is set automatically inside the object ExecInfo
      false
    } else {
      true
    }
  }

  /**
   * Determine the operation type for the scan.
   * If it is a delta lake metadata table scan, then return ReadDeltaLog.
   * Otherwise, return ReadExec.
   * @return the operation type
   */
  override def pullOpType: OpTypes.Value = if (isDeltaLakeMetaScan) {
    OpTypes.ReadDeltaLog
  } else {
    super.pullOpType
  }

  /**
   * Duration metrics for file source scans.
   * See [[GenericExecParser.durationSqlMetrics]] for details.
   *
   * Includes both regular scan time and metadata time (for Delta Lake metadata scans).
   */
  override protected val durationSqlMetrics: Set[String] = Set(
    "scan time",
    // delta lake metadata tables have a metric called metadata time.
    "metadata time")

  /**
   * Determine the operation type for RDD scans.
   * If it is a delta lake metadata table scan, then return ReadRDDDeltaLog.
   * Otherwise, return ReadRDD.
   * A deltaLake scann RDD has known pattern in the node name or description.
   * nodeName: Scan ExistingRDD Delta Table State #0 - /path/to/_delta_log
   * nodeDesc: Scan ExistingRDD Delta Table State #0 - /path/to/_delta_log[...]
   * @return
   */
  private def pullOpTypeForRDD: OpTypes.Value = {
    if (isDeltaLakeMetaScanRDD) {
      OpTypes.ReadRDDDeltaLog
    } else {
      OpTypes.ReadRDD
    }
  }

  /**
   * Check if the scan operation is a Delta Lake metadata table scan.
   * Delta Lake metadata table scans are not supported.
   * @return true if it is a Delta Lake metadata table scan, false otherwise.
   */
  private lazy val isDeltaLakeMetaScan: Boolean = {
    val appEnabled = app match {
      case None => false  // no app provided then we assume it is false to be safe.
      case Some(a) =>
        // If the app is provided, then check if delta lake is enabled or it is databricks
        // Databricks has delta lake built-in.
        a.isDeltaLakeOSSEnabled || a.dbPlugin.isEnabled
    }
    appEnabled &&
      (node.desc.contains(DELTALAKE_META_KEYWORD) || node.desc.contains(DELTALAKE_LOG_FILE_KEYWORD))
  }

  private lazy val isDeltaLakeMetaScanRDD: Boolean = {
    val appEnabled = app match {
      case None => false  // no app provided then we assume it is false to be safe.
      case Some(a) =>
        // If the app is provided, then check if delta lake is enabled or it is databricks
        // Databricks has delta lake built-in.
        a.isDeltaLakeOSSEnabled || a.dbPlugin.isEnabled
    }
    appEnabled &&
      (node.name.contains(DELTALAKE_META_SCAN_RDD_KEYWORD) ||
        node.desc.contains(DELTALAKE_META_KEYWORD))
  }

  override def parse: ExecInfo = {
    // Remove trailing spaces from node name
    // Example: Scan parquet . ->  Scan parquet.
    val nodeName = trimmedNodeName
    val rddCheckRes = RDDCheckHelper.isDatasetOrRDDPlan(nodeName, node.desc)
    if (rddCheckRes.nodeNameRDD) {
      // This is a scanRDD. We do not need to parse it as a normal node.
      // cleanup the node name if possible:
      val (newNodeName, readType) = if (nodeName.contains("ExistingRDD")) {
        val nodeNameLength = nodeName.indexOf("ExistingRDD") + "ExistingRDD".length
        // At this point the node contains existing RDD, next we check if it is a deltaLake RDD
        // scan.
        (nodeName.substring(0, nodeNameLength), pullOpTypeForRDD)
      } else {
        // The node does not contain ExistingRDD. No need to check the deltaLake RDD scan
        (nodeName, OpTypes.ReadRDD)
      }
      ExecInfo.createExecNoNode(
        sqlID,
        newNodeName,
        "",
        1.0,
        duration = None,
        node.id,
        opType = readType,
        isSupported = false,
        children = None,
        expressions = Seq.empty
      )
    } else {
      parseNonRDDScan
    }
  }
}

object FileSourceScanExecParser extends GroupParserTrait {

  override def accepts(nodeName: String): Boolean = {
    ReadParser.isScanNode(nodeName)
  }

  override def accepts(
    nodeName: String,
    confProvider: Option[CacheablePropsHandler]): Boolean = {
    accepts(nodeName)
  }

  override def accepts(
      node: SparkPlanGraphNode,
      confProvider: Option[CacheablePropsHandler]): Boolean = {
    accepts(node.name, confProvider)
  }

  /**
   * Create an ExecParser for the given node.
   *
   * @param node     spark plan graph node
   * @param checker  plugin type checker
   * @param sqlID    SQL ID
   * @param execName optional exec name override
   * @param opType   optional op type override
   * @param app      optional AppBase instance
   * @return an ExecParser for the given node
   */
  override def createExecParser(
      node: SparkPlanGraphNode,
      checker: PluginTypeChecker,
      sqlID: Long,
      execName: Option[String] = None,
      opType: Option[OpTypes.Value] = None,
      app: Option[AppBase]): ExecParser = {
    FileSourceScanExecParser(
      node = node,
      checker = checker,
      sqlID = sqlID,
      app = app
    )
  }
}
