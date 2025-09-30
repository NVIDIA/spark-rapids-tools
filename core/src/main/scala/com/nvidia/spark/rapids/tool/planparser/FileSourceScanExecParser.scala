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

import com.nvidia.spark.rapids.tool.planparser.delta.DeltaLakeHelper.{DELTALAKE_LOG_FILE_KEYWORD, DELTALAKE_LOG_KEYWORD}
import com.nvidia.spark.rapids.tool.planparser.ops.UnsupportedExprOpRef
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.{AppBase, RDDCheckHelper}
import org.apache.spark.sql.rapids.tool.util.CacheablePropsHandler

case class FileSourceScanExecParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    override val app: Option[AppBase]
) extends GenericExecParser(
    node = node,
    checker = checker,
    sqlID = sqlID,
    app = app
) with Logging {
  private var calculatedSpeedup: Double = 1.0
  // Hive table scans are also FileSourceScanExec nodes, but we want to
  // report them differently.
  // We detect them using the HiveParseHelper.
  private val isHiveScan: Boolean = HiveParseHelper.isHiveTableScanNode(node)
  // Matches the first alphaneumeric characters of a string after trimming leading/trailing
  // white spaces.
  private val nodeNameRegeX = """^\s*(\w+).*""".r
  // Record containing the parsed information from the read node.
  private lazy val readInfo: ReadMetaData = if (isHiveScan) {
    HiveParseHelper.parseReadNode(node)
  } else {
    ReadParser.parseReadNode(node)
  }
  // Caches the normalized format extracted from readInfo
  private lazy val readFormat: String = readInfo.getReadFormatLC

  // 1- Set the exec name to nodeLabel + format
  // 2- If the format is not found, then put the entire node description to make it easy to
  // troubleshoot by reading the output files.
  private lazy val scanNodeToken: String = nodeNameRegeX.findFirstMatchIn(trimmedNodeName) match {
    case Some(m) => m.group(1)
    // in case not found, use the full exec name
    case None => fullExecName
  }
  /**
   * Check if the scan operation is a Delta Lake metadata table scan.
   * Delta Lake metadata table scans are not supported.
   * @return true if it is a Delta Lake metadata table scan, false otherwise.
   */
  private lazy val isDeltaLakeMetadataScan: Boolean = {
    val appEnabled = app match {
      case None => true  // no app provided then we assume it is true to be safe.
      case Some(a) =>
        // If the app is provided, then check if delta lake is enabled or it is databricks
        // Databricks has delta lake built-in.
        a.deltaLakeEnabled || a.isDatabricks
    }
    appEnabled &&
      (node.desc.contains(DELTALAKE_LOG_KEYWORD) || node.desc.contains(DELTALAKE_LOG_FILE_KEYWORD))
  }

  /**
   * Check if the scan operation is supported.
   * Delta Lake metadata table scans are not supported.
   * @return true if supported, false otherwise
   */
  private def isScanOpSupported: Boolean = {
    if (isDeltaLakeMetadataScan) {
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
  def pullOpType: OpTypes.Value = if (isDeltaLakeMetadataScan) {
    OpTypes.ReadDeltaLog
  } else {
    OpTypes.ReadExec
  }

  // The node name for Scans is Scan <format> so here we hardcode
  override lazy val fullExecName: String = if (isHiveScan) {
    HiveParseHelper.SCAN_HIVE_EXEC_NAME
  } else {
    "FileSourceScanExec"
  }
  override protected def getDurationSqlMetrics: Set[String] = Set(
    "scan time",
    // delta lake metadata tables have a metric called metadata time.
    "metadata time")

  override def reportedExecName: String = s"$scanNodeToken $readFormat"

  /**
   * Calculate the speedup factor for the scan operation.
   * The speedup factor is calculated as:
   * speedupFactor = checker.getSpeedupFactor(fullExecName) * score
   * where score is the read score ratio calculated from the readInfo and the checker.
   * @param registeredName optional registered name to check
   * @return the speedup factor
   */
  override def pullSpeedupFactor(registeredName: Option[String] = None): Double = {
    val speedupFactor = checker.getSpeedupFactor(fullExecName)
    // don't use the isExecSupported because we have finer grain.
    val score = ReadParser.calculateReadScoreRatio(readInfo, checker)
    Math.max(speedupFactor * score, 1.0)
  }

  /**
   * Check if the scan operation is supported.
   * Delta Lake metadata table scans are not supported.
   * If the operation is supported, then check if the format and schema are supported.
   * If both are supported, then calculate the speedup factor.
   * If the calculated speedup factor is GT 1.0, then return true.
   * Otherwise, return false.
   * @param registeredName optional registered name to check
   * @return true if supported, false otherwise
   */
  override def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    if (checker.isExecSupported(registeredName.getOrElse(fullExecName)) && isScanOpSupported) {
      calculatedSpeedup = pullSpeedupFactor()
      // If calculated speedup factor is GT 1.0, then the format and the schema run on GPU.
      // Otherwise, it returns false.
      return calculatedSpeedup > 1.0
    }
    false
  }

  override def parse: ExecInfo = {
    // Remove trailing spaces from node name
    // Example: Scan parquet . ->  Scan parquet.
    val nodeName = trimmedNodeName
    val rddCheckRes = RDDCheckHelper.isDatasetOrRDDPlan(nodeName, node.desc)
    if (rddCheckRes.nodeNameRDD) {
      // This is a scanRDD. We do not need to parse it as a normal node.
      // cleanup the node name if possible:
      val newNodeName = if (nodeName.contains("ExistingRDD")) {
        val nodeNameLength = nodeName.indexOf("ExistingRDD") + "ExistingRDD".length
        nodeName.substring(0, nodeNameLength)
      } else {
        nodeName
      }
      ExecInfo.createExecNoNode(
        sqlID,
        newNodeName,
        "",
        1.0,
        duration = None,
        node.id,
        OpTypes.ReadRDD,
        isSupported = false,
        children = None,
        expressions = Seq.empty
      )
    } else {
      val duration = computeDuration
      val expressions = parseExpressions()
      val notSupportedExprs = getNotSupportedExprs(expressions)
      val isExecSupported = pullSupportedFlag() && notSupportedExprs.isEmpty
      createExecInfo(
        calculatedSpeedup,
        isExecSupported,
        duration,
        notSupportedExprs = notSupportedExprs,
        expressions = expressions)
    }
  }

  override protected def createExecInfo(
    speedupFactor: Double,
    isSupported: Boolean,
    duration: Option[Long],
    notSupportedExprs: Seq[UnsupportedExprOpRef],
    expressions: Array[String]
  ): ExecInfo = {
    val expr = if (readInfo.hasUnknownFormat) {
      node.desc
    } else {
      s"Format: $readFormat"
    }
    ExecInfo.createExecNoNode(
      sqlID = sqlID,
      exec = reportedExecName,
      expr = expr,
      speedupFactor = speedupFactor,
      duration = duration,
      nodeId = node.id,
      opType = pullOpType,
      isSupported = isSupported,
      children = getChildren,
      unsupportedExecReason = unsupportedReason,
      expressions = expressions)
  }
}

object FileSourceScanExecParser extends GroupParserTrait {
  val execName = "FileSourceScan"

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
