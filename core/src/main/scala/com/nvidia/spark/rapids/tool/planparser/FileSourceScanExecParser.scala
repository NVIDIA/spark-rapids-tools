/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.{AppBase, RDDCheckHelper}

case class FileSourceScanExecParser(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long,
    app: AppBase) extends ExecParser with Logging {

  // The node name for Scans is Scan <format> so here we hardcode
  val fullExecName = "FileSourceScanExec"
  val execNameRef = ExecRef.getOrCreate(fullExecName)

  override def parse: ExecInfo = {
    // Remove trailing spaces from node name
    // Example: Scan parquet . ->  Scan parquet.
    val nodeName = node.name.trim
    val rddCheckRes = RDDCheckHelper.isDatasetOrRDDPlan(nodeName, node.desc)
    if (rddCheckRes.nodeNameRDD) {
      // This is a scanRDD. we do not need to parse it as a normal node.
      // cleanup the node name if possible:
      val newNodeName = if (nodeName.contains("ExistingRDD")) {
        val nodeNameLength = nodeName.indexOf("ExistingRDD") + "ExistingRDD".length
        nodeName.substring(0, nodeNameLength)
      } else {
        nodeName
      }
      val nameRef = ExecRef.getOrCreate(newNodeName)
      ExecInfo.createExecNoNode(sqlID, newNodeName, "", 1.0, duration = None,
        node.id, OpTypes.ReadRDD, false, None, execRef = nameRef)
    } else {
      val accumId = node.metrics.find(_.name == "scan time").map(_.accumulatorId)
      val maxDuration = SQLPlanParser.getTotalDuration(accumId, app)
      val (execName, readInfo) = if (HiveParseHelper.isHiveTableScanNode(node)) {
        // Use the hive parser
        (HiveParseHelper.SCAN_HIVE_EXEC_NAME, HiveParseHelper.parseReadNode(node))
      } else {
        // Use the default parser
        (fullExecName, ReadParser.parseReadNode(node))
      }
      val speedupFactor = checker.getSpeedupFactor(execName)
      // don't use the isExecSupported because we have finer grain.
      val score = ReadParser.calculateReadScoreRatio(readInfo, checker)
      val overallSpeedup = Math.max(speedupFactor * score, 1.0)

      ExecInfo.createExecNoNode(
        sqlID = sqlID,
        exec = nodeName,
        expr = "",
        speedupFactor = overallSpeedup,
        duration = maxDuration,
        nodeId = node.id,
        opType = OpTypes.ReadExec,
        isSupported = score > 0,
        execRef = execNameRef)
    }
  }
}
