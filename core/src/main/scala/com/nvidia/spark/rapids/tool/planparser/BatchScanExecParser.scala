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
import org.apache.spark.sql.rapids.tool.AppBase

case class BatchScanExecParser(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long,
    app: AppBase) extends ExecParser with Logging {
  val nodeName = "BatchScan"
  val fullExecName = nodeName + "Exec"

  override def parse: ExecInfo = {
    val accumId = node.metrics.find(_.name == "scan time").map(_.accumulatorId)
    val maxDuration = SQLPlanParser.getTotalDuration(accumId, app)
    val readInfo = ReadParser.parseReadNode(node)
    // don't use the isExecSupported because we have finer grain.
    val score = ReadParser.calculateReadScoreRatio(readInfo, checker)
    val speedupFactor = checker.getSpeedupFactor(fullExecName)
    val overallSpeedup = Math.max((speedupFactor * score), 1.0)

    // 1- Set the exec name to be the batchScan + format
    // 2- If the format cannot be found, then put the entire node description to make it easy to
    // troubleshoot by reading the output files.
    val readFormat = readInfo.getReadFormatLC
    val execExpression = if (readInfo.hasUnknownFormat) {
      node.desc
    } else {
      s"Format: $readFormat"
    }

    ExecInfo.createExecNoNode(
      sqlID = sqlID,
      exec = s"$nodeName $readFormat",
      expr = execExpression,
      speedupFactor = overallSpeedup,
      duration = maxDuration,
      nodeId = node.id,
      opType = OpTypes.ReadExec,
      isSupported = score > 0.0,
      children = None,
      expressions = Seq.empty)
  }
}
