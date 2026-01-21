/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser.iceberg

import com.nvidia.spark.rapids.tool.planparser.{ExecInfo, GenericExecParser, SupportedOpStub}
import com.nvidia.spark.rapids.tool.planparser.ops.UnsupportedExprOpRef
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode

/**
 * A parser for Iceberg merge-related operations (MergeRows, ReplaceData).
 *
 * This parser handles Iceberg MERGE INTO operations:
 * - MergeRows: The intermediate exec that handles merge logic (keep, discard, split rows)
 * - ReplaceData: The write operator for copy-on-write mode
 *
 * DAG structure (copy-on-write mode):
 * {{{
 *   ReplaceData (12)           <- Write operator (CoW mode)
 *   +- Project (11)
 *      +- MergeRows (10)       <- Merge logic operator
 *         +- SortMergeJoin FullOuter (9)
 *            :- BatchScan (target table)
 *            +- BatchScan (source table)
 * }}}
 *
 * GPU support exists when data types are compatible:
 * - GpuMergeRows handles the merge logic
 * - GpuReplaceData handles the write
 *
 *
 * @param node the Spark plan graph node
 * @param checker the plugin type checker
 * @param sqlID SQL ID associated with the node
 * @param opStub the SupportedOpStub for this operation
 * @param app optional application context
 */
class MergeRowsIcebergParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    opStub: SupportedOpStub,
    override val app: Option[AppBase] = None
) extends GenericExecParser(
    node = node,
    checker = checker,
    sqlID = sqlID,
    execName = Some(opStub.execID),
    app = app) {

  override lazy val fullExecName: String = opStub.execID

  override def pullSpeedupFactor(registeredName: Option[String] = None): Double = 1.0

  override def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    // Support is determined by the opStub.isSupported flag.
    opStub.isSupported
  }

  // The value that will be reported as ExecName in the ExecInfo object created by this parser.
  override def reportedExecName: String = trimmedNodeName

  override def createExecInfo(
      speedupFactor: Double,
      isSupported: Boolean,
      duration: Option[Long],
      notSupportedExprs: Seq[UnsupportedExprOpRef],
      expressions: Array[String]): ExecInfo = {
    // We do not want to parse the node description to avoid mistakenly marking the node as RDD/UDF.
    ExecInfo.createExecNoNode(
      sqlID,
      exec = reportedExecName,
      expr = "",
      speedupFactor, duration, node.id,
      opType = opStub.pullOpType,
      isSupported = isSupported,
      children = None,
      unsupportedExecReason = unsupportedReason,
      expressions = Seq.empty
    )
  }
}
