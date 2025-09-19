/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser.delta

import com.nvidia.spark.rapids.tool.planparser.{ExecParser, GroupParserTrait, OpTypes}
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.AppBase

/**
 * A stub class to hold the static information about a Delta Lake operator.
 * @param nodeName the exact name of the SparkPlanGraphNode
 * @param isSupported whether this operator is supported in the plugin
 * @param opType the type of the operator, default to OpTypes.Exec
 * @param sqlMetricNames the set of SQL metric names to be used to compute the duration
 * @param deltaSpecific whether this operator is specific to Delta Lake. Default to true.
 */
case class SupportedOpStub(
    nodeName: String,
    isSupported: Boolean = true,
    opType: Option[OpTypes.Value] = Option(OpTypes.Exec),
    sqlMetricNames: Set[String] = Set.empty,
    deltaSpecific: Boolean = true) {
  // The name without the "Execute " prefix if any.
  val execNoPrefix: String = if (nodeName.startsWith("Execute ")) {
      nodeName.stripPrefix("Execute ")
    } else {
      nodeName
    }
  // The ID used by the plugin. This is useful to match with the PluginTypeChecker.
  val execID: String = {
    execNoPrefix + "Exec"
  }
  // Get the OpType, default to OpTypes.Exec
  def pullOpType: OpTypes.Value = {
    opType match {
      case Some(v) => v
      case None => OpTypes.Exec
    }
  }
}

/**
 * Delta Lake operators.
 * Later, we need to improve this by defining a csv/conf file to load the static list of operators
 * in Delta Lake. And we also need a clear separation between OSS Delta Lake and Databricks.
 */
object DeltaLakeOps extends GroupParserTrait {
  // DeltaLakeOperations
  // Execs that have suffix "Edge" are Databricks specific implementations.
  private val execUpdateCMD = "Execute UpdateCommand"
  private val execUpdateCMDEdge = "Execute UpdateCommandEdge"
  private val execDeleteCMD = "Execute DeleteCommand"
  private val execDeleteCMDEdge = "Execute DeleteCommandEdge"
  private val execMergeIntoCMDEdge = "Execute MergeIntoCommandEdge"
  private val execMergeIntoCMD = "Execute MergeIntoCommand"
  private val execOptimizeTableCMDEdge = "Execute OptimizeTableCommandEdge"
  private val execOptimizeTableCMD = "Execute OptimizeTableCommand"
  private val execWriteIntoDeltaCMD = "Execute WriteIntoDeltaCommand"
  val execSaveIntoDataSourceCMD = "Execute SaveIntoDataSourceCommand"
  val execDescribeDeltaHistoryCMD = "Execute DescribeDeltaHistoryCommand"
  val execShowPartitionsDeltaCMD = "Execute ShowPartitionsDeltaCommand"

  // A Map between the spark node name and the SupportedOpStub
  private val SUPPORTED_EXECS: Map[String, SupportedOpStub] = Map(
    execUpdateCMD ->
      SupportedOpStub(
        execUpdateCMD,
        opType = Option(OpTypes.WriteExec),
        sqlMetricNames = Set("time taken to execute the entire operation")),
    execUpdateCMDEdge ->
      SupportedOpStub(execUpdateCMDEdge, opType = Option(OpTypes.WriteExec)),
    execDeleteCMD ->
      SupportedOpStub(
        execDeleteCMD,
        opType = Option(OpTypes.WriteExec),
        sqlMetricNames = Set("time taken to execute the entire operation")),
    execDeleteCMDEdge ->
      SupportedOpStub(execDeleteCMDEdge, opType = Option(OpTypes.WriteExec)),
    execMergeIntoCMD ->
      SupportedOpStub(
        execMergeIntoCMD,
        opType = Option(OpTypes.WriteExec),
        sqlMetricNames = Set("time taken to execute the entire operation")),
    execMergeIntoCMDEdge ->
      SupportedOpStub(execMergeIntoCMDEdge, opType = Option(OpTypes.WriteExec)),
    execOptimizeTableCMD ->
      SupportedOpStub(execOptimizeTableCMD, opType = Option(OpTypes.WriteExec)),
    execOptimizeTableCMDEdge ->
      SupportedOpStub(execOptimizeTableCMDEdge, opType = Option(OpTypes.WriteExec)),
    // Exclusive to Delta Lake OSS
    execSaveIntoDataSourceCMD ->
      SupportedOpStub(execSaveIntoDataSourceCMD, opType = Option(OpTypes.WriteExec)),
    execWriteIntoDeltaCMD ->
      SupportedOpStub(execWriteIntoDeltaCMD, opType = Option(OpTypes.WriteExec)),
    execDescribeDeltaHistoryCMD ->
      SupportedOpStub(execDescribeDeltaHistoryCMD, isSupported = false),
    execShowPartitionsDeltaCMD ->
      SupportedOpStub(execShowPartitionsDeltaCMD, isSupported = false)
  )

  private def getOpStub(sparkPlanGraphNode: SparkPlanGraphNode): SupportedOpStub = {
    SUPPORTED_EXECS.get(sparkPlanGraphNode.name) match {
      case Some(v) => v
      case None => SupportedOpStub(sparkPlanGraphNode.name, isSupported = false)
    }
  }

  def getExecNoPrefix(opKey: String): Option[String] = {
    SUPPORTED_EXECS.get(opKey).map(_.execNoPrefix)
  }

  def isExclusiveDeltaWriteOp(nodeName: String): Boolean = {
    SUPPORTED_EXECS.get(nodeName) match {
      case Some(v) => v.opType.contains(OpTypes.WriteExec)
      case None => false
    }
  }

  override def accepts(nodeName: String): Boolean = {
    SUPPORTED_EXECS.contains(nodeName)
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
      execName: Option[String],
      opType: Option[OpTypes.Value],
      app: Option[AppBase]): ExecParser = {
    new DeltaLakeBlankExec(
      node = node,
      checker = checker,
      sqlID = sqlID,
      opStub = getOpStub(node),
      app = app)
  }
}
