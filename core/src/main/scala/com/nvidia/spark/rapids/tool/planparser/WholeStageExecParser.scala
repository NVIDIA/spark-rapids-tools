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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphCluster

abstract class WholeStageExecParserBase(
    override val node: SparkPlanGraphCluster,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    val appInst: AppBase,
    reusedNodeIds: Set[Long],
    nodeIdToStagesFunc: Long => Set[Int]
) extends GenericExecParser(
    node,
    checker,
    sqlID,
    execName = Some("WholeStageCodegenExec"),
    app = Some(appInst)
) {
  // Matches the first alphanumeric characters of a string after trimming leading/trailing
  // white spaces.
  val nodeNameRegeX = """^\s*(\w+).*""".r

  /**
   * Duration metric for whole stage codegen execution.
   * See [[GenericExecParser.durationSqlMetrics]] for details.
   */
  override protected val durationSqlMetrics: Set[String] = Set("duration")

  /**
   * Creates the appropriate SQL plan parser for parsing child operators within this cluster.
   *
   * Delegates to SQLPlanParser.createParserAgent to select the correct parser based on the
   * application's execution engine (PhotonPlanParser for Photon apps, OssSQLPlanParser for
   * standard Spark). This ensures child operators are parsed using the same platform-specific
   * logic as their parent cluster.
   *
   * @return SQLPlanParserTrait instance (PhotonPlanParser or OssSQLPlanParser)
   */
  def createPlanParserObj: SQLPlanParserTrait = {
    SQLPlanParser.createParserAgent(appInst)
  }

  /**
   * Returns the cluster node's display name for the expression field.
   *
   * For standard WholeStageCodegen clusters, this returns the node name (e.g.,
   * "WholeStageCodegen (3)"). Platform-specific implementations may override this
   * to return platform-specific names (e.g., "PhotonShuffleMapStage" for Photon).
   *
   * @return The node's name as the pretty expression
   */
  override def reportedExpr: String = node.name

  var childNodes: ArrayBuffer[ExecInfo] = ArrayBuffer.empty[ExecInfo]

  def populateChildNodes(): Unit = {
    val objParser = createPlanParserObj
    childNodes = node.nodes.flatMap { c =>
      // Pass the nodeToStagesFunc to the child nodes so they can get the stages.
      objParser.parsePlanNode(c, sqlID, checker, appInst, reusedNodeIds,
        nodeIdToStagesFunc = nodeIdToStagesFunc)
    }
  }

  override def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    childNodes.exists(_.isSupported == true)
  }

  override def getChildren: Option[Seq[ExecInfo]] = {
    if (childNodes.isEmpty) {
      None
    } else {
      Some(childNodes.toSeq)
    }
  }

  override def pullSpeedupFactor(registeredName: Option[String] = None): Double = {
    // average speedup across the execs in the WholeStageCodegen for now.
    SQLPlanParser.averageSpeedup(
      childNodes.filterNot(_.shouldRemove).map(_.speedupFactor).toSeq)
  }

  override def reportedExecName: String = {
    // Remove any suffix to get the node label without any trailing number.
    nodeNameRegeX.findFirstMatchIn(node.name) match {
      case Some(m) => m.group(1)
      // in case not found, use the full exec name
      case None => fullExecName
    }
  }

  override def parse: ExecInfo = {
    // TODO - does metrics for time have previous ops?  per op thing, only some do
    // the durations in wholestage code gen can include durations of other wholestage code
    // gen in the same stage, so we can't just add them all up.
    // Perhaps take the max of those in Stage?
    val duration = computeDuration
    val stagesInNode = nodeIdToStagesFunc.apply(node.id)
    // We could skip the entire wholeStage if it is duplicate; but we will lose the information of
    // the children nodes.
    val isDupNode = reusedNodeIds.contains(node.id)
    populateChildNodes()
    val isExecSupported = pullSupportedFlag()
    val (speedupFactor, isSupported) = if (isExecSupported) {
      (pullSpeedupFactor(), true)
    } else {
      // Set the custom reasons for unsupported execs
      setUnsupportedReasonFromChecker()
      (1.0, false)
    }

    // The node should be marked as shouldRemove when all the children of the
    // wholeStageCodeGen are marked as shouldRemove.
    val removeNode = isDupNode || childNodes.forall(_.shouldRemove)

    val execInfo = ExecInfo(
      node = node,
      sqlID = sqlID,
      exec = reportedExecName,
      expr = reportedExpr,
      speedupFactor = speedupFactor,
      duration = duration,
      nodeId = node.id,
      isSupported = isSupported,
      children = Some(childNodes.toSeq),
      stages = stagesInNode,
      shouldRemove = removeNode,
      // unsupported expressions should not be set for the cluster nodes.
      unsupportedExprs = Seq.empty,
      // expressions of wholeStageCodeGen should not be set. They belong to the children nodes.
      expressions = Seq.empty
    ).withClusterFlag()  // Mark as cluster node for proper identification in analysis
    execInfo
  }
}

case class WholeStageExecParser(
    override val node: SparkPlanGraphCluster,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    override val appInst: AppBase,
    reusedNodeIds: Set[Long],
    nodeIdToStagesFunc: Long => Set[Int]
) extends WholeStageExecParserBase(node, checker, sqlID, appInst, reusedNodeIds, nodeIdToStagesFunc)
