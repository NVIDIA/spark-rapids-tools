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
import org.apache.spark.sql.execution.ui.SparkPlanGraphCluster
import org.apache.spark.sql.rapids.tool.AppBase

abstract class WholeStageExecParserBase(
    node: SparkPlanGraphCluster,
    checker: PluginTypeChecker,
    sqlID: Long,
    app: AppBase,
    reusedNodeIds: Set[Long],
    nodeIdToStagesFunc: Long => Set[Int]) extends Logging {

  val fullExecName = "WholeStageCodegenExec"
  // Matches the first alphanumeric characters of a string after trimming leading/trailing
  // white spaces.
  val nodeNameRegeX = """^\s*(\w+).*""".r

  def parse: Seq[ExecInfo] = {
    // TODO - does metrics for time have previous ops?  per op thing, only some do
    // the durations in wholestage code gen can include durations of other wholestage code
    // gen in the same stage, so we can't just add them all up.
    // Perhaps take the max of those in Stage?
    val accumId = node.metrics.find(_.name == "duration").map(_.accumulatorId)
    val maxDuration = SQLPlanParser.getTotalDuration(accumId, app)
    val stagesInNode = nodeIdToStagesFunc.apply(node.id)
    // We could skip the entire wholeStage if it is duplicate; but we will lose the information of
    // the children nodes.
    val isDupNode = reusedNodeIds.contains(node.id)
    val childNodes = node.nodes.flatMap { c =>
      // Pass the nodeToStagesFunc to the child nodes so they can get the stages.
      SQLPlanParser.parsePlanNode(c, sqlID, checker, app, reusedNodeIds,
        nodeIdToStagesFunc = nodeIdToStagesFunc)
    }
    // if any of the execs in WholeStageCodegen supported mark this entire thing as supported
    val anySupported = childNodes.exists(_.isSupported == true)
    // average speedup across the execs in the WholeStageCodegen for now
    val supportedChildren = childNodes.filterNot(_.shouldRemove)
    val avSpeedupFactor = SQLPlanParser.averageSpeedup(supportedChildren.map(_.speedupFactor))
    // The node should be marked as shouldRemove when all the children of the
    // wholeStageCodeGen are marked as shouldRemove.
    val removeNode = isDupNode || childNodes.forall(_.shouldRemove)
    // Remove any suffix to get the node label without any trailing number.
    val nodeLabel = nodeNameRegeX.findFirstMatchIn(node.name) match {
      case Some(m) => m.group(1)
      // in case not found, use the full exec name
      case None => fullExecName
    }
    val execInfo = ExecInfo(
      node = node,
      sqlID = sqlID,
      exec = nodeLabel,
      expr = node.name,
      speedupFactor = avSpeedupFactor,
      duration = maxDuration,
      nodeId = node.id,
      isSupported = anySupported,
      children = Some(childNodes),
      stages = stagesInNode,
      shouldRemove = removeNode,
      // unsupported expressions should not be set for the cluster nodes.
      unsupportedExprs = Seq.empty,
      // expressions of wholeStageCodeGen should not be set. They belong to the children nodes.
      expressions = Seq.empty)
    Seq(execInfo)
  }
}

case class WholeStageExecParser(
  node: SparkPlanGraphCluster,
  checker: PluginTypeChecker,
  sqlID: Long,
  app: AppBase,
  reusedNodeIds: Set[Long],
  nodeIdToStagesFunc: Long => Set[Int])
  extends WholeStageExecParserBase(node, checker, sqlID, app, reusedNodeIds, nodeIdToStagesFunc)
