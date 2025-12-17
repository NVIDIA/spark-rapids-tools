/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser.photon

import com.nvidia.spark.rapids.tool.planparser._
import com.nvidia.spark.rapids.tool.planparser.SQLPlanParser.OssSparkPlanParserTrait
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.{PhotonSparkPlanGraphCluster, PhotonSparkPlanGraphNode, SparkPlanGraphNode}

/**
 * Parser for Photon execution plans from Databricks environments.
 *
 * Photon is Databricks' native vectorized query engine that replaces Spark's execution layer
 * with optimized operators. This parser handles Photon-specific plan structures and converts
 * them to their Spark CPU equivalents for analysis and qualification purposes.
 *
 * The parser supports:
 * - Photon-specific operators (e.g., PhotonProject, PhotonFilter)
 * - Photon-specific parsers for operators with unique behavior
 *   (e.g., PhotonBroadcastNestedLoopJoin)
 * - Photon cluster nodes representing stages (e.g., PhotonShuffleMapStage, PhotonResultStage)
 *
 * The conversion to Spark CPU equivalents enables:
 * - Consistent analysis across different execution engines
 * - Reuse of existing Spark operator support metadata
 * - Accurate speedup factor calculations for GPU acceleration recommendations
 */
object PhotonPlanParser extends OssSparkPlanParserTrait {

  /**
   * Checks if a graph node represents a Photon operator.
   *
   * @param node The SparkPlanGraphNode to check
   * @return true if the node is a PhotonSparkPlanGraphNode instance, false otherwise
   */
  def isPhotonNode(node: SparkPlanGraphNode): Boolean = {
    node.isInstanceOf[PhotonSparkPlanGraphNode]
  }

  /**
   * @inheritdoc
   *
   * Routes Photon operators with custom parsers to specialized Photon parsers,
   * or delegates to Spark CPU parser for standard operators since node name and
   * description are already converted to Spark equivalents.
   */
  override def parseGraphNode(
      node: SparkPlanGraphNode,
      sqlID: Long,
      checker: PluginTypeChecker,
      app: AppBase
  ): ExecInfo = {
    node.platformName match {
      case "PhotonBroadcastNestedLoopJoin" =>
        PhotonBroadcastNestedLoopJoinExecParser(node, checker, sqlID).parse
      case _ =>
        // If no photon specific parser is defined, parse using Spark CPU parser.
        // This is allowed because `node.name` and `node.desc` are set to the Spark CPU
        // operator's name and description.
        super.parseGraphNode(node, sqlID, checker, app)
    }
  }

  /**
   * @inheritdoc
   *
   * Handles Photon-specific cluster types (PhotonShuffleMapStage, PhotonResultStage,
   * PhotonUnionShuffleMapStage) by delegating to PhotonStageExecParser, or falls back
   * to parent Spark cluster parser for standard WholeStageCodegen nodes.
   */
  override def parseClusterNode(
      node: SparkPlanGraphNode,
      sqlID: Long,
      checker: PluginTypeChecker,
      app: AppBase,
      reusedNodeIds: Set[Long],
      nodeIdToStagesFunc: Long => Set[Int]
  ): Seq[ExecInfo] = {
    // It is possible to have non-photon cluster in an eventlog with photon enabled,
    // so we need to match the type here.
    node match {
      case photonCluster: PhotonSparkPlanGraphCluster =>
        PhotonStageExecParser(
          photonCluster,
          checker,
          sqlID,
          app,
          reusedNodeIds,
          nodeIdToStagesFunc = nodeIdToStagesFunc
        ).parse
      case _ =>
        // fall to the super class to handle non-Photon clusters
        super.parseClusterNode(
          node, sqlID, checker, app, reusedNodeIds, nodeIdToStagesFunc = nodeIdToStagesFunc)
    }
  }

  def acceptsCtxt(app: AppBase): Boolean = {
    app.isPhoton
  }

}
