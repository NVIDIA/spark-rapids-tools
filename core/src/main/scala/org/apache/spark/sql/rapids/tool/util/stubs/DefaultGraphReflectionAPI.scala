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

package org.apache.spark.sql.rapids.tool.util.stubs

import scala.collection.mutable

import org.apache.spark.sql.rapids.tool.plangraph.{AuronSparkPlanGraphCluster, AuronSparkPlanGraphNode, PhotonSparkPlanGraphCluster, PhotonSparkPlanGraphNode, RAPIDSSparkPlanGraphCluster, RAPIDSSparkPlanGraphNode, SparkPlanGraphCluster, SparkPlanGraphEdge, SparkPlanGraphNode, SQLPlanMetric}
import org.apache.spark.sql.rapids.tool.util.stubs.auron.AuronSparkPlanInfo
import org.apache.spark.sql.rapids.tool.util.stubs.db.PhotonSparkPlanInfo
import org.apache.spark.sql.rapids.tool.util.stubs.rapids.RAPIDSSparkPlanInfo

/**
 * Implementation that uses the SparkPlanGraphNode, SparkPlanGraphCluster, SparkPlanGraphEdge and
 * SQLPlanMetric classes from the Spark codebase. This API is used to reflectively construct these
 * classes at runtime.
 */
class DefaultGraphReflectionAPI extends GraphReflectionAPI {
  /**
   * @inheritdoc
   *
   * Note: This method is for testing only. Production code should use the planInfo-based overload.
   */
  def constructNode(
      id: Long,
      name: String,
      desc: String,
      metrics: collection.Seq[SQLPlanMetric]
  ): SparkPlanGraphNode = {
    new SparkPlanGraphNode(id, name, desc, metrics)
  }

  /**
   * @inheritdoc
   *
   * Implementation routes to platform-specific node constructors:
   * - Photon: Uses actualName/actualDesc (platform-specific) and sparkName/sparkDesc
   *   (CPU equivalent)
   * - RAPIDS: Uses actualName/actualDesc for both (since RAPIDS nodes map directly to Spark names)
   * - Default: Uses platformName/platformDesc from the base planInfo
   */
  override def constructNode(
      id: Long,
      planInfo: SparkPlanInfo,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    planInfo match {
      case aP: AuronSparkPlanInfo =>
        AuronSparkPlanGraphNode(
          id,
          aP.actualName,
          aP.actualDesc,
          aP.sparkName,
          aP.sparkDesc,
          metrics)
      case pI: PhotonSparkPlanInfo =>
        PhotonSparkPlanGraphNode(
          id,
          pI.actualName,
          pI.actualDesc,
          pI.sparkName,
          pI.sparkDesc,
          metrics)
      case pI: RAPIDSSparkPlanInfo =>
        RAPIDSSparkPlanGraphNode(
          id,
          pI.actualName,
          pI.actualDesc,
          pI.actualName,
          pI.actualDesc,
          metrics)
      case _ =>
        new SparkPlanGraphNode(id, planInfo.platformName, planInfo.platformDesc, metrics)
    }
  }

  /**
   * @inheritdoc
   *
   * Implementation routes to platform-specific cluster constructors:
   * - Photon: Uses actualName/actualDesc (e.g., "PhotonShuffleMapStage") and sparkName/sparkDesc
   * - RAPIDS: Uses actualName/actualDesc for both platform and Spark representations
   * - Default: Uses platformName/platformDesc from the base planInfo
   */
  def constructCluster(
      id: Long,
      planInfo: SparkPlanInfo,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]
  ): SparkPlanGraphCluster = {
    planInfo match {
      case pI: AuronSparkPlanInfo =>
        AuronSparkPlanGraphCluster(
          id,
          pI.actualName,
          pI.actualDesc,
          pI.sparkName,
          pI.sparkDesc,
          nodes,
          metrics)
      case pI: PhotonSparkPlanInfo =>
        PhotonSparkPlanGraphCluster(
          id,
          pI.actualName,
          pI.actualDesc,
          pI.sparkName,
          pI.sparkDesc,
          nodes,
          metrics)
      case pI: RAPIDSSparkPlanInfo =>
        // We should not really be here. RAPIDS plans have no cluster nodes.
        RAPIDSSparkPlanGraphCluster(
          id,
          pI.actualName,
          pI.actualDesc,
          pI.sparkName,
          pI.sparkDesc,
          nodes,
          metrics)
      case _ =>
        new SparkPlanGraphCluster(id, planInfo.platformName, planInfo.platformDesc, nodes, metrics)
    }
  }

  def constructCluster(id: Long,
      name: String,
      desc: String,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]
  ): SparkPlanGraphCluster = {
    new SparkPlanGraphCluster(id, name, desc, nodes, metrics)
  }

  def constructSQLPlanMetric(name: String,
      accumulatorId: Long,
      metricType: String
  ): SQLPlanMetric = {
    SQLPlanMetric(name, accumulatorId, metricType)
  }

  def constructEdge(fromId: Long, toId: Long): SparkPlanGraphEdge = {
    SparkPlanGraphEdge(fromId, toId)
  }
}
