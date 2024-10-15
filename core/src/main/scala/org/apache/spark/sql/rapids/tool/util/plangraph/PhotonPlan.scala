/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.tool.util.plangraph

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.planparser.DatabricksParseHelper

import org.apache.spark.sql.execution.ui.{SparkPlanGraphCluster, SparkPlanGraphNode, SQLPlanMetric}

/**
 * Extension of SparkPlanGraphNode to handle Photon nodes.
 * Note:
 * - photonName and photonDesc are the name and description of the Photon node
 * - sparkName and sparkDesc are the name and description of the equivalent Spark node
 */
class PhotonSparkPlanGraphNode(
    id: Long,
    val photonName: String,
    val photonDesc: String,
    sparkName: String,
    sparkDesc: String,
    metrics: collection.Seq[SQLPlanMetric])
  extends SparkPlanGraphNode(id, sparkName, sparkDesc, metrics)

object PhotonSparkPlanGraphNode {
  def from(node: SparkPlanGraphNode): PhotonSparkPlanGraphNode = {
    val sparkName = DatabricksParseHelper.mapPhotonToSpark(node.name)
    val sparkDesc = DatabricksParseHelper.mapPhotonToSpark(node.desc)
    new PhotonSparkPlanGraphNode(node.id, node.name, node.desc, sparkName, sparkDesc, node.metrics)
  }
}

/**
 * Extension of SparkPlanGraphCluster to handle Photon nodes that are
 * mapped to WholeStageCodegen.
 * Note:
 * - photonName and photonDesc are the name and description of the Photon node
 * - name and desc are the name and description of the equivalent Spark node
 */
class PhotonSparkPlanGraphCluster(
    id: Long,
    val photonName: String,
    val photonDesc: String,
    sparkName: String,
    sparkDesc: String,
    nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
    metrics: collection.Seq[SQLPlanMetric])
  extends SparkPlanGraphCluster(id, sparkName, sparkDesc, nodes, metrics)

object PhotonSparkPlanGraphCluster {
    def from(cluster: SparkPlanGraphCluster): PhotonSparkPlanGraphCluster = {
      val sparkName = DatabricksParseHelper.mapPhotonToSpark(cluster.name)
      val sparkDesc = DatabricksParseHelper.mapPhotonToSpark(cluster.desc)
      new PhotonSparkPlanGraphCluster(cluster.id, cluster.name, cluster.desc, sparkName,
        sparkDesc, cluster.nodes, cluster.metrics)
    }
}
