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
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.runtimeMirror

import org.apache.spark.sql.rapids.tool.plangraph.{SparkPlanGraphCluster, SparkPlanGraphEdge, SparkPlanGraphNode, SQLPlanMetric}

/**
 * API to define methods used to construct SparkPlanGraphNode, SparkPlanGraphCluster,
 * SparkPlanGraphEdge and SQLPlanMetric objects at runtime.
 */
trait GraphReflectionAPI {
  protected val mirror: universe.Mirror = runtimeMirror(getClass.getClassLoader)

  /**
   * Constructs a basic SparkPlanGraphNode with explicit name and description.
   *
   * This overload is primarily for testing purposes and should not be used in production code.
   * Use the planInfo-based overload instead to ensure proper platform-aware node construction.
   *
   * @param id Unique identifier for the node
   * @param name Operator name
   * @param desc Operator description
   * @param metrics Performance metrics for the node
   * @return Basic SparkPlanGraphNode instance
   */
  def constructNode(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode

  /**
   * Constructs a platform-aware node for the execution plan graph.
   *
   * Creates a SparkPlanGraphNode (or platform-specific subclass) for a single operator.
   * The implementation selects the appropriate node type based on the planInfo's platform:
   * - PhotonSparkPlanInfo → PhotonSparkPlanGraphNode (preserves Photon + Spark names)
   * - RAPIDSSparkPlanInfo → RAPIDSSparkPlanGraphNode (preserves RAPIDS + Spark names)
   * - Default → SparkPlanGraphNode (standard Spark node)
   *
   * @param id Unique identifier for the node
   * @param planInfo Platform-aware SparkPlanInfo containing operator metadata
   * @param metrics Performance metrics for the node
   * @return Platform-specific SparkPlanGraphNode instance
   */
  def constructNode(
      id: Long,
      planInfo: SparkPlanInfo,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode

  def constructSQLPlanMetric(name: String,
      accumulatorId: Long,
      metricType: String): SQLPlanMetric

  def constructCluster(id: Long,
      name: String,
      desc: String,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphCluster

  /**
   * Constructs a platform-aware cluster node for the execution plan graph.
   *
   * Creates a SparkPlanGraphCluster (or platform-specific subclass) that groups multiple
   * operators executing together. The implementation selects the appropriate cluster type
   * based on the planInfo's platform:
   * - PhotonSparkPlanInfo → PhotonSparkPlanGraphCluster (preserves Photon + Spark names)
   * - RAPIDSSparkPlanInfo → RAPIDSSparkPlanGraphCluster (preserves RAPIDS + Spark names)
   * - Default → SparkPlanGraphCluster (standard Spark cluster)
   *
   * @param id Unique identifier for the cluster node
   * @param planInfo Platform-aware SparkPlanInfo containing operator metadata
   * @param nodes Child operators within this cluster
   * @param metrics Performance metrics for the cluster
   * @return Platform-specific SparkPlanGraphCluster instance
   */
  def constructCluster(
      id: Long,
      planInfo: SparkPlanInfo,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphCluster

  def constructEdge(fromId: Long, toId: Long): SparkPlanGraphEdge
}
