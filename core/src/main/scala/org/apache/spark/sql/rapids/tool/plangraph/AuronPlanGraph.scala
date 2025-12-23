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

package org.apache.spark.sql.rapids.tool.plangraph

import scala.collection.mutable

/**
 * Platform-specific SparkPlanGraphNode for Auron execution plans.
 *
 * @param id Unique identifier for the node.
 * @param actualName The actual platform-specific name of the node.
 * @param actualDesc The actual platform-specific description of the node.
 * @param sparkName The equivalent Spark name of the node.
 * @param sparkDesc The equivalent Spark description of the node.
 * @param metrics Metrics associated with this node.
 */
case class AuronSparkPlanGraphNode(
  override val id: Long,
  actualName: String,
  actualDesc: String,
  sparkName: String,
  sparkDesc: String,
  override val metrics: collection.Seq[SQLPlanMetric]
) extends PWPlanGraphNode(id, actualName, actualDesc, sparkName, sparkDesc, metrics)

/**
 * Platform-specific SparkPlanGraphCluster for auron execution plans.
 *
 * @param id Unique identifier for the cluster node.
 * @param actualName The actual platform-specific name of the cluster node.
 * @param actualDesc The actual platform-specific description of the cluster node.
 * @param sparkName The equivalent Spark name of the cluster node.
 * @param sparkDesc The equivalent Spark description of the cluster node.
 * @param nodes Child nodes contained within this cluster.
 * @param metrics Metrics associated with this cluster node.
 */
case class AuronSparkPlanGraphCluster(
  override val id: Long,
  actualName: String,
  actualDesc: String,
  sparkName: String,
  sparkDesc: String,
  override val nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
  override val metrics: collection.Seq[SQLPlanMetric]
) extends PWPlanGraphCluster(id, actualName, actualDesc, sparkName, sparkDesc, nodes, metrics)
