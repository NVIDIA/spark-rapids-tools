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

package org.apache.spark.sql.rapids.tool.util.stubs

import scala.collection.mutable

import org.apache.spark.sql.execution.ui.{SparkPlanGraphCluster, SparkPlanGraphEdge, SparkPlanGraphNode, SQLPlanMetric}

/**
 * Implementation that uses the SparkPlanGraphNode, SparkPlanGraphCluster, SparkPlanGraphEdge and
 * SQLPlanMetric classes from the Spark codebase. This API is used to reflectively construct these
 * classes at runtime.
 */
class DefaultGraphReflectionAPI extends GraphReflectionAPI {
  def constructNode(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    new SparkPlanGraphNode(id, name, desc, metrics)
  }

  def constructSQLPlanMetric(name: String,
      accumulatorId: Long,
      metricType: String): SQLPlanMetric = {
    SQLPlanMetric(name, accumulatorId, metricType)
  }

  def constructCluster(id: Long,
      name: String,
      desc: String,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphCluster = {
    new SparkPlanGraphCluster(id, name, desc, nodes, metrics)
  }

  def constructEdge(fromId: Long, toId: Long): SparkPlanGraphEdge = {
    SparkPlanGraphEdge(fromId, toId)
  }
}
