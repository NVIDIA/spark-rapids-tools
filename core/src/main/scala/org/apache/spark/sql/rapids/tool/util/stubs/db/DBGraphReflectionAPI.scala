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

package org.apache.spark.sql.rapids.tool.util.stubs.db

import scala.collection.mutable

import org.apache.spark.sql.execution.ui.{SparkPlanGraphCluster, SparkPlanGraphEdge, SparkPlanGraphNode, SQLPlanMetric}
import org.apache.spark.sql.rapids.tool.annotation.ToolsReflection
import org.apache.spark.sql.rapids.tool.util.stubs.DefaultGraphReflectionAPI

/**
 * Implementation that uses the SparkPlanGraphNode, SparkPlanGraphCluster, SparkPlanGraphEdge and
 * SQLPlanMetric classes from the DataBricks codeBase.
 * This API is used to reflectively construct these classes at runtime.
 */
@ToolsReflection("DataBricks", "DB defines different constructors for SparkGraphComponents")
case class DBGraphReflectionAPI() extends DefaultGraphReflectionAPI {
  private val nodeStub = DBGraphNodeStub(mirror)
  private val clusterStub = DBGraphClusterStub(mirror)
  private val edgeStub = DBGraphEdgeStub(mirror)
  private val metricStub = DBGraphSQLMetricStub(mirror)

  override def constructNode(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    nodeStub.createInstance(id, name, desc, metrics)
  }

  override def constructSQLPlanMetric(name: String,
      accumulatorId: Long,
      metricType: String): SQLPlanMetric = {
    metricStub.createInstance(name, accumulatorId, metricType)
  }

  override def constructCluster(id: Long,
      name: String,
      desc: String,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphCluster = {
    clusterStub.createInstance(id, name, desc, nodes, metrics)
  }

  override def constructEdge(fromId: Long, toId: Long): SparkPlanGraphEdge = {
    edgeStub.createInstance(fromId, toId)
  }
}
