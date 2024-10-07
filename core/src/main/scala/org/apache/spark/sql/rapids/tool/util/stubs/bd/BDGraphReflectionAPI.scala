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

package org.apache.spark.sql.rapids.tool.util.stubs.bd

import scala.collection.mutable

import org.apache.spark.sql.execution.ui.{SparkPlanGraphCluster, SparkPlanGraphNode, SQLPlanMetric}
import org.apache.spark.sql.rapids.tool.annotation.ToolsReflection
import org.apache.spark.sql.rapids.tool.util.stubs.DefaultGraphReflectionAPI

@ToolsReflection("BD-3.2.1", "Reflection to add extra arguments to the Node/Cluster constructors")
case class BDGraphReflectionAPI() extends DefaultGraphReflectionAPI {
  private val nodeStub = BDGraphNodeStub(mirror)
  private val clusterStub = BDGraphClusterStub(mirror)

  override def constructNode(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    nodeStub.createInstance(id, name, desc, metrics)
  }

  override def constructCluster(id: Long,
      name: String,
      desc: String,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphCluster = {
    clusterStub.createInstance(id, name, desc, nodes, metrics)
  }
}
