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
import scala.reflect.runtime.universe.Mirror

import org.apache.spark.sql.execution.ui.{SparkPlanGraphCluster, SparkPlanGraphNode, SQLPlanMetric}
import org.apache.spark.sql.rapids.tool.util.stubs.GraphReflectionEntry

case class DBGraphClusterStub(m: Mirror)
  extends GraphReflectionEntry[org.apache.spark.sql.execution.ui.SparkPlanGraphCluster](
    m, "org.apache.spark.sql.execution.ui.SparkPlanGraphCluster") {
  // DataBricks has different constructor of the sparkPlanGraphNode
  // (final long id, final java.lang.String name, final java.lang.String desc,
  // final ArrayBuffer<org.apache.spark.sql.execution.ui.SparkPlanGraphNode> nodes,
  // final scala.collection.Seq<org.apache.spark.sql.execution.ui.SQLPlanMetric> metrics,
  // final java.lang.String rddScopeId)

  // 10.4 is the same as other versions
  // (final long id, final java.lang.String name, final java.lang.String desc,
  // final ArrayBuffer<org.apache.spark.sql.execution.ui.SparkPlanGraphNode> nodes,
  // final Seq<org.apache.spark.sql.execution.ui.SQLPlanMetric> metrics,
  // final java.lang.String rddScopeId
  def createInstance(id: Long,
      name: String,
      desc: String,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphCluster = {
    val argValues = List(id, name, desc, nodes, metrics, "")
    createInstanceFromList(argValues)
  }
}
