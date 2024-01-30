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

package org.apache.spark.sql.rapids.tool.util

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.reflect.runtime.universe._

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphEdge, SparkPlanGraphNode, SQLPlanMetric}


// Container class to hold snapshot of the reflection fields instead of recalculating them every
// time we call the constructor
case class DBReflectionContainer() {
  private val mirror = runtimeMirror(getClass.getClassLoader)
  // Get the node class symbol
  private val nodeClassSymbol =
    mirror.staticClass("org.apache.spark.sql.execution.ui.SparkPlanGraphNode")
  // Get the node constructor method symbol
  private val nodeConstr = nodeClassSymbol.primaryConstructor.asMethod
  // Get the SQL class symbol
  private val metricClassSymbol =
    mirror.staticClass("org.apache.spark.sql.execution.ui.SQLPlanMetric")
  // Get the metric constructor method symbol
  private val metricConstr = metricClassSymbol.primaryConstructor.asMethod

  def constructNode(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    // Define argument values
    val argValues = List(id, name, desc, metrics, "", false, None, None)
    mirror.reflectClass(nodeClassSymbol)
      .reflectConstructor(nodeConstr)(argValues: _*)
      .asInstanceOf[org.apache.spark.sql.execution.ui.SparkPlanGraphNode]
  }

  def constructSQLPlanMetric(name: String,
      accumulatorId: Long,
      metricType: String): SQLPlanMetric = {
    // Define argument values
    val argValues = List(name, accumulatorId, metricType, false)
    mirror.reflectClass(metricClassSymbol)
      .reflectConstructor(metricConstr)(argValues: _*)
      .asInstanceOf[org.apache.spark.sql.execution.ui.SQLPlanMetric]
  }
}

/**
 * This code is mostly copied from org.apache.spark.sql.execution.ui.SparkPlanGraph
 * with changes to handle GPU nodes. Without this special handle, the default SparkPlanGraph
 * would not be able to recognize reused/exchange nodes leading to duplicating nodes.
 *
 * Build a SparkPlanGraph from the root of a SparkPlan tree.
 */
object ToolsPlanGraph {
  // TODO: We should have a util to detect if the runtime is Databricks.
  //      This can be achieved by checking for spark properties
  //      spark.databricks.clusterUsageTags.clusterAllTags
  private lazy val dbRuntimeReflection = DBReflectionContainer()
  // By default call the Spark constructor. If this fails, we fall back to the DB constructor
  private def constructGraphNode(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    try {
      new SparkPlanGraphNode(id, name, desc, metrics)
    } catch {
      case _: java.lang.NoSuchMethodError =>
        // DataBricks has different constructor of the sparkPlanGraphNode
        // [(long,java.lang.String,java.lang.String,scala.collection.Seq,java.lang.String,
        // boolean,scala.Option,scala.Option)] and
        // [final long id, final java.lang.String name, final java.lang.String desc,
        // final scala.collection.Seq<org.apache.spark.sql.execution.ui.SQLPlanMetric> metrics,
        // final java.lang.String rddScopeId, final boolean started,
        // final scala.Option<scala.math.BigInt> estRowCount)
        dbRuntimeReflection.constructNode(id, name, desc, metrics)
    }

  }

  private def constructSQLPlanMetric(name: String,
      accumulatorId: Long,
      metricType: String): SQLPlanMetric = {
    try {
      SQLPlanMetric(name, accumulatorId, metricType)
    } catch {
      case _: java.lang.NoSuchMethodError =>
        // DataBricks has different constructor of the sparkPlanGraphNode
        //Array(final java.lang.String name, final long accumulatorId,
        // final java.lang.String metricType, final boolean experimental)
        dbRuntimeReflection.constructSQLPlanMetric(name, accumulatorId, metricType)
    }
  }

  /**
   * Build a SparkPlanGraph from the root of a SparkPlan tree.
   */
  def apply(planInfo: SparkPlanInfo): SparkPlanGraph = {
    val nodeIdGenerator = new AtomicLong(0)
    val nodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
    val edges = mutable.ArrayBuffer[SparkPlanGraphEdge]()
    val exchanges = mutable.HashMap[SparkPlanInfo, SparkPlanGraphNode]()
    buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, null, null, exchanges)
    new SparkPlanGraph(nodes, edges)
  }

  private def processPlanInfo(nodeName: String): String = {
    if (nodeName.startsWith("Gpu")) {
      nodeName.replaceFirst("Gpu", "")
    } else {
      nodeName
    }
  }

  private def buildSparkPlanGraphNode(
      planInfo: SparkPlanInfo,
      nodeIdGenerator: AtomicLong,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      edges: mutable.ArrayBuffer[SparkPlanGraphEdge],
      parent: SparkPlanGraphNode,
      subgraph: SparkPlanGraphCluster,
      exchanges: mutable.HashMap[SparkPlanInfo, SparkPlanGraphNode]): Unit = {
    processPlanInfo(planInfo.nodeName) match {
      case name if name.startsWith("WholeStageCodegen") =>
        val metrics = planInfo.metrics.map { metric =>
          SQLPlanMetric(metric.name, metric.accumulatorId, metric.metricType)
        }

        val cluster = new SparkPlanGraphCluster(
          nodeIdGenerator.getAndIncrement(),
          planInfo.nodeName,
          planInfo.simpleString,
          mutable.ArrayBuffer[SparkPlanGraphNode](),
          metrics)
        nodes += cluster

        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, cluster, exchanges)
      case "InputAdapter" =>
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, exchanges)
      case "BroadcastQueryStage" | "ShuffleQueryStage" =>
        if (exchanges.contains(planInfo.children.head)) {
          // Point to the re-used exchange
          val node = exchanges(planInfo.children.head)
          edges += SparkPlanGraphEdge(node.id, parent.id)
        } else {
          buildSparkPlanGraphNode(
            planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, exchanges)
        }
      case "TableCacheQueryStage" =>
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, exchanges)
      case "Subquery" | "SubqueryBroadcast" if subgraph != null =>
        // Subquery should not be included in WholeStageCodegen
        buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, parent, null, exchanges)
      case "Subquery" | "SubqueryBroadcast" if exchanges.contains(planInfo) =>
        // Point to the re-used subquery
        val node = exchanges(planInfo)
        edges += SparkPlanGraphEdge(node.id, parent.id)
      case "ReusedSubquery" =>
        // Re-used subquery might appear before the original subquery, so skip this node and let
        // the previous `case` make sure the re-used and the original point to the same node.
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, subgraph, exchanges)
      case "ReusedExchange" if exchanges.contains(planInfo.children.head) =>
        // Point to the re-used exchange
        val node = exchanges(planInfo.children.head)
        edges += SparkPlanGraphEdge(node.id, parent.id)
      case name =>
        val metrics = planInfo.metrics.map { metric =>
          constructSQLPlanMetric(metric.name, metric.accumulatorId, metric.metricType)
        }
        val node = constructGraphNode(nodeIdGenerator.getAndIncrement(),
          planInfo.nodeName, planInfo.simpleString, metrics)
        if (subgraph == null) {
          nodes += node
        } else {
          subgraph.nodes += node
        }
        if (name.contains("Exchange") || name.contains("Subquery")) {
          exchanges += planInfo -> node
        }

        if (parent != null) {
          edges += SparkPlanGraphEdge(node.id, parent.id)
        }
        planInfo.children.foreach(
          buildSparkPlanGraphNode(_, nodeIdGenerator, nodes, edges, node, subgraph, exchanges))
    }
  }
}
