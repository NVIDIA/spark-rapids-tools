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

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphEdge, SparkPlanGraphNode, SQLPlanMetric}
import org.apache.spark.sql.rapids.tool.store.AccumNameRef
import org.apache.spark.sql.rapids.tool.util.stubs.GraphReflectionAPIHelper

/**
 * This code is mostly copied from org.apache.spark.sql.execution.ui.SparkPlanGraph
 * with changes to handle GPU nodes. Without this special handle, the default SparkPlanGraph
 * would not be able to recognize reused/exchange nodes leading to duplicating nodes.
 *
 * Build a SparkPlanGraph from the root of a SparkPlan tree.
 */
object ToolsPlanGraph {
  // By default call the Spark constructor. If this fails, we fall back to the DB constructor
  def constructGraphNode(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    GraphReflectionAPIHelper.api.get.constructNode(id, name, desc, metrics)
  }

  private def constructSQLPlanMetric(name: String,
      accumulatorId: Long,
      metricType: String): SQLPlanMetric = {
    val accNameRef = AccumNameRef.getOrCreateAccumNameRef(name)
    GraphReflectionAPIHelper.api.get.constructSQLPlanMetric(accNameRef.value, accumulatorId,
      metricType)
  }

  private def constructCluster(id: Long,
      name: String,
      desc: String,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphCluster = {
    GraphReflectionAPIHelper.api.get.constructCluster(id, name, desc, nodes, metrics)
  }

  private def constructEdge(fromId: Long, toId: Long): SparkPlanGraphEdge = {
    GraphReflectionAPIHelper.api.get.constructEdge(fromId, toId)
  }

  /**
   * Build a SparkPlanGraph from the root of a SparkPlan tree.
   */
  def apply(planInfo: SparkPlanInfo): SparkPlanGraph = {
    try {
      val nodeIdGenerator = new AtomicLong(0)
      val nodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
      val edges = mutable.ArrayBuffer[SparkPlanGraphEdge]()
      val exchanges = mutable.HashMap[SparkPlanInfo, SparkPlanGraphNode]()
      GraphReflectionAPIHelper.api match {
        case Some(_) =>
          // If we have a runtime API, use it to build a graph.
          buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, null, null, exchanges)
          new SparkPlanGraph(nodes, edges)
        case None =>
          // If the runtime API is not available, fall back to the default constructor defined in
          // Spark Runtime.
          SparkPlanGraph(planInfo)
      }
    } catch {
      // If the construction of the graph fails due to NoSuchMethod, then it is possible the
      // runtime is not compatible with the constructors and we fallback to the loaded runtime jars
      case _ : java.lang.NoSuchMethodError | _ : java.lang.IllegalArgumentException =>
        SparkPlanGraph(planInfo)
    }
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
          constructSQLPlanMetric(metric.name, metric.accumulatorId, metric.metricType)
        }

        val cluster = constructCluster(
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
          edges += constructEdge(node.id, parent.id)
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
        edges += constructEdge(node.id, parent.id)
      case "ReusedSubquery" =>
        // Re-used subquery might appear before the original subquery, so skip this node and let
        // the previous `case` make sure the re-used and the original point to the same node.
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, subgraph, exchanges)
      case "ReusedExchange" if exchanges.contains(planInfo.children.head) =>
        // Point to the re-used exchange
        val node = exchanges(planInfo.children.head)
        edges += constructEdge(node.id, parent.id)
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
          edges += constructEdge(node.id, parent.id)
        }
        planInfo.children.foreach(
          buildSparkPlanGraphNode(_, nodeIdGenerator, nodes, edges, node, subgraph, exchanges))
    }
  }
}
