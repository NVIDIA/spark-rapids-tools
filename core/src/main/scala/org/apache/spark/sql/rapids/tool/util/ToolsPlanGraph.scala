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

import com.nvidia.spark.rapids.tool.planparser.DatabricksParseHelper

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui._
import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.store.AccumNameRef
import org.apache.spark.sql.rapids.tool.util.plangraph.PlanGraphTransformer
import org.apache.spark.sql.rapids.tool.util.stubs.{GraphReflectionAPI, GraphReflectionAPIHelper}


class ToolsPlanGraph(app: AppBase, val sparkGraph: SparkPlanGraph) {
  val nodeToStageCluster = mutable.HashMap[Long, Int]()
  def nodes: collection.Seq[SparkPlanGraphNode] = sparkGraph.nodes
  def edges: collection.Seq[SparkPlanGraphEdge] = sparkGraph.edges
  def allNodes: collection.Seq[SparkPlanGraphNode] = sparkGraph.allNodes

  def getNodeStagesByAccum(node: SparkPlanGraphNode): Set[Int] = {
    val nodeAccums = node.metrics.map(_.accumulatorId)
    nodeAccums.flatMap(app.accumManager.getAccStageIds).toSet
  }

  def getNodeStageClusters(node: SparkPlanGraphNode): Set[Int] = {
    nodeToStageCluster.get(node.id) match {
      case Some(stageId) => Set(stageId)
      case _ => Set.empty
    }
  }

  def getNodeStages(node: SparkPlanGraphNode): Set[Int] = {
    val stageIdsByAccum = getNodeStagesByAccum(node)
    nodeToStageCluster.get(node.id) match {
      case Some(stageId) => stageIdsByAccum + stageId
      case _ => stageIdsByAccum
    }
  }

  def multiplexCases(nodeName: String): Int = {
    // nodes like shuffleRead should not be assigned to incoming edges
    var result = 0
    if (!isEpilogueNode(nodeName)) {
      // only incoming edges
      result |= 1
    }
    if (!isPrologueNode(nodeName)) {
      // only outgoing edges
      result |= 2
    }
    result
  }

  def isPrologueNode(nodeName: String): Boolean = {
    nodeName match {
      case "Exchange" | "BroadcastQueryStage" | "ShuffleQueryStage" | "TableCacheQueryStage"
           | "ResultQueryStage" =>
        true
      case _ => false
    }
  }

  def isEpilogueNode(nodeName: String): Boolean = {
    nodeName match {
      case nName if nName.contains("ShuffleRead") =>
        true
      case _ => false
    }
  }

  /**
   * This method is used to assign a node to clusterID. A cluster is used to wrap nodes together
   * this could be a stageId.
   * @param node
   * @return
   */
  def getStageClusterId(node: SparkPlanGraphNode): Option[Int] = {
    // Nodes like exchange, shuffleWrite, etc. should not be assigned to metrics that belongs to
    // ReadMetrics
    val normalizedName = ToolsPlanGraph.processPlanInfo(node.name)
    normalizedName match {
      case nName if isPrologueNode(nName) =>
        // cases that are supposed to be end of the stage cluster
        val stageIds = getNodeStagesByAccum(node)
        // only use the smalled StageId for now
        if (stageIds.size >= 1) {
          Some(stageIds.min)
        } else {
          None
        }
      case nName if isEpilogueNode(nName) =>
        // cases that are supposed to be beginning of a new stage
        val stageIds = getNodeStagesByAccum(node)
        if (stageIds.size >= 1) {
          Some(stageIds.max)
        } else {
          None
        }
      case _ =>
        // cases that are supposed to be beginning of a new stage
        val stageIds = getNodeStagesByAccum(node)
        //TODO: assert(stageIds.size <= 1)
        stageIds.headOption
    }
  }

  def assignWholeStageNode(
    wNode: SparkPlanGraphCluster,
    orphanNodes: mutable.ArrayBuffer[SparkPlanGraphNode],
    clusterId: Int): Boolean = {
    // node has an assigned cluster
    if (clusterId == wNode.id && nodeToStageCluster.contains(wNode.id)) {
      false
    } else {
      nodeToStageCluster.put(wNode.id, clusterId)
      orphanNodes -= wNode
      wNode.nodes.foreach { childNode =>
        if (!nodeToStageCluster.contains(childNode.id)) {
          // assign the node to the same stage of wNode
          nodeToStageCluster.put(childNode.id, clusterId)
          // remove the orphanNode from the list
          orphanNodes -= childNode
        }
      }
      true
    }
  }

  def assignGenericNodeToStageCluster(
    node: SparkPlanGraphNode,
    orphanNodes: mutable.ArrayBuffer[SparkPlanGraphNode],
    clusterId: Int): Boolean = {
    node match {
      // while we are iterating capture which ones are wholeStageCodeGen
      case cluster: SparkPlanGraphCluster =>
        assignWholeStageNode(cluster, orphanNodes, clusterId)
      case _ =>
        nodeToStageCluster.put(node.id, clusterId)
        orphanNodes -= node
        true
    }
  }

  def getAllWStageNodes(): Seq[SparkPlanGraphCluster] = {
    allNodes.collect {
      case cluster: SparkPlanGraphCluster if cluster.isInstanceOf[SparkPlanGraphCluster] =>
        cluster
    }
  }


  def assignNodesToStageClusters(): Unit = {
    // nodes that are not assigned to any cluster
    val orphanNodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
    //val wholeStageNodes = getAllWStageNodes()

    allNodes.foreach { node =>
      if (!nodeToStageCluster.contains(node.id)) {
        // maybe it was added when we visited the wholeStageCodeGen
        val clusterId = getStageClusterId(node)
        if (clusterId.isDefined) {
          node match {
            // while we are iterating capture which ones are wholeStageCodeGen
            case cluster: SparkPlanGraphCluster =>
              assignWholeStageNode(cluster, orphanNodes, clusterId.get)
            case _ =>
              nodeToStageCluster.put(node.id, clusterId.get)
          }
        } else {
          orphanNodes += node
        }
      }
    }
    // at this point, we made a quick visit handling all the straightforward cases
    // TODO: there is a problem if wholeStageCodeGen has no metrics
    // at this point, we have handled all straightforward cases.
    // now, we iterate on the orphanNodes and try to assign them to the correct cluster.
    // Easy case: assign wholeStageCodeGen cluster to its children if possible
    var changeFlag = orphanNodes.nonEmpty
    while (changeFlag) {
      changeFlag = false
      // now pick the orphans and try to check the neighbors
      val orphanNodesCopy = orphanNodes.clone()
      orphanNodesCopy.foreach { orphanNode =>
        // if this this a wholeStageNode, then it won't have any edges and the only way to connect
        // it is through the child nodes
        val normalOrphName = ToolsPlanGraph.processPlanInfo(orphanNode.name)
        orphanNode match {
          case wNode: SparkPlanGraphCluster =>
            // corner case the wholestageCodeGen is not connected by edges to other nodes
            wNode.nodes.find { childNode => nodeToStageCluster.contains(childNode.id) } match {
              case Some(childNode) =>
                val clusterID = nodeToStageCluster(childNode.id)
                changeFlag ||= assignWholeStageNode(wNode, orphanNodes, clusterID)
              case _ => // do nothing
            }
          case _ =>
            // get the sink nodes that are connected to non orphaneNodes
            val nodeCase = multiplexCases(normalOrphName)
            var clusterID = -1
            if ((nodeCase & 1) > 0) {
              val allIncomingEdges =
                edges.filter(e => e.toId == orphanNode.id && nodeToStageCluster.contains(e.fromId))
              if (allIncomingEdges.nonEmpty) {
                // get the clusterId of the first non orphan node
                clusterID = nodeToStageCluster(allIncomingEdges.head.fromId)
                // TODO: Assert that all the source nodes have the same clusterId
              }
            }
            if (clusterID == -1 && (nodeCase & 2) > 0) {
              // try to match based on the sinkNodes
              val allOutgoingEdges =
                edges.filter(e => e.fromId == orphanNode.id && nodeToStageCluster.contains(e.toId))
              if (allOutgoingEdges.nonEmpty) {
                clusterID = nodeToStageCluster(allOutgoingEdges.head.toId)
              }
            }
            if (clusterID != -1) {
              changeFlag ||= assignGenericNodeToStageCluster(orphanNode, orphanNodes, clusterID)
            }
        }
      }
    }
  }
}

/**
 * This code is mostly copied from org.apache.spark.sql.execution.ui.SparkPlanGraph
 * with changes to handle GPU nodes. Without this special handle, the default SparkPlanGraph
 * would not recognize reused/exchange nodes leading to duplicating nodes.
 *
 * Build a SparkPlanGraph from the root of a SparkPlan tree.
 */
object ToolsPlanGraph {
  // Captures the API loaded at runtime if any.
  var api: GraphReflectionAPI = _

  // The actual code used to build the graph. If the API is not available, then fallback to the
  // Spark default API.
  private lazy val graphBuilder: SparkPlanInfo => SparkPlanGraph = {
    GraphReflectionAPIHelper.api match {
      case Some(_) =>
        // set the api to the available one
        api = GraphReflectionAPIHelper.api.get
        (planInfo: SparkPlanInfo) => {
          val nodeIdGenerator = new AtomicLong(0)
          val nodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
          val edges = mutable.ArrayBuffer[SparkPlanGraphEdge]()
          val exchanges = mutable.HashMap[SparkPlanInfo, SparkPlanGraphNode]()
          buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, null, null, exchanges)
          new SparkPlanGraph(nodes, edges)
        }
      case _ =>
        // Fallback to the default SparkPlanGraph
        (planInfo: SparkPlanInfo) => {
          SparkPlanGraph(planInfo)
        }
    }
  }

  // used for testing purpose
  def constructGraphNode(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    GraphReflectionAPIHelper.api.get.constructNode(id, name, desc, metrics)
  }

  // Normalize the accumName before creating it.
  private def constructSQLPlanMetric(name: String,
      accumulatorId: Long,
      metricType: String): SQLPlanMetric = {
    val accNameRef = AccumNameRef.getOrCreateAccumNameRef(name)
    GraphReflectionAPIHelper.api.get.constructSQLPlanMetric(accNameRef.value, accumulatorId,
      metricType)
  }

  /**
   * Build a SparkPlanGraph from the root of a SparkPlan tree.
   */
  def apply(planInfo: SparkPlanInfo): SparkPlanGraph = {
    try {
      graphBuilder(planInfo)
    } catch {
      // If the construction of the graph fails due to NoSuchMethod, then it is possible the
      // runtime is not compatible with the constructors and we fallback to the loaded runtime jars
      case _ : java.lang.NoSuchMethodError | _ : java.lang.IllegalArgumentException =>
        SparkPlanGraph(planInfo)
    }
  }

  def createGraphWithStageClusters(app: AppBase, planInfo: SparkPlanInfo): ToolsPlanGraph = {
    val sGraph = ToolsPlanGraph(planInfo)
    val toolsGraph = new ToolsPlanGraph(app, sGraph)
    toolsGraph.assignNodesToStageClusters()
    toolsGraph
  }

  private def processPlanInfo(nodeName: String): String = {
    if (nodeName.startsWith("Gpu")) {
      nodeName.replaceFirst("Gpu", "")
    } else if (DatabricksParseHelper.isPhotonNode(nodeName)) {
      DatabricksParseHelper.mapPhotonToSpark(nodeName)
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

        val baseCluster = api.constructCluster(
          nodeIdGenerator.getAndIncrement(),
          planInfo.nodeName,
          planInfo.simpleString,
          mutable.ArrayBuffer[SparkPlanGraphNode](),
          metrics)
        // Transform if it is a specialized type(e.g. Photon)
        val cluster = PlanGraphTransformer.transformPlanCluster(baseCluster)
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
          edges += api.constructEdge(node.id, parent.id)
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
        edges += api.constructEdge(node.id, parent.id)
      case "ReusedSubquery" =>
        // Re-used subquery might appear before the original subquery, so skip this node and let
        // the previous `case` make sure the re-used and the original point to the same node.
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, subgraph, exchanges)
      case "ReusedExchange" if exchanges.contains(planInfo.children.head) =>
        // Point to the re-used exchange
        val node = exchanges(planInfo.children.head)
        edges += api.constructEdge(node.id, parent.id)
      case name =>
        val metrics = planInfo.metrics.map { metric =>
          constructSQLPlanMetric(metric.name, metric.accumulatorId, metric.metricType)
        }
        val baseNode = api.constructNode(nodeIdGenerator.getAndIncrement(),
          planInfo.nodeName, planInfo.simpleString, metrics)
        // Transform if it is a specialized type(e.g. Photon)
        val node = PlanGraphTransformer.transformPlanNode(baseNode)
        if (subgraph == null) {
          nodes += node
        } else {
          subgraph.nodes += node
        }
        if (name.contains("Exchange") || name.contains("Subquery")) {
          exchanges += planInfo -> node
        }

        if (parent != null) {
          edges += api.constructEdge(node.id, parent.id)
        }
        planInfo.children.foreach(
          buildSparkPlanGraphNode(_, nodeIdGenerator, nodes, edges, node, subgraph, exchanges))
    }
  }
}
