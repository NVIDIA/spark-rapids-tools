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
  val nodeToStageCluster = mutable.HashMap[Long, Set[Int]]()
  val EMPTY_SET: Set[Int] = Set.empty

  def nodes: collection.Seq[SparkPlanGraphNode] = sparkGraph.nodes
  def edges: collection.Seq[SparkPlanGraphEdge] = sparkGraph.edges
  def allNodes: collection.Seq[SparkPlanGraphNode] = sparkGraph.allNodes

  /**
   * Get stages that are associated with the accumulators of the node.
   * Use this method if the purpose is to get raw information about the node-stage relationship
   * based on the AccumIds without applying any logic.
   * @param node the node to get the stages for
   * @return a set of stageIds or empty if None
   */
  def getNodeStagesByAccum(node: SparkPlanGraphNode): Set[Int] = {
    val nodeAccums = node.metrics.map(_.accumulatorId)
    nodeAccums.flatMap(app.accumManager.getAccStageIds).toSet
  }

  /**
   * Get the stage clusters that the node belongs to.
   * Use this method if the purpose is to a logical representation about the node-stage
   * relationship. For example, an "Exchange" node will return only a single stageID which is
   * responsible for writing the data.
   * @param node the node to get the stages for
   * @return a set of stageIds or empty if None
   */
  def getNodeStageClusters(node: SparkPlanGraphNode): Set[Int] = {
    nodeToStageCluster.getOrElse(node.id, EMPTY_SET)
  }

  /**
   * Get the stages that the node belongs to. This function is used to get all the stages that can
   * be assigned to a node. For exmple, if we want to get the "Exchange" node stages, then we call
   * that method.
   * @param node the node to get the stages for
   * @return a set of stageIds or empty if None
   */
  def getAllNodeStages(node: SparkPlanGraphNode): Set[Int] = {
    val stageIdsByAccum = getNodeStagesByAccum(node)
    nodeToStageCluster.get(node.id) match {
      case Some(stageId) => stageIdsByAccum ++ stageId
      case _ => stageIdsByAccum
    }
  }

  /**
   * Check if a node exec is an epilogue. A.k.a, the exec has to be the tail of a stage.
   * @param nodeName normalized node name (i.e., no GPU prefix)
   * @return true if the node is an epilogue exec
   */
  def isEpilogueExec(nodeName: String): Boolean = {
    nodeName match {
      case "Exchange" | "BroadcastQueryStage" | "ShuffleQueryStage" | "TableCacheQueryStage"
           | "ResultQueryStage" | "BroadcastExchange" =>
        true
      case _ => false
    }
  }

  /**
   * Check if a node exec is a prologue. A.k.a, the exec has to be the head of a stage.
   * @param nodeName normalized node name (i.e., no GPU prefix)
   * @return true if the node is a prologue exec
   */
  def isPrologueExec(nodeName: String): Boolean = {
    nodeName match {
      case nName if nName.contains("ShuffleRead") =>
        true
      case _ => false
    }
  }

  /**
   * Given a nodeName, this method returns a code that represents the node type.
   * For example, an exchange node has to be at the end of a stage. ShuffleRead has to be at the
   * beginning of a stage and so.
 *
   * @param nodeName the normalized name of the sparkNode (i.e., no GPU prefix).
   * @return a code representing the type of the node:
   *         (1) if the node can be assigned based on incoming edges. i.e., all nodes except the
   *             head of stage like shuffleRead.
   *         (2) if the node can be assigned based on outgoing edges. i.e.,
   *             all nodes except the tail of stage like shuffleWrite/exchange.
   *         (3) if the node can be assigned based on both incoming and outgoing edges.
   */
  def multiplexCases(nodeName: String): Int = {
    // nodes like shuffleRead should not be assigned to incoming edges
    var result = 0
    if (!isPrologueExec(nodeName)) {
      // Those are the nodes that can be assigned stages based on incoming edges.
      result |= 1
    }
    if (!isEpilogueExec(nodeName)) {
      // Those are the nodes that can be assigned stages based on outgoing edges.
      result |= 2
    }
    result
  }

  /**
   * This method is used to assign a node to clusterID during the first walk of the graph.
   * A cluster is used to wrap nodes together this could be a stageId.
   * @param node the sparkNode to assign
   * @return the clusterId that the node belongs to
   */
  def getStageClusterId(node: SparkPlanGraphNode): Set[Int] = {
    // Nodes like exchange, shuffleWrite, etc. should not be assigned to metrics that belongs to
    // ReadMetrics
    // First normalize the name.
    val normalizedName = ToolsPlanGraph.processPlanInfo(node.name)
    normalizedName match {
      case nName if isEpilogueExec(nName) =>
        // cases that are supposed to be tail of the stage cluster
        val stageIds = getNodeStagesByAccum(node)
        // only use the smalled StageId for now
        if (stageIds.size >= 1) {
          Set[Int](stageIds.min)
        } else {
          EMPTY_SET
        }
      case nName if isPrologueExec(nName) =>
        // cases that are supposed to be head of a new stage. We should pick the stages associated
        // with the reading metrics (pick the highest number for simplicity).
        val stageIds = getNodeStagesByAccum(node)
        if (stageIds.size >= 1) {
          Set[Int](stageIds.max)
        } else {
          EMPTY_SET
        }
      case _ =>
        // Everything else should be handled here. If a node a has more than one stage,
        // we should handle that here
        val stageIds = getNodeStagesByAccum(node)
        //TODO: assert(stageIds.size <= 1)
        //stageIds.headOption
        stageIds
    }
  }

  def removeNodeFromOrphans(node: SparkPlanGraphNode,
    orphanNodes: mutable.ArrayBuffer[SparkPlanGraphNode],
    clusters: Set[Int]): Unit = {
    nodeToStageCluster.put(node.id, clusters)
    orphanNodes -= node
  }

  /**
   * Assign a wholeStageNode to a clusterId. A WholeStageNode is visited after its children are.
   * If any of the children is not assigned to a cluster, the wNode will transfer its assignment to
   * the child.
   * @param wNode the wholeStageCodeGen node to be visited
   * @param orphanNodes the list of nodes that are not assigned to any cluster
   * @param clusterId the clusterId to assign the node to
   * @return true if a change was made.
   */
  def commitNodeToStageCluster(
    wNode: SparkPlanGraphCluster,
    orphanNodes: mutable.ArrayBuffer[SparkPlanGraphNode],
    clusters: Set[Int]): Boolean = {
    // node assigned cluster
    if (nodeToStageCluster.contains(wNode.id) && clusters.subsetOf(nodeToStageCluster(wNode.id))) {
      // Node is assigned to the same cluster before. Nothing to be done.
      false
    } else {
      val newClusterIds = clusters ++ nodeToStageCluster.getOrElse(wNode.id, EMPTY_SET)
      // remove the wNode from orphanNodes if it exists
      removeNodeFromOrphans(wNode, orphanNodes, newClusterIds)
      // assign the children to the same clusters if any of them is not assigned already
      wNode.nodes.foreach { childNode =>
        // TODO assert that the child node is not assigned something different than the wStage
        if (!nodeToStageCluster.contains(childNode.id)) {
          // assign the node to the same stage of wNode and remove it from orphans
          removeNodeFromOrphans(childNode, orphanNodes, newClusterIds)
        }
      }
      true
    }
  }

  /**
   * Assign a node to a clusterId. This method is used to assign a node to a clusterId during the
   * first visit.
   * @param node sparkNode to be assigned
   * @param orphanNodes the list of nodes that are not assigned to any cluster
   * @param clusters the clusterIds to assign the node to
   * @return true if a change was made.
   */
  def commitNodeToStageCluster(
    node: SparkPlanGraphNode,
    orphanNodes: mutable.ArrayBuffer[SparkPlanGraphNode],
    clusters: Set[Int]): Boolean = {
    node match {
      // while we are iterating capture which ones are wholeStageCodeGen
      case cluster: SparkPlanGraphCluster =>
        commitNodeToStageCluster(cluster, orphanNodes, clusters)
      case _ =>
        removeNodeFromOrphans(node, orphanNodes, clusters)
        true
    }
  }

  def getAllWStageNodes(): Seq[SparkPlanGraphCluster] = {
    allNodes.collect {
      case cluster: SparkPlanGraphCluster if cluster.isInstanceOf[SparkPlanGraphCluster] =>
        cluster
    }
  }

  /**
   * Walk through the graph nodes and assign them to the correct stage cluster.
   */
  def assignNodesToStageClusters(): Unit = {
    // nodes that are not assigned to any cluster
    val orphanNodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
    // 1st- visit all the nodes and assign them to the correct cluster based on their metrics.
    //      In the process, we can assign the children of cWholeStageCodeGen to the same clusters.
    allNodes.foreach { node =>
      if (!nodeToStageCluster.contains(node.id)) {
        // maybe it was added when we visited the wholeStageCodeGen
        val clusterIds = getStageClusterId(node)
        if (clusterIds.nonEmpty) {
          // found assignment
          commitNodeToStageCluster(node, orphanNodes, clusterIds)
        } else {
          // This node has no assignment. Add it to the orphanNodes
          orphanNodes += node
        }
      }
    }
    // At this point, we made a quick visit handling all the straightforward cases
    // 2nd- we iterate on the orphanNodes and try to assign them to the correct cluster.
    var changeFlag = orphanNodes.nonEmpty
    while (changeFlag) {
      // We keep iterating assigning nodes until there is no change in a single iteration.
      changeFlag = false
      // Pick the orphans and try to check their neighbors
      // P.S: Copy the orphanNodes because we cannot remove objects inside the loop.
      val orphanNodesCopy = orphanNodes.clone()
      orphanNodesCopy.foreach { orphanNode =>
        // if this this a wholeStageNode, then it won't have any edges and the only way to connect
        // it is through the child nodes
        val normalOrphName = ToolsPlanGraph.processPlanInfo(orphanNode.name)
        val updatedFlag = orphanNode match {
          case wNode: SparkPlanGraphCluster =>
            // WholeStageCodeGen represents a corner case because it is not connected by edges.
            // The only way to set the clusterID is to get it from the children if any.
            wNode.nodes.find { childNode => nodeToStageCluster.contains(childNode.id) } match {
              case Some(childNode) =>
                val clusterIDs = nodeToStageCluster(childNode.id)
                commitNodeToStageCluster(wNode, orphanNodes, clusterIDs)
              case _ => // do nothing
                false
            }
          case _ =>
            // Handle all non wholeStageCodeGen nodes
            // check what type of node to deal with, to determine the restrictions on assigning
            // nodes to stages
            val nodeCase = multiplexCases(normalOrphName)
            var clusterIDs = EMPTY_SET
            if ((nodeCase & 1) > 0) {
              // get all the incoming edges that can propagate a cluster assignment
              val inEdgesWithIds =
                edges.filter(e => e.toId == orphanNode.id && nodeToStageCluster.contains(e.fromId))
              if (inEdgesWithIds.nonEmpty) {
                // TODO: Assert that all the source nodes have the same clusterId
                // get the clusterId of the first non orphan node
                clusterIDs = nodeToStageCluster(inEdgesWithIds.head.fromId)
              }
            }
            if (clusterIDs.isEmpty && (nodeCase & 2) > 0) {
              // Try to match based on the sinkNodes
              val outEdgesWithIds =
                edges.filter(e => e.fromId == orphanNode.id && nodeToStageCluster.contains(e.toId))
              if (outEdgesWithIds.isEmpty && isPrologueExec(normalOrphName)) {
                // corner case that a shuffle reader is reading on driver so, it is not feasible to
                // match it to stage without looking to the exchange that passed the data to it.
                // in that case, we need to grab if there is an exchange pointing to this node
              }
              if (outEdgesWithIds.nonEmpty) {
                // TODO: Assert that all the source nodes have the same clusterId
                // get the clusterId of the first non orphan node
                clusterIDs = nodeToStageCluster(outEdgesWithIds.head.toId)
              }
            }
            if (clusterIDs.nonEmpty) {
              commitNodeToStageCluster(orphanNode, orphanNodes, clusterIDs)
            } else {
              if (!normalOrphName.contains("CreateViewCommand")) {
                println("could not belong anywhere")
              }
              false
            }
        }
        changeFlag |= updatedFlag
      }
      // corner case for shuffleRead when it is reading from teh driver followed by an exchange that
      // has no metric
      if (!changeFlag && orphanNodes.nonEmpty) {
        // we could not assign any node to a cluster. This means that we have a cycle in the graph
        // and we need to break it.
        // We need to assign the nodes to a cluster based on the order of the nodes in the graph.
        // This is a corner case that we need to handle.
        // Pick customShuffleRead if any
        changeFlag |= orphanNodes.filter(
          n => isPrologueExec(ToolsPlanGraph.processPlanInfo(n.name))).exists {
          orphanNode =>
            // try to get if there is a node pointing to it
            val inEdgesWithIds =
              edges.filter(e => e.toId == orphanNode.id && nodeToStageCluster.contains(e.fromId))
            if (inEdgesWithIds.nonEmpty) {
              // assign the maximum clusterId to the node
              val possibleIds = inEdgesWithIds.map { e =>
                val nodeObj = allNodes.find(no => no.id == e.fromId).get
                // we need to get all the Ids and not only the logical ones.
                getAllNodeStages(nodeObj)
              }.reduce(_ ++ _)
              val newIDs = Set[Int](possibleIds.max)
              commitNodeToStageCluster(orphanNode, orphanNodes, newIDs)
            } else {
              false
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
