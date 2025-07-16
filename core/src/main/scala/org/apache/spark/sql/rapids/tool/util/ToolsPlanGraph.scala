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

package org.apache.spark.sql.rapids.tool.util

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.planparser.DatabricksParseHelper

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui._
import org.apache.spark.sql.rapids.tool.AccumToStageRetriever
import org.apache.spark.sql.rapids.tool.store.AccumNameRef
import org.apache.spark.sql.rapids.tool.util.plangraph.PlanGraphTransformer
import org.apache.spark.sql.rapids.tool.util.stubs.{GraphReflectionAPI, GraphReflectionAPIHelper}

class NodeStageMapper {
  // A map between SQLNode Id and the clusterIds that the node belongs to.
  // This map keeps track of the logical binding between nodes and stages.
  // For example, for qualification this map will be the source of exec-to-stage assignment.
  private val logicalNTS: mutable.LongMap[Set[Int]] = mutable.LongMap[Set[Int]]()
  // A map between SQLNode Id and the stages. This map keeps track of node-to-stage assignment based
  // on accumulable IDs without applying any heuristics or logic.
  private val rawNTS: mutable.LongMap[Set[Int]] = mutable.LongMap[Set[Int]]()
  // It is possible to represent the clusters as map [clusterId, Set[SQLNodeIds]].
  // While this is more memory efficient, it is more time-consuming to find the clusters a
  // node belongs to since we have to iterate through all the keys.
  private val rawSTN: mutable.Map[Int, mutable.Set[Long]] =
    mutable.HashMap[Int, mutable.Set[Long]]()

  // used to insert the map between stage-to-node
  private def updateSTNMap(m: mutable.Map[Int, mutable.Set[Long]],
      allKeys: collection.Set[Int], nId: Long): Unit = {
    allKeys.foreach { stageId =>
      val existingNodes = m.getOrElseUpdate(stageId, mutable.Set())
      existingNodes.add(nId)
    }
  }

  // Used to add a map node-to-stage based on the accumulableIds
  def addRawNTS(nId: Long, stageIDs: Set[Int]): Boolean = {
    if (stageIDs.isEmpty || rawNTS.contains(nId)) {
      // Do not add node twice because the raw mapping between node and stages should be constant.
      // Do not add empty stageIds.
      return false
    }
    rawNTS.update(nId, stageIDs)
    updateSTNMap(rawSTN, stageIDs, nId)
    true
  }

  // Used to add a map node-to-stage based on the heuristics and logic and it might not necessarily
  // match what the entries in the raw map.
  def addLogicalNTS(nId: Long, stageIds: Set[Int]): Boolean = {
    if (stageIds.isEmpty) {
      return false
    }
    val existingStages = logicalNTS.getOrElse(nId, Set())
    logicalNTS.put(nId, existingStages ++ stageIds)
    true
  }

  // Get the stage-to-node based on accumulableIds.
  def getRawSTN(stageId: Int): collection.Set[Long] = {
    rawSTN.getOrElse(stageId, ToolsPlanGraph.EMPTY_NODES)
  }

  // Get the node-to-stage based on accumulableIds.
  def getRawNTS(nId: Long): Set[Int] = {
    rawNTS.getOrElse(nId, ToolsPlanGraph.EMPTY_CLUSTERS)
  }

  // Get the node-to-stage based on the heuristic and logical assignments.
  def getLogicalNTS(nId: Long): Set[Int] = {
    logicalNTS.getOrElse(nId, ToolsPlanGraph.EMPTY_CLUSTERS)
  }

  // Check if the node has been already added to the logical map.
  def hasLogicKey(nId: Long): Boolean = {
    logicalNTS.contains(nId)
  }

  // Return both raw and logical mapping for a given node.. It combined both entries.
  def getAllNTSAssignment(nId: Long): Set[Int] = {
    getRawNTS(nId) ++ getLogicalNTS(nId)
  }
}

/**
 * A wrapper of the original SparkPlanGraph with additional information about the
 * node-to-stage mapping.
 * 1- The graph is constructed by visiting PlanInfos and creating GraphNodes and Edges.
 *    Although it is more efficient to assign stages during the construction of the nodes,
 *    the design is intentionally keeping those two phases separate to make the code more modular
 *    and easier to maintain.
 * 2- Traverse the nodes and assign them to stages based on the metrics.
 * 3- Nodes that belong to a graph cluster (childs of WholeStageCodeGen) while missing
 *    metrics, are assigned same as their WholeStageCodeGen node.
 * 4- Iterate on all the orphanNodes and assign them to stages based on their adjacents nodes.
 * 5- The iterative process is repeated until no assignment can be made.
 *
 * @param sparkGraph the original SparkPlanGraph to wrap
 * @param accumToStageRetriever The object that eventually can retrieve StageIDs from AccumIds.
 */
class ToolsPlanGraph(val sparkGraph: SparkPlanGraph,
    accumToStageRetriever: AccumToStageRetriever) {
  // Container that manages the details of updating the node-to-stage mapping.
  private val nodeStageMapper = new NodeStageMapper()

  // shortcut to the nodes
  def nodes: collection.Seq[SparkPlanGraphNode] = sparkGraph.nodes
  // shortcut to the edges
  def edges: collection.Seq[SparkPlanGraphEdge] = sparkGraph.edges
  // delegate the call to the original graph
  def allNodes: collection.Seq[SparkPlanGraphNode] = sparkGraph.allNodes

  /**
   * Get stages that are associated with the accumulators of the node.
   * Use this method if the purpose is to get raw information about the node-stage relationship
   * based on the AccumIds without applying any logic.
   * @param node the node to get the stages for
   * @return a set of stageIds or empty if None
   */
   private def getNodeStagesByAccum(node: SparkPlanGraphNode): Set[Int] = {
    val nodeAccums = node.metrics.map(_.accumulatorId)
    val stageIds = accumToStageRetriever.getStageIDsFromAccumIds(nodeAccums)
     nodeStageMapper.addRawNTS(node.id, stageIds)
     stageIds
  }

  /**
   * Get the stages that the node belongs to. This function is used to get all the stages that can
   * be assigned to a node. For example, if we want to get the "Exchange" node stages, then we call
   * that method.
   * @param node the node to get the stages for
   * @return a set of stageIds or empty if None
   */
  private def getAllNodeStages(node: SparkPlanGraphNode): Set[Int] = {
    nodeStageMapper.getAllNTSAssignment(node.id)
  }

  /**
   * Check if a node exec is an epilogue. A.k.a, the exec has to be the tail of a stage.
   * @param nodeName normalized node name (i.e., no GPU prefix)
   * @return true if the node is an epilogue exec
   */
  private def isEpilogueExec(nodeName: String): Boolean = {
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
  private def isPrologueExec(nodeName: String): Boolean = {
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
   * @return a code representing thenode type:
   *         (1) if the node can be assigned based on incoming edges. i.e., all nodes except the
   *             head of stage like shuffleRead.
   *         (2) if the node can be assigned based on outgoing edges. i.e.,
   *             all nodes except the tail of stage like shuffleWrite/exchange.
   *         (3) if the node can be assigned based on both incoming and outgoing edges.
   */
  private def multiplexCases(nodeName: String): Int = {
    // nodes like shuffleRead should not be assigned to incoming edges
    var result = 0
    if (!isPrologueExec(nodeName)) {
      // Those are the nodes that can be assigned based on incoming edges.
      result |= 1
    }
    if (!isEpilogueExec(nodeName)) {
      // Those are the nodes that can be assigned based on outgoing edges.
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
  private def populateNodeClusters(node: SparkPlanGraphNode): Set[Int] = {
    // First normalize the name.
    val normalizedName = ToolsPlanGraph.processPlanInfo(node.name)
    val stageIds = getNodeStagesByAccum(node)
    normalizedName match {
      case nName if isEpilogueExec(nName) =>
        // Cases that are tail of the stage cluster.
        if (stageIds.size <= 1) {
          stageIds
        } else {
          // Only use the smallest StageId because this would represent the stage that writes
          // the data.
          Set[Int](stageIds.min)
        }
      case nName if isPrologueExec(nName) =>
        // Cases that are head of a new stage.
        if (stageIds.size <= 1) {
          ToolsPlanGraph.EMPTY_CLUSTERS
        } else {
          // We should pick the stages associated with the reading metrics. This is likely to be
          // the stage with the highest ID value.
          Set[Int](stageIds.max)
        }
      case _ =>
        // Everything else goes here.
        // It is possible to have multiple stages for a given node.
        stageIds
    }
  }

  /**
   * Updates the data structure that keeps track of the nodes cluster assignment.
   * It adds the node to the map and remove the node from the orphans list if it exists.
   * @param node the node to be assigned.
   * @param orphanNodes the list of nodes that are not assigned to any cluster.
   * @param clusters the clusterIds to assign the node to
   */
  private def removeNodeFromOrphans(node: SparkPlanGraphNode,
      orphanNodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      clusters: Set[Int]): Boolean = {
    if (nodeStageMapper.addLogicalNTS(node.id, clusters)) {
      orphanNodes -= node
      true
    } else {
      false
    }

  }

  /**
   * Commits a wholeStageNode to a cluster.
   * A WholeStageNode is visited after its children are. If any of the children is not assigned to
   * a cluster, the wNode will transfer its assignment to the child.
   * @param wNode the wholeStageCodeGen node to be visited
   * @param orphanNodes the list of nodes that are not assigned to any cluster.
   * @param clusters the clusterId to assign the node to.
   * @return true if a change is made.
   */
  private def commitNodeToStageCluster(
    wNode: SparkPlanGraphCluster,
    orphanNodes: mutable.ArrayBuffer[SparkPlanGraphNode],
    clusters: Set[Int]): Boolean = {
    var result = false
    if (nodeStageMapper.hasLogicKey(wNode.id)
        && clusters.subsetOf(nodeStageMapper.getLogicalNTS(wNode.id))) {
      // Nothing to do since the node is assigned to the same cluster before.
    } else {
      val newClusterIds =
        clusters ++ nodeStageMapper.getLogicalNTS(wNode.id)
      // Remove the wNode from orphanNodes if it exists
      result = removeNodeFromOrphans(wNode, orphanNodes, newClusterIds)
      // Assign the children to the same clusters if any of them is not assigned already.
      wNode.nodes.foreach { childNode =>
        if (!nodeStageMapper.hasLogicKey(childNode.id)) {
          // Assign the child node to the same stage of wNode and remove it from orphans
          result = removeNodeFromOrphans(childNode, orphanNodes, newClusterIds) || result
        }
      }
    }
    result
  }

  /**
   * Assign a node to a clusterId. This method is used to assign a node to a clusterId during the
   * first visit.
   * @param node sparkNode to be assigned
   * @param orphanNodes the list of nodes that are not assigned to any cluster
   * @param clusters the clusterIds to assign the node to
   * @return true if a change is made.
   */
  private def commitNodeToStageCluster(
    node: SparkPlanGraphNode,
    orphanNodes: mutable.ArrayBuffer[SparkPlanGraphNode],
    clusters: Set[Int]): Boolean = {
    node match {
      case cluster: SparkPlanGraphCluster =>
        // WholeCodeGen represents a special case because it propagates its assignment to
        // children nodes.
        commitNodeToStageCluster(cluster, orphanNodes, clusters)
      case _ =>
        removeNodeFromOrphans(node, orphanNodes, clusters)
        true
    }
  }

  /**
   * Walk through the graph nodes and assign them to the correct stage cluster.
   */
  protected def assignNodesToStageClusters(): Unit = {
    // Keep track of nodes that have no assignment to any cluster.
    val orphanNodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
    // Step(1): Visit all the nodes and assign them to the correct cluster based on AccumIDs.
    //      In the process, WholeStageCodeGens propagate their assignment to the child nodes if
    //      they are orphans.
    allNodes.foreach { node =>
      if (!nodeStageMapper.hasLogicKey(node.id)) {
        // Get clusterIDs based on AccumIds
        val clusterIds = populateNodeClusters(node)
        if (clusterIds.nonEmpty) {
          // Found assignment
          commitNodeToStageCluster(node, orphanNodes, clusterIds)
        } else {
          // This node has no assignment. Add it to the orphanNodes
          orphanNodes += node
        }
      }
    }
    // Step(2): At this point, we made a quick visit handling all the straightforward cases.
    //    Iterate on the orphanNodes and try to assign them based on the adjacent nodes.
    var changeFlag = orphanNodes.nonEmpty
    while (changeFlag) {
      // Iterate on the orphanNodes and try to assign them based on the adjacent nodes until no
      // changes can be done in a single iteration.
      changeFlag = false
      // P.S: Copy the orphanNodes because we cannot remove objects inside the loop.
      val orphanNodesCopy = orphanNodes.clone()
      orphanNodesCopy.foreach { currNode =>
        if (orphanNodes.contains(currNode)) { // Avoid dup processing caused by wholeStageCodeGen
          val currNodeName = ToolsPlanGraph.processPlanInfo(currNode.name)
          val updatedFlag = currNode match {
            case wNode: SparkPlanGraphCluster =>
              // WholeStageCodeGen is a corner case because it is not connected by edges.
              // The only way to set the clusterID is to get it from the children if any.
              wNode.nodes.find { childNode => nodeStageMapper.hasLogicKey(childNode.id) } match {
                case Some(childNode) =>
                  val clusterIDs = nodeStageMapper.getLogicalNTS(childNode.id)
                  commitNodeToStageCluster(wNode, orphanNodes, clusterIDs)
                case _ => // do nothing if we could not find a child node with a clusterId
                  false
              }
            case _ =>
              // Handle all other nodes.
              // Set the node type to determine the restrictions (i.e., exchange is
              // positioned at the tail of a stage and shuffleRead should be the head of a stage).
              val nodeCase = multiplexCases(currNodeName)
              var clusterIDs = ToolsPlanGraph.EMPTY_CLUSTERS
              if ((nodeCase & 1) > 0) {
                // Assign cluster based on incoming edges.
                val inEdgesWithIds =
                  edges.filter(e => e.toId == currNode.id && nodeStageMapper.hasLogicKey(e.fromId))
                if (inEdgesWithIds.nonEmpty) {
                  // For simplicity, assign the node based on the first incoming adjacent node.
                  clusterIDs = nodeStageMapper.getLogicalNTS(inEdgesWithIds.head.fromId)
                }
              }
              if (clusterIDs.isEmpty && (nodeCase & 2) > 0) {
                // Assign cluster based on outgoing edges (i.e., ShuffleRead).
                // Corner case: TPC-DS Like Bench q2 (sqlID 24).
                //              A shuffleReader is reading on driver followed by an exchange without
                //              metrics.
                //              The metrics will not have a valid accumID.
                //              In that case, it is not feasible to match it to a cluster without
                //              considering the incoming node (exchange in that case). This corner
                //              case is handled later as a last-ditch effort.
                val outEdgesWithIds =
                  edges.filter(e => e.fromId == currNode.id && nodeStageMapper.hasLogicKey(e.toId))
                if (outEdgesWithIds.nonEmpty) {
                  // For simplicity, assign the node based on the first outgoing adjacent node.
                  clusterIDs = nodeStageMapper.getLogicalNTS(outEdgesWithIds.head.toId)
                }
              }
              if (clusterIDs.nonEmpty) {
                // There is a possible assignment. Commit it.
                commitNodeToStageCluster(currNode, orphanNodes, clusterIDs)
              } else {
                // nothing has changed
                false
              }
          } // End of setting the UpdatedFlag variable.
          changeFlag |= updatedFlag
        } // End of if orphanNodes.contains(currNode).
      } // End of iteration on orphanNodes.
      // Corner case for shuffleRead when it is reading from the driver followed by an exchange that
      // has no metrics.
      if (!changeFlag && orphanNodes.nonEmpty) {
        // This is to handle the special case of a shuffleRead that is reading from the driver.
        // We could not assign any node to a cluster. This means that we have a cycle in the graph,
        // and we need to break it.
        // This is done by breaking the rule, allowing the shuffleRead to pick the highest stage
        // order of the ancestor node.
        changeFlag |= orphanNodes.filter(
          n => isPrologueExec(ToolsPlanGraph.processPlanInfo(n.name))).exists { // Picks shuffleRead
          orphanNode =>
            // Get adjacent nodes to the shuffleRead that have cluster assignment.
            val inEdgesWithIds =
              edges.filter(e => e.toId == orphanNode.id && nodeStageMapper.hasLogicKey(e.fromId))
            if (inEdgesWithIds.nonEmpty) {
              // At this point, we need to get all the possible stageIDs that can be assigned to the
              // adjacent nodes because and not only the logical ones.
              val possibleIds = inEdgesWithIds.map { e =>
                val adjacentNode = allNodes.find(eN => eN.id == e.fromId).get
                getAllNodeStages(adjacentNode)
              }.reduce(_ ++ _)
              // Assign the maximum value clusterId to the node.
              val newIDs = Set[Int](possibleIds.max)
              commitNodeToStageCluster(orphanNode, orphanNodes, newIDs)
            } else {
              false
            }
        }
      } // end of corner case handling
    } // end of changeFlag loop
  } // end of assignNodesToStageClusters
  // Start the construction of the graph
  assignNodesToStageClusters()

  // Define public interface methods
  /**
   * Get the stage clusters that the node belongs to.
   * Use this method if this logical representation of the node-to-stage relationship.
   * For example, an "Exchange" node returns only a single stageID which is the stage that writes
   * the data.
   * @param node the node to get the stages for
   * @return a set of stageIds or empty if None
   */
  def getNodeStageLogicalAssignment(node: SparkPlanGraphNode): Set[Int] = {
    getNodeStageLogicalAssignment(node.id)
  }
  /**
   * Get the stage clusters that the node belongs to.
   * Use this method if this logical representation of the node-to-stage relationship.
   * For example, an "Exchange" node returns only a single stageID which is the stage that writes
   * the data.
   * @param nodeId the nodeId to get the stages for
   * @return a set of stageIds or empty if None
   */
  def getNodeStageLogicalAssignment(nodeId: Long): Set[Int] = {
    nodeStageMapper.getLogicalNTS(nodeId)
  }

  /**
   * Get the stage clusters that the node belongs to based on the accumulableIds. This method is
   * used when the caller needs the raw representation of node-to-stage without any heuristics.
   * @param nodeId the id of the SQLNode
   * @return a set of stageIds or empty if None
   */
  def getNodeStageRawAssignment(nodeId: Long): Set[Int] = {
    nodeStageMapper.getRawNTS(nodeId)
  }

  /**
   * Get the nodes that belong to a specific stageId. This method uses the raw assignment based on
   * accumulableIds and does not apply any heuristics or logic to bind between nodes and stages.
   * This method is used to speed-up retrieval of stages O(1).
   * @param stageId the if of the stage
   * @return a set of nodeIds or empty if None
   */
  def getStageNodesByRawAssignment(stageId: Int): collection.Set[Long] = {
    nodeStageMapper.getRawSTN(stageId)
  }

  /**
   * retrieve all the sinkNodes adajacent to a given node.
   * @param nodeId the id of the current node
   * @return a set of nodeIds, or empty if none.
   */
  def getSinkNodes(nodeId: Long): collection.Set[Long] = {
    edges.filter(_.fromId == nodeId).map(_.toId).toSet
  }
} // end of class ToolsPlanGraph

/**
 * This code is mostly copied from org.apache.spark.sql.execution.ui.SparkPlanGraph
 * with changes to handle GPU nodes. Without this special handle, the default SparkPlanGraph
 * would not recognize reused/exchange nodes leading to duplicating nodes.
 *
 * Build a SparkPlanGraph from the root of a SparkPlan tree.
 */
object ToolsPlanGraph {
  val EMPTY_NODES: Set[Long] = Set.empty
  // Empty cluster set used to represent a node that is not assigned to any cluster.
  val EMPTY_CLUSTERS: Set[Int] = Set.empty
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

  def createGraphWithStageClusters(planInfo: SparkPlanInfo,
      accumStageMapper: AccumToStageRetriever): ToolsPlanGraph = {
    val sGraph = ToolsPlanGraph(planInfo)
    new ToolsPlanGraph(sGraph, accumStageMapper)
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
