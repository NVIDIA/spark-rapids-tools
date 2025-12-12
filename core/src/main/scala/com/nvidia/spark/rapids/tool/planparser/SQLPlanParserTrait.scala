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

package com.nvidia.spark.rapids.tool.planparser

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.util.stubs.SparkPlanInfo

/**
 * Trait defining the contract for parsing Spark SQL execution plans.
 *
 * This trait establishes the interface for parsing physical execution plans from different
 * Spark platforms (OSS Spark, Photon, etc.) into a unified ExecInfo structure for analysis
 * and GPU acceleration qualification.
 *
 * Implementations of this trait must provide:
 * - Plan-level parsing: Converting entire SQL execution plans into analyzable structures
 * - Node-level parsing: Handling individual operators and cluster nodes
 * - Platform detection: Identifying cluster nodes vs. regular operators
 *
 * Key use cases:
 * - GPU acceleration analysis and qualification
 * - Performance profiling and optimization recommendations
 * - Cross-platform execution plan analysis
 */
trait SQLPlanParserTrait {
  /**
   * Parses a complete SQL execution plan into structured ExecInfo for analysis.
   *
   * This is the top-level parsing method that processes an entire SQL query's physical
   * execution plan. Implementations should handle:
   * - Building or retrieving plan graphs with stage-to-operator mappings
   * - Identifying and excluding duplicate nodes (e.g., from ReusedExchange)
   * - Recursively parsing all operators and cluster nodes
   * - Applying platform-specific transformations and validations
   *
   * @param planInfo Root SparkPlanInfo containing the execution plan tree
   * @param sqlID Unique identifier for the SQL query within the application
   * @param sqlDesc Human-readable description or text of the SQL query
   * @param checker PluginTypeChecker for determining GPU support status of operators
   * @param app Application context containing platform info, event log data, and metrics
   * @return PlanInfo containing all parsed operators with GPU support status and performance data
   */
  def parseSQLPlan(
      planInfo: SparkPlanInfo,
      sqlID: Long,
      sqlDesc: String,
      checker: PluginTypeChecker,
      app: AppBase
  ): PlanInfo

  /**
   * Parses a single plan graph node (operator or cluster) into ExecInfo structure(s).
   *
   * This method serves as the main dispatcher for node-level parsing. It should:
   * - Detect duplicate nodes to avoid double-counting in performance analysis
   * - Route cluster nodes (WholeStageCodegen, Photon stages) to cluster parsers
   * - Route individual operators to appropriate specialized parsers
   * - Handle parsing errors gracefully without failing the entire analysis
   * - Correlate operators with their execution stages for performance metrics
   *
   * @param node SparkPlanGraphNode to parse (may be a cluster or individual operator)
   * @param sqlID SQL query identifier
   * @param checker PluginTypeChecker for GPU support determination
   * @param app Application context
   * @param reusedNodeIds Set of node IDs that are reused (from ReusedExchange) and should
   *                      be marked as shouldRemove to avoid double-counting
   * @param nodeIdToStagesFunc Function mapping node IDs to their execution stage IDs for
   *                           correlating operators with task-level metrics
   * @return Sequence of ExecInfo (typically one, but clusters may return multiple)
   */
  def parsePlanNode(
      node: SparkPlanGraphNode,
      sqlID: Long,
      checker: PluginTypeChecker,
      app: AppBase,
      reusedNodeIds: Set[Long],
      nodeIdToStagesFunc: Long => Set[Int]
  ): Seq[ExecInfo]

  /**
   * Parses an individual operator node (non-cluster) into ExecInfo.
   *
   * This method handles the core operator parsing logic by routing each operator type
   * to its specialized parser. Implementations should:
   * - Match operator names to appropriate parser classes
   * - Extract expressions from the operator for GPU support analysis
   * - Determine GPU support status and calculate speedup factors
   * - Parse operator-specific metadata (join types, scan formats, etc.)
   *
   * @param node SparkPlanGraphNode representing a single operator
   * @param sqlID SQL query identifier
   * @param checker PluginTypeChecker for GPU support
   * @param app Application context
   * @return ExecInfo containing parsed operator details including support status and metrics
   */
  def parseGraphNode(
      node: SparkPlanGraphNode,
      sqlID: Long,
      checker: PluginTypeChecker,
      app: AppBase
  ): ExecInfo

  /**
   * Parses a cluster node (WholeStageCodegen or platform-specific stage) into ExecInfo.
   *
   * Cluster nodes represent groups of operators compiled and executed together for
   * performance optimization. This method should:
   * - Parse all child operators within the cluster
   * - Aggregate GPU support status across children
   * - Calculate cluster-level speedup factors
   * - Preserve individual operator details as nested children
   *
   * @param node SparkPlanGraphNode representing a cluster (e.g., WholeStageCodegen)
   * @param sqlID SQL query identifier
   * @param checker PluginTypeChecker for GPU support
   * @param app Application context
   * @param reusedNodeIds Set of reused node IDs to mark as shouldRemove
   * @param nodeIdToStagesFunc Function mapping node IDs to stage IDs
   * @return Sequence of ExecInfo containing the cluster with nested child operators
   */
  def parseClusterNode(
      node: SparkPlanGraphNode,
      sqlID: Long,
      checker: PluginTypeChecker,
      app: AppBase,
      reusedNodeIds: Set[Long],
      nodeIdToStagesFunc: Long => Set[Int]
  ): Seq[ExecInfo]

  /**
   * Determines if a graph node represents a cluster node.
   *
   * Cluster nodes group multiple operators that execute together as a compiled unit.
   * Examples include:
   * - WholeStageCodegen (standard Spark)
   * - PhotonShuffleMapStage, PhotonResultStage (Databricks Photon)
   *
   * Implementations should use type checking or platform-specific logic to identify clusters.
   *
   * @param node The graph node to check
   * @return true if the node represents a cluster, false for individual operators
   */
  def isClusterNode(node: SparkPlanGraphNode): Boolean
}
