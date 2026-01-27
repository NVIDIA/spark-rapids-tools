/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser.iceberg

import com.nvidia.spark.rapids.tool.planparser.{ExecParser, GroupParserTrait, SupportedOpStub}
import com.nvidia.spark.rapids.tool.planparser.ops.OpTypes
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.store.WriteOperationMetadataTrait
import org.apache.spark.sql.rapids.tool.util.CacheablePropsHandler

/**
 * A GroupParserTrait implementation for Iceberg operations.
 * This class identifies and creates ExecParsers for Iceberg-specific operations
 * based on the node name and configuration.
 *
 * Handles:
 * - AppendData: Write operation for INSERT/APPEND
 * - MergeRows: Intermediate exec for MERGE INTO operations (handles merge logic)
 * - ReplaceData: Write operation for copy-on-write MERGE INTO
 * - WriteDelta: Write operation for merge-on-read MERGE INTO
 *
 * MERGE INTO DAG structures:
 * {{{
 *   Copy-on-Write (CoW):
 *     ReplaceData (final write)
 *     +- Project
 *        +- MergeRows (merge logic)
 *           +- SortMergeJoin FullOuter
 *
 *   Merge-on-Read (MoR):
 *     WriteDelta (final write - delete files)
 *     +- Exchange
 *        +- MergeRows (merge logic)
 *           +- SortMergeJoin RightOuter
 * }}}
 */
object IcebergOps extends GroupParserTrait {
  // A Map between the spark node name and the SupportedOpStub
  // Includes all Iceberg-specific execs
  private val DEFINED_EXECS: Map[String, SupportedOpStub] = IcebergHelper.DEFINED_EXECS

  /**
   * Checks whether this parserGroup should handle the given node name.
   *
   * @param nodeName name of the node to check
   * @return true if this parserGroup can handle the node, false otherwise
   */
  override def accepts(nodeName: String): Boolean = {
    DEFINED_EXECS.contains(nodeName)
  }

  /**
   * Checks whether this parserGroup should handle the given node name.
   * If conf-provider is defined, it checks if the providerImpl is Iceberg.
   *
   * @param nodeName     name of the node to check
   * @param confProvider optional configuration provider
   * @return true if this parserGroup can handle the node, false otherwise
   */
  override def accepts(
      nodeName: String,
      confProvider: Option[CacheablePropsHandler]): Boolean = {
    confProvider match {
      case Some(a) if a.isIcebergEnabled =>
        a.isIcebergEnabled && accepts(nodeName)
      case _ => accepts(nodeName)
    }
  }

  /**
   * Checks whether this parserGroup should handle the given node.
   *
   * @param node         spark plan graph node
   * @param confProvider optional configuration provider.
   *                     This is used to access the spark configurations to decide whether
   *                     the node is handled by the parserGroup. For example, check if the
   *                     providerImpl is Iceberg/DeltaLake
   * @return true if this parserGroup can handle the node, false otherwise
   */
  override def accepts(
      node: SparkPlanGraphNode,
      confProvider: Option[CacheablePropsHandler]): Boolean = {
    accepts(node.name, confProvider)
  }

  override def createExecParser(
      node: SparkPlanGraphNode,
      checker: PluginTypeChecker,
      sqlID: Long,
      execName: Option[String],
      opType: Option[OpTypes.Value],
      app: Option[AppBase]): ExecParser = {
    DEFINED_EXECS.get(node.name) match {
      case Some(stub) if stub.execID.equals("AppendDataExec") =>
        new AppendDataIcebergParser(
          node = node,
          checker = checker,
          sqlID = sqlID,
          opStub = stub,
          app = app
        )
      // MergeRows: stub.execID will be "MergeRowsExec" (auto-appended by SupportedOpStub)
      case Some(stub) if stub.execID.equals("MergeRowsExec") =>
        new MergeRowsIcebergParser(
          node = node,
          checker = checker,
          sqlID = sqlID,
          opStub = stub,
          app = app
        )
      // ReplaceData: Write operator for copy-on-write MERGE INTO
      case Some(stub) if stub.execID.equals("ReplaceDataExec") =>
        new MergeRowsIcebergParser(
          node = node,
          checker = checker,
          sqlID = sqlID,
          opStub = stub,
          app = app
        )
      // WriteDelta: Write operator for merge-on-read MERGE INTO
      // Writes "delete files" instead of rewriting data files
      case Some(stub) if stub.execID.equals("WriteDeltaExec") =>
        new MergeRowsIcebergParser(
          node = node,
          checker = checker,
          sqlID = sqlID,
          opStub = stub,
          app = app
        )
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported Iceberg op: ${node.name}"
        )
    }
  }

  /**
   * Extracts write operation metadata from Iceberg write operators for inclusion in
   * WriteOperationRecords (used for write format reporting).
   *
   * Iceberg Operators and Write Metadata:
   * - AppendData: Extracted from simpleString (node.desc) - contains "IcebergWrite(table=...)"
   * - ReplaceData: Extracted from physicalPlanDescription - Arguments has "IcebergWrite(...)"
   * - WriteDelta: Extracted from physicalPlanDescription - indicates position delete format
   * - MergeRows: Returns None - NOT a write operation (OpType: Exec), correctly excluded
   *
   * @param node the SparkPlanGraphNode to extract metadata from
   * @param confProvider optional configuration provider to determine if Iceberg is enabled
   * @param physicalPlanDescription optional physical plan description string for extracting
   *                                ReplaceData/WriteDelta metadata (not available in simpleString)
   * @return Some(WriteOperationMetadataTrait) if extraction is successful, None otherwise
   */
  def extractOpMeta(
      node: SparkPlanGraphNode,
      confProvider: Option[CacheablePropsHandler],
      physicalPlanDescription: Option[String] = None
  ): Option[WriteOperationMetadataTrait] = {
    if (!accepts(node, confProvider)) {
      return None
    }

    node.name match {
      // AppendData, ReplaceData, WriteDelta: Use unified IcebergWriteExtract
      case name if IcebergWriteExtract.accepts(name) =>
        IcebergWriteExtract.buildWriteOp(
          opName = name,
          nodeDescr = node.desc,
          physicalPlanDescription = physicalPlanDescription,
          nodeId = node.id
        )

      // MergeRows: NOT a write operation - intentionally excluded
      case IcebergHelper.EXEC_MERGE_ROWS =>
        None

      case _ =>
        None
    }
  }
}
