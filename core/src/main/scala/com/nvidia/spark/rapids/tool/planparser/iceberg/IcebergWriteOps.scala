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

package com.nvidia.spark.rapids.tool.planparser.iceberg

import com.nvidia.spark.rapids.tool.planparser.{ExecParser, GroupParserTrait, OpTypes, SupportedOpStub}
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.store.WriteOperationMetadataTrait
import org.apache.spark.sql.rapids.tool.util.CacheablePropsHandler

/**
 * A GroupParserTrait implementation for Iceberg write operations.
 * This class identifies and creates ExecParsers for Iceberg write operations
 * based on the node name and configuration.
 */
object IcebergWriteOps extends GroupParserTrait {
  // A Map between the spark node name and the SupportedOpStub
  private val DEFINED_EXECS: Map[String, SupportedOpStub] =
    IcebergHelper.DEFINED_EXECS.filter {
      case (_, stub) => stub.opType.contains(OpTypes.WriteExec)
    }

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
   * @param confProvider optional configuration provider.
   *                     This is used to access the spark configurations to decide whether
   *                     the node is handled by the parserGroup. For example, check if the
   *                     providerImpl is Iceberg/DeltaLake
   * @return true if this parserGroup can handle the node, false otherwise
   */
  override def accepts(
      nodeName: String,
      confProvider: Option[CacheablePropsHandler]): Boolean = {
    confProvider match {
      case Some(a) if a.icebergEnabled =>
        a.icebergEnabled && accepts(nodeName)
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

  /**
   * Create an ExecParser for the given node.
   *
   * @param node     spark plan graph node
   * @param checker  plugin type checker
   * @param sqlID    SQL ID
   * @param execName optional exec name override
   * @param opType   optional op type override
   * @param app      optional AppBase instance
   * @return an ExecParser for the given node
   */
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
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported Iceberg write op: ${node.name}"
        )
    }
  }

  /**
   * Extracts the WriteOperationMetadataTrait from the given SparkPlanGraphNode if it is an
   * accepted Iceberg write operation.
   * If the node is not accepted, it returns None.
   * @param node the SparkPlanGraphNode to extract metadata from
   * @param confProvider optional configuration provider to determine if Iceberg is enabled
   * @return Some(WriteOperationMetadataTrait) if extraction is successful, None otherwise
   */
  def extractOpMeta(
      node: SparkPlanGraphNode,
      confProvider: Option[CacheablePropsHandler]
  ): Option[WriteOperationMetadataTrait] = {
    if (!accepts(node, confProvider)) {
      return None
    }
    node match {
      case n if AppendDataIcebergExtract.accepts(n.name) =>
        Some(AppendDataIcebergExtract.buildWriteOp(n.desc))
      case _ =>
        None
    }
  }
}
