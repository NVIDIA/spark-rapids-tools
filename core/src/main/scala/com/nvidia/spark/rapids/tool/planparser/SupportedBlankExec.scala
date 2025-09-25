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

import com.nvidia.spark.rapids.tool.planparser.ops.UnsupportedExprOpRef
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.ExecHelper.{EX_NAME_WRITE_FILES, LIST_OF_SUPPORTED_BLANK_EXECS}

/**
 * A generic parser for supported execs that do not have any expressions to parse.
 * This parser assumes the exec is supported with a speedup factor of 1.5.
 * If you want to handle a specific exec differently, create a new parser for it.
 */
class SupportedBlankExec(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    override val execName: Option[String] = None,
    opType: Option[OpTypes.Value] = None,
    override val app: Option[AppBase]
) extends GenericExecParser(
    node = node,
    checker = checker,
    sqlID = sqlID,
    execName = execName,
    app = app) {

  override def pullSpeedupFactor(registeredName: Option[String] = None): Double = 1.5

  def pullOpType: OpTypes.Value = {
    opType match {
      case Some(v) => v
      case None =>
        // Determine opType based on node name if not provided
        SupportedBlankExec.EXEC_TYPES.getOrElse(trimmedNodeName, OpTypes.Exec)
    }
  }

  override def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    true
  }

  // If execName is provided, use it. Otherwise, derive from node name.
  override lazy val fullExecName: String = {
    execName match {
      case Some(v) => v
      case _ =>
        // If the exec has a composite name (i.e., Execute XYZ). keep the name as is
        if (trimmedNodeName.exists(_.isWhitespace)) {
          trimmedNodeName
        } else {
          trimmedNodeName + "Exec"
        }
    }
  }

  // No expressions to parse for this exec.
  override def parseExpressions(): Array[String] = Array.empty[String]

  // No expressions to check for this exec.
  override def getNotSupportedExprs(
      expressions: Array[String]): Seq[UnsupportedExprOpRef] = Seq.empty

  /**
   * Create the ExecInfo object for this exec.
   * @param speedupFactor the speedup factor for this exec
   * @param isSupported whether this exec is supported
   * @param duration the duration of this exec
   * @param notSupportedExprs the list of unsupported expressions for this exec
   * @param expressions the list of expressions for this exec
   * @return the ExecInfo object.
   */
  override def createExecInfo(
      speedupFactor: Double,
      isSupported: Boolean,
      duration: Option[Long],
      notSupportedExprs: Seq[UnsupportedExprOpRef],
      expressions: Array[String]): ExecInfo = {
    ExecInfo.createExecNoNode(
      sqlID,
      reportedExecName,
      "",
      pullSpeedupFactor(),
      duration,
      node.id,
      opType = pullOpType,
      isSupported = isSupported,
      children = getChildren,
      unsupportedExprs = notSupportedExprs,
      expressions = expressions)
  }
}

object SupportedBlankExec extends GroupParserTrait {
  private val EXEC_TYPES: Map[String, OpTypes.Value] = Map(
    EX_NAME_WRITE_FILES -> OpTypes.WriteExec
  )

  override def accepts(nodeName: String): Boolean = {
    // This is a catch-all parser for any exec that is not specifically handled elsewhere.
    // It assumes the exec is supported with a speedup factor of 1.5.
    // If you want to handle a specific exec differently, create a new parser for it.
    LIST_OF_SUPPORTED_BLANK_EXECS.contains(nodeName.trim)
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
      node: SparkPlanGraphNode, checker: PluginTypeChecker,
      sqlID: Long, execName: Option[String],
      opType: Option[OpTypes.Value],
      app: Option[AppBase]): ExecParser = {
    new SupportedBlankExec(node, checker, sqlID, execName, opType, app)
  }
}
