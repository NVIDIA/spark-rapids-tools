/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode

abstract class BroadcastNestedLoopJoinExecParserBase(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long) extends ExecParser {

  val fullExecName: String = node.name + "Exec"

  protected def extractBuildAndJoinTypes(exprStr: String): (String, String) = {
    // BuildRight, LeftOuter, ((CEIL(cast(id1#1490 as double)) <= cast(id2#1496 as bigint))
    // AND (cast(id1#1490 as bigint) < CEIL(cast(id2#1496 as double))))
    // Get joinType and buildSide by splitting the input string.
    val nestedLoopParameters = exprStr.split(",", 3)
    val buildSide = nestedLoopParameters(0).trim
    val joinType = nestedLoopParameters(1).trim
    (buildSide, joinType)
  }

  override def parse: ExecInfo = {
    // BroadcastNestedLoopJoin doesn't have duration
    val exprString = node.desc.replaceFirst("^BroadcastNestedLoopJoin\\s*", "")
    val (buildSide, joinType) = extractBuildAndJoinTypes(exprString)
    val (expressions, supportedJoinType) =
      SQLPlanParser.parseNestedLoopJoinExpressions(exprString, buildSide, joinType)
    val notSupportedExprs = expressions.filterNot(expr => checker.isExprSupported(expr))
    val duration = None
    val (speedupFactor, isSupported) = if (checker.isExecSupported(fullExecName) &&
      notSupportedExprs.isEmpty && supportedJoinType) {
      (checker.getSpeedupFactor(fullExecName), true)
    } else {
      (1.0, false)
    }
    ExecInfo(node, sqlID, node.name, "", speedupFactor, duration, node.id, isSupported,
      children = None, expressions = expressions)
  }
}

case class BroadcastNestedLoopJoinExecParser(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long)
  extends BroadcastNestedLoopJoinExecParserBase(node, checker, sqlID)
