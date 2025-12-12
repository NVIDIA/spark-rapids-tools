/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode

case class SortMergeJoinExecParser(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long) extends ExecParser {
  private val opName = "SortMergeJoin"

  val fullExecName = opName + "Exec"

  override def parse: ExecInfo = {
    // SortMergeJoin doesn't have duration
    val duration = None
    // parse the description after getting rid of the prefix that can include an argument
    val trimPosition = node.desc.indexOf(" ")
    val exprString = node.desc.substring(trimPosition + 1)
    val (expressions, supportedJoinType) = SQLPlanParser.parseEquijoinsExpressions(exprString)
    val notSupportedExprs = expressions.filterNot(expr => checker.isExprSupported(expr))
    val (speedupFactor, isSupported) = if (supportedJoinType &&
      checker.isExecSupported(fullExecName) && notSupportedExprs.isEmpty) {
      (checker.getSpeedupFactor(fullExecName), true)
    } else {
      (1.0, false)
    }
    ExecInfo(node, sqlID, opName, "", speedupFactor, duration, node.id, isSupported,
      children = None, expressions = expressions)
  }
}

object SortMergeJoinExecParser {
  private val execNameRegEx = "(SortMergeJoin)(?:\\(.+\\))?".r

  def accepts(nodeName: String): Boolean = {
    nodeName.matches(execNameRegEx.regex)
  }

}
