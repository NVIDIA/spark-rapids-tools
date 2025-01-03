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

package com.nvidia.spark.rapids.tool.planparser

import com.nvidia.spark.rapids.tool.planparser.ops.UnsupportedExprOpRef
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode

case class WindowGroupLimitParser(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long) extends ExecParser {

  val fullExecName: String = node.name + "Exec"
  // row_number() is currently not supported by the plugin (v24.04)
  // Ref: https://github.com/NVIDIA/spark-rapids/pull/10500
  val supportedRankingExprs = Set("rank", "dense_rank", "row_number")

  private def validateRankingExpr(rankingExprs: Array[String]): Boolean = {
    rankingExprs.length == 1 && supportedRankingExprs.contains(rankingExprs.head)
  }

  override def getUnsupportedExprReasonsForExec(
      expressions: Array[String]): Seq[UnsupportedExprOpRef] = {
    expressions.distinct.flatMap { expr =>
      if (!supportedRankingExprs.contains(expr)) {
        Some(UnsupportedExprOpRef(expr,
          s"Ranking function $expr is not supported in $fullExecName"))
      } else {
        None
      }
    }
  }

  /**
   * Node Description:
   * WindowGroupLimit [category#16], [amount#17 DESC NULLS LAST], dense_rank(amount#17), 2, Final
   *
   * Support criteria:
   * 1. Exec is supported by the plugin.
   * 2. Ranking function is supported by the plugin.
   * 3. Ranking function is supported by plugin's implementation of WindowGroupLimitExec.
   */
  override def parse: ExecInfo = {
    val exprString = node.desc.replaceFirst("WindowGroupLimit\\s*", "")
    val expressions = SQLPlanParser.parseWindowGroupLimitExpressions(exprString)
    val notSupportedExprs = checker.getNotSupportedExprs(expressions) ++
        getUnsupportedExprReasonsForExec(expressions)
    // Check if exec is supported and ranking expression is supported.
    val isExecSupported = checker.isExecSupported(fullExecName)
    val areAllExprsSupported = notSupportedExprs.isEmpty && validateRankingExpr(expressions)
    val (speedupFactor, isSupported) = if (isExecSupported && areAllExprsSupported) {
      (checker.getSpeedupFactor(fullExecName), true)
    } else {
      (1.0, false)
    }
    ExecInfo(node, sqlID, node.name, "", speedupFactor, None, node.id, isSupported, None,
      unsupportedExprs = notSupportedExprs, expressions = expressions)
  }
}
