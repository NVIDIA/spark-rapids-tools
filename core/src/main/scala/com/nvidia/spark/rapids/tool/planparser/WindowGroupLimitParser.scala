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

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode

case class WindowGroupLimitParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    override val app: Option[AppBase]
) extends GenericExecParser(
    node,
    checker,
    sqlID,
    execName = Some("WindowGroupLimitExec"),
    expressionFunction = Some(SQLPlanParser.parseWindowGroupLimitExpressions),
    app = app
) {

  private var parsedExpressions = Array.empty[String]

  private val supportedRankingExprs = Set("rank", "dense_rank", "row_number")

  private def validateRankingExpr(rankingExprs: Array[String]): Boolean = {
    rankingExprs.length == 1 && supportedRankingExprs.contains(rankingExprs.head)
  }

  override protected def parseExpressions(): Array[String] = {
    parsedExpressions = super.parseExpressions()
    parsedExpressions
  }

  override protected def getNotSupportedExprs(
      expressions: Array[String]): Seq[UnsupportedExprOpRef] = {
    // Get unsupported expressions from parent
    val parentUnsupported = super.getNotSupportedExprs(expressions)
    // Add our custom ranking expression validation
    val rankingUnsupported = getUnsupportedExprReasonsForExec(expressions)
    parentUnsupported ++ rankingUnsupported
  }

  override def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    // Parse expressions to check ranking validation
    super.pullSupportedFlag(registeredName) && validateRankingExpr(parsedExpressions)
  }

  private def getUnsupportedExprReasonsForExec(
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
}
