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

import com.nvidia.spark.rapids.tool.planparser.ops.UnsupportedReasonRef
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode


abstract class BroadcastNestedLoopJoinExecParserBase(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    override val app: Option[AppBase]
) extends GenericExecParser(
    node,
    checker,
    sqlID,
    execName = Option("BroadcastNestedLoopJoinExec"),
    app = app
) {

  private var supportedJoinType: Boolean = true

  protected def extractBuildAndJoinTypes(exprStr: String): (String, String) = {
    // BuildRight, LeftOuter, ((CEIL(cast(id1#1490 as double)) <= cast(id2#1496 as bigint))
    // AND (cast(id1#1490 as bigint) < CEIL(cast(id2#1496 as double))))
    // Get joinType and buildSide by splitting the input string.
    val nestedLoopParameters = exprStr.split(",", 3)
    val buildSide = nestedLoopParameters(0).trim
    val joinType = nestedLoopParameters(1).trim
    (buildSide, joinType)
  }

  override protected def parseExpressions(): Array[String] = {
    val exprString = getExprString
    val (buildSide, joinType) = extractBuildAndJoinTypes(exprString)
    val (expressions, joinTypeIsSupported) =
      SQLPlanParser.parseNestedLoopJoinExpressions(exprString, buildSide, joinType)
    supportedJoinType = joinTypeIsSupported
    if (!supportedJoinType) {
      setUnsupportedReason(UnsupportedReasonRef.UNSUPPORTED_JOIN_TYPE)
    }
    expressions
  }

  override def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    supportedJoinType && super.pullSupportedFlag(registeredName)
  }

}

case class BroadcastNestedLoopJoinExecParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    override val app: Option[AppBase]
) extends BroadcastNestedLoopJoinExecParserBase(node, checker, sqlID, app)
