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
import org.apache.spark.sql.rapids.tool.AppBase

/**
 * A parser meant to be used for generic exec nodes that do not have a specific parser.
 */
class GenericExecParser(
    val node: SparkPlanGraphNode,
    val checker: PluginTypeChecker,
    val sqlID: Long,
    val execName: Option[String] = None,
    val expressionFunction: Option[String => Array[String]] = None,
    val app: Option[AppBase] = None
) extends ExecParser {

  var unsupportedReason = ""

  lazy val trimmedNodeName: String = GenericExecParser.cleanupNodeName(node)

  lazy val fullExecName: String = execName.getOrElse(trimmedNodeName + "Exec")

  def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    checker.isExecSupported(registeredName.getOrElse(fullExecName))
  }

  def pullSpeedupFactor(registeredName: Option[String] = None): Double = {
    checker.getSpeedupFactor(registeredName.getOrElse(fullExecName))
  }

  def setUnsupportedReason(r: String): Unit = {
    unsupportedReason = r
  }

  override def parse: ExecInfo = {
    val duration = computeDuration
    val expressions = parseExpressions()

    val notSupportedExprs = getNotSupportedExprs(expressions)
    val isExecSupported = pullSupportedFlag() && notSupportedExprs.isEmpty

    val (speedupFactor, isSupported) = if (isExecSupported) {
      (pullSpeedupFactor(), true)
    } else {
      (1.0, false)
    }

    createExecInfo(speedupFactor, isSupported, duration,
      notSupportedExprs = notSupportedExprs, expressions = expressions)
  }

  protected def parseExpressions(): Array[String] = {
    expressionFunction match {
      case Some(func) =>
        val exprString = getExprString
        func(exprString)
      case None =>
        Array.empty[String]
    }
  }

  // By default, there are no children. Override this method if there are children.
  def getChildren: Option[Seq[ExecInfo]] = None

  protected def getExprString: String = {
    node.desc.replaceFirst(s"^${node.name}\\s*", "")
  }

  protected def getNotSupportedExprs(expressions: Array[String]): Seq[UnsupportedExprOpRef] = {
    checker.getNotSupportedExprs(expressions)
  }

  protected def getDurationSqlMetrics: Set[String] = Set.empty

  protected def getDurationMetricIds: Seq[Long] = {
    node.metrics.find(m => getDurationSqlMetrics.contains(m.name)).map(_.accumulatorId).toSeq
  }

  protected def computeDuration: Option[Long] = {
    // Sum the durations for all metrics returned by getDurationMetricIds
    val durations = getDurationMetricIds.flatMap { metricId =>
      app.flatMap(appInstance => SQLPlanParser.getTotalDuration(Some(metricId), appInstance))
    }
    durations.reduceOption(_ + _)
  }

  // The value that will be reported as ExecName in the ExecInfo object created by this parser.
  def reportedExecName: String = trimmedNodeName

  protected def createExecInfo(
      speedupFactor: Double,
      isSupported: Boolean,
      duration: Option[Long],
      notSupportedExprs: Seq[UnsupportedExprOpRef],
      expressions: Array[String]
  ): ExecInfo = {
    ExecInfo(
      node,
      sqlID,
      // Remove trailing spaces from node name if any
      reportedExecName,
      "",
      speedupFactor,
      duration,
      node.id,
      isSupported,
      children = getChildren,
      unsupportedExprs = notSupportedExprs,
      expressions = expressions
    )
  }
}

object GenericExecParser {
  def cleanupNodeName(nodeName: String): String = {
    nodeName.trim
  }
  def cleanupNodeName(node: SparkPlanGraphNode): String = {
    cleanupNodeName(node.name)
  }
  def apply(
      node: SparkPlanGraphNode,
      checker: PluginTypeChecker,
      sqlID: Long,
      execName: Option[String] = None,
      expressionFunction: Option[String => Array[String]] = None,
      app: Option[AppBase] = None
  ): GenericExecParser = {
    val fullExecName = execName.getOrElse(node.name + "Exec")
    new GenericExecParser(node, checker, sqlID, Some(fullExecName), expressionFunction, app)
  }
}
