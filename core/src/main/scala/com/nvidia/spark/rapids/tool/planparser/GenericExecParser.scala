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

import com.nvidia.spark.rapids.tool.planparser.ops.{UnsupportedExprOpRef, UnsupportedReasonRef}
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode

/**
 * A parser meant to be used for generic exec nodes that do not have a specific parser.
 */
class GenericExecParser(
    val node: SparkPlanGraphNode,
    val checker: PluginTypeChecker,
    val sqlID: Long,
    val execName: Option[String] = None,
    val expressionFunction: Option[String => Array[String]] = None,
    var unsupportedReason: Option[UnsupportedReasonRef] = None,
    val app: Option[AppBase] = None
) extends ExecParser {
  lazy val trimmedNodeName: String = GenericExecParser.cleanupNodeName(node)

  lazy val fullExecName: String = execName.getOrElse(trimmedNodeName + "Exec")

  /**
   * Set of SQL metric names used to calculate the duration of this operator.
   *
   * Override this field in child classes to specify which metrics should be summed
   * to compute the operator's total duration. The metric names should match exactly
   * as they appear in the Spark execution plan (e.g., "scan time", "time to build").
   *
   * By default, this is empty, meaning no duration will be calculated.
   *
   * Note: The actual metric lookup supports substring matching - if a metric name in the
   * plan contains any of these strings, it will be included. For example, "scan time"
   * will match "scan time (ms)".
   *
   * See also:
   * - `useDriverMetrics`: Set to true if these metrics are driver-side (not executor-side)
   * - `getDurationMetricIds`: Retrieves the accumulator IDs for these metrics
   * - `computeDuration`: Sums the metric values to calculate total duration
   *
   * Examples:
   * - Scan operators: Set("scan time", "metadata time")
   * - Aggregate operators: Set("time in aggregation build")
   * - Exchange operators: Set("time to collect", "time to build", "time to broadcast")
   */
  protected val durationSqlMetrics: Set[String] = Set.empty

  def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    checker.isExecSupported(registeredName.getOrElse(fullExecName))
  }

  def pullSpeedupFactor(registeredName: Option[String] = None): Double = {
    checker.getSpeedupFactor(registeredName.getOrElse(fullExecName))
  }

  def setUnsupportedReason(r: UnsupportedReasonRef): Unit = {
    unsupportedReason = Some(r)
  }

  def setUnsupportedReason(r: String): Unit = {
    // Only set the reason if the unsupportedReason is not already set
    if (unsupportedReason.isEmpty) {
      r match {
        case "" => unsupportedReason = None
        case _ => unsupportedReason = Some(UnsupportedReasonRef.getOrCreate(r))
      }
    }
  }

  def setUnsupportedReasonFromChecker(registeredName: Option[String] = None): Unit = {
    if (unsupportedReason.isEmpty) {
      setUnsupportedReason(
        checker.getNotSupportedExecsReason(registeredName.getOrElse(fullExecName)))
    }
  }

  override def parse: ExecInfo = {
    val duration = computeDuration
    val expressions = parseExpressions()

    val notSupportedExprs = getNotSupportedExprs(expressions)
    val isExecSupported = pullSupportedFlag() && notSupportedExprs.isEmpty

    val (speedupFactor, isSupported) = if (isExecSupported) {
      (pullSpeedupFactor(), true)
    } else {
      // Set the custom reasons for unsupported execs
      setUnsupportedReasonFromChecker()
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

  /** Override this to return true if the metrics are driver-side (not executor-side) */
  protected def useDriverMetrics: Boolean = false

  protected def getDurationMetricIds: Seq[Long] = {
    if (durationSqlMetrics.isEmpty) {
      return Seq.empty
    }
    node.metrics.collect {
      case m if durationSqlMetrics.contains(m.name) => m.accumulatorId
    }.toSeq
  }

  protected def computeDuration: Option[Long] = {
    // Sum the durations for all metrics returned by getDurationMetricIds
    val durations = getDurationMetricIds.flatMap { metricId =>
      app.flatMap { appInstance =>
        if (useDriverMetrics) {
          // this is for metrics that are only available on the driver
          SQLPlanParser.getDriverTotalDuration(Some(metricId), appInstance)
        } else {
          SQLPlanParser.getTotalDuration(Some(metricId), appInstance)
        }
      }
    }
    durations.reduceOption(_ + _)
  }

  // The value that will be reported as ExecName in the ExecInfo object created by this parser.
  def reportedExecName: String = trimmedNodeName

  /**
   * Returns the expression string to report in ExecInfo, which shows the original platform-specific
   * operator name for plans that have been converted to OSS equivalents.
   *
   * This is primarily needed for non-OSS parsers (e.g., Photon, Auron, GPU) that convert
   * platform-specific operators to OSS Spark operators for analysis. The reported expression
   * preserves the original operator name so users can see what was actually executed.
   *
   * For example:
   * - A PhotonProject node is converted to Project for analysis, but reportedExpr shows
   *   "PhotonProject"
   * - A GpuFilter node is converted to Filter, but reportedExpr shows "GpuFilter"
   * - A NativeHashAggregate is converted to HashAggregate, but reportedExpr shows
   *   "NativeHashAggregate"
   *
   * For OSS Spark nodes (node.isOssSparkNode == true), returns empty string since no conversion
   * occurred and the exec name already represents what was executed.
   *
   * @return The original platform-specific operator name, or empty string for OSS nodes
   */
  def reportedExpr: String = {
    if (node.isOssSparkNode) {
      ""
    } else {
      node.platformName
    }
  }

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
      reportedExpr,
      speedupFactor,
      duration,
      node.id,
      isSupported,
      children = getChildren,
      unsupportedExecReason = unsupportedReason,
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
    new GenericExecParser(
      node = node,
      checker = checker,
      sqlID = sqlID,
      execName = Some(fullExecName),
      expressionFunction = expressionFunction,
      unsupportedReason = None,
      app = app)
  }
}
