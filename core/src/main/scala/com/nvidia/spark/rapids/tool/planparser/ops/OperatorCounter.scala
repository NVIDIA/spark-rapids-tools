/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser.ops

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.planparser.{ExecInfo, PlanInfo}

/**
 * `OperatorCounter` is responsible for counting the occurrences of execs and expressions
 * in a given execution plan (`PlanInfo`). It maintains counts separately for supported and
 * unsupported execs and expressions.
 *
 * @param planInfo The execution plan information to analyze.
 */
case class OperatorCounter(planInfo: PlanInfo) {

  /**
   * Represents data for an exec or expression, including its reference,
   * occurrence count, and stages where it appears.
   *
   * @param opRef  The operator reference.
   * @param count  The number of times the operator appears.
   * @param stages The set of stages where the operator appears.
   */
  case class OperatorData(
      opRef: OperatorRefBase,
      var count: Int = 0,
      var stages: Set[Int] = Set())

  // Summarizes the count information for an exec or expression, including whether it is supported.
  case class OperatorCountSummary(
    opData: OperatorData,
    isSupported: Boolean)

  private val supportedMap: mutable.Map[OperatorRefBase, OperatorData] = mutable.Map()
  private val unsupportedMap: mutable.Map[OperatorRefBase, OperatorData] = mutable.Map()

  // Returns a sequence of `OperatorCountSummary`, combining both supported and
  // unsupported operators.
  def getOpsCountSummary(): Seq[OperatorCountSummary] = {
    supportedMap.values.map(OperatorCountSummary(_, isSupported = true)).toSeq ++
      unsupportedMap.values.map(OperatorCountSummary(_, isSupported = false)).toSeq
  }


  // Updates the operator data in the given map (supported or unsupported).
  // Increments the count and updates the stages where the operator appears.
  private def updateOpRefEntry(opRef: OperatorRefBase, stages: Set[Int],
    targetMap: mutable.Map[OperatorRefBase, OperatorData]): Unit = {
    val operatorData = targetMap.getOrElseUpdate(opRef, OperatorData(opRef))
    operatorData.count += 1
    operatorData.stages ++= stages
  }

  // Processes an `ExecInfo` node to update exec and expression counts.
  // Separates supported and unsupported execs and expressions into their respective maps.
  private def processExecInfo(execInfo: ExecInfo): Unit = {
    val opMap = execInfo.isSupported match {
      case true => supportedMap
      case false => unsupportedMap
    }
    updateOpRefEntry(execInfo.execRef, execInfo.stages, opMap)
    // update the map for supported expressions. We should exclude the unsupported expressions.
    execInfo.expressions.filterNot(
      e => execInfo.unsupportedExprs.exists(exp => exp.opRef.equals(e))).foreach { expr =>
      updateOpRefEntry(expr, execInfo.stages, supportedMap)
    }
    // update the map for unsupported expressions
    execInfo.unsupportedExprs.foreach { expr =>
      updateOpRefEntry(expr, execInfo.stages, unsupportedMap)
    }
  }

  // Counts the execs and expressions in the execution plan.
  private def countOperators(): Unit = {
    planInfo.execInfo.foreach { exec =>
      exec.isClusterNode match {
        // we do not want to count the cluster nodes in that aggregation
        case true =>
          if (exec.children.nonEmpty) {
            exec.children.get.foreach { child =>
              processExecInfo(child)
            }
          }
        case false => processExecInfo(exec)
      }
    }
  }

  countOperators()
}
