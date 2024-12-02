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

case class OperatorCounter(planInfo: PlanInfo) {
  case class OperatorData(
      opRef: OperatorRefBase,
      var count: Int = 0,
      var stages: Set[Int] = Set())

  case class OperatorCountSummary(
    opData: OperatorData,
    isSupported: Boolean)

  private val supportedMap: mutable.Map[OperatorRefBase, OperatorData] = mutable.Map()
  private val unsupportedMap: mutable.Map[OperatorRefBase, OperatorData] = mutable.Map()

  def getOpsCountSummary(): Seq[OperatorCountSummary] = {
    supportedMap.values.map(OperatorCountSummary(_, isSupported = true)).toSeq ++
      unsupportedMap.values.map(OperatorCountSummary(_, isSupported = false)).toSeq
  }

  private def updateOpRefEntry(opRef: OperatorRefBase, stages: Set[Int],
    targetMap: mutable.Map[OperatorRefBase, OperatorData]): Unit = {
    val operatorData = targetMap.getOrElseUpdate(opRef, OperatorData(opRef))
    operatorData.count += 1
    operatorData.stages ++= stages
  }

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
