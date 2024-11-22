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

package com.nvidia.spark.rapids.tool.analysis

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.planparser.{ExecInfo, ExecRef, ExprRef, OpTypes}

case class ExecInfoAnalyzer(execInfos: Seq[ExecInfo]) {

  // Internal case classes for uniquely identifying execs and expressions
  private case class OperatorKey(nameRef: ExecRef, opType: OpTypes.OpType, isSupported: Boolean)
  private case class ExpressionKey(nameRef: ExprRef, opType: OpTypes.OpType, isSupported: Boolean)

  /**
   * Holds aggregated data for an exec.
   *
   * @param count  Number of times the exec occurs.
   * @param stages Set of stages where the exec is found.
   */
  case class ExecData(
      var count: Int = 0,
      var stages: Set[Int] = Set()
  )

  /**
   * Holds aggregated data for an expression.
   *
   * @param count  Number of times the expression occurs.
   * @param stages Set of stages where the expression is found.
   */
  case class ExpressionData(
      var count: Int = 0,
      var stages: Set[Int] = Set()
  )

  // Internal data structure for aggregating execs and expressions by SQL ID.
  // The structure is a Map from SQL ID to a Map of OperatorKey to a tuple containing
  // ExecData and a Map of ExpressionKey to ExpressionData.
  private val aggregatedData: mutable.Map[Long, mutable.Map[OperatorKey,
      (ExecData, mutable.Map[ExpressionKey, ExpressionData])]] = mutable.Map()

  def analyze(): Unit = {
    execInfos.foreach(traverse)
  }

  /**
   * Recursively traverses the execution tree to collect exec and expression statistics
   *
   * @param execInfo The execution information node to process
   */
  private def traverse(execInfo: ExecInfo): Unit = {
    val sqlID = execInfo.sqlID
    val operatorName = execInfo.execsRef.value

    // Check if the operator name is non-empty
    if (operatorName.nonEmpty) {
      val operatorKey = OperatorKey(execInfo.execsRef, execInfo.opType, execInfo.isSupported)
      val sqlMap = aggregatedData.getOrElseUpdate(sqlID, mutable.Map())

      val (operatorData, exprDataMap) =
        sqlMap.getOrElseUpdate(operatorKey, (ExecData(), mutable.Map()))
      operatorData.count += 1
      operatorData.stages ++= execInfo.stages

      execInfo.exprsRef.foreach { exprRef =>
        val exprName = exprRef.value
        // Check if the expression name is non-empty
        if (exprName.nonEmpty) {
          val exprKey = ExpressionKey(exprRef, OpTypes.Expr, execInfo.isSupported)
          val exprData = exprDataMap.getOrElseUpdate(exprKey, ExpressionData())
          exprData.count += 1
          exprData.stages ++= execInfo.stages
        }
      }
    }
    execInfo.children.foreach(_.foreach(traverse))
  }

  case class ExpressionResult(
      exprRef: ExprRef,
      opType: OpTypes.OpType,
      isSupported: Boolean,
      count: Int,
      stages: Set[Int]
  )

  case class ExecResult(
      execRef: ExecRef,
      opType: OpTypes.OpType,
      isSupported: Boolean,
      count: Int,
      stages: Set[Int],
      expressions: Seq[ExpressionResult]
  )

  case class AllOperatorsResult(
      sqlID: Long,
      operators: Seq[ExecResult]
  )

  def getResults: Seq[AllOperatorsResult] = {
    aggregatedData.map { case (sqlID, operatorMap) =>
      val operatorResults = operatorMap.map { case (operatorKey, (operatorData, exprDataMap)) =>
        val expressionResults = exprDataMap.map { case (exprKey, exprData) =>
          ExpressionResult(
            exprRef = exprKey.nameRef,
            opType = exprKey.opType,
            isSupported = exprKey.isSupported,
            count = exprData.count,
            stages = exprData.stages
          )
        }.toSeq
        ExecResult(
          execRef = operatorKey.nameRef,
          opType = operatorKey.opType,
          isSupported = operatorKey.isSupported,
          count = operatorData.count,
          stages = operatorData.stages,
          expressions = expressionResults
        )
      }.toSeq
      AllOperatorsResult(sqlID, operatorResults)
    }.toSeq
  }
}
