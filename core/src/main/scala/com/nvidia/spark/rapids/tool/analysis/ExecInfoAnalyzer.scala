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

import com.nvidia.spark.rapids.tool.planparser.{ExecInfo, ExecRef, ExprRef, OpTypes, PlanInfo}

case class ExecInfoAnalyzer(planInfo: PlanInfo) {

  // Internal case classes for uniquely identifying execs and expressions
  private case class ExecKey(nameRef: ExecRef, opType: OpTypes.OpType, isSupported: Boolean)
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

  private val execData: mutable.Map[Long, mutable.Map[ExecKey, ExecData]] = mutable.Map()

  private val expressionData: mutable.Map[Long,
      mutable.Map[ExpressionKey, ExpressionData]] = mutable.Map()

  def analyze(): Unit = {
    planInfo.execInfo.foreach(traverse)
  }

  /**
   * Recursively traverses the execution tree to collect exec and expression statistics
   *
   * @param execInfo The execution information node to process
   */
  private def traverse(execInfo: ExecInfo): Unit = {
    val sqlID = execInfo.sqlID
    val operatorName = execInfo.execRef.value

    // Check if the operator name is non-empty
    if (operatorName.nonEmpty) {
      val execKey = ExecKey(execInfo.execRef, execInfo.opType, execInfo.isSupported)
      val sqlMap = execData.getOrElseUpdate(sqlID, mutable.Map())

      val operatorData =
        sqlMap.getOrElseUpdate(execKey, (ExecData()))
      operatorData.count += 1
      operatorData.stages ++= execInfo.stages

      execInfo.exprsRef.foreach { exprRef =>
        val exprName = exprRef.value
        // Check if the expression name is non-empty
        if (exprName.nonEmpty) {
          val exprKey = ExpressionKey(exprRef, OpTypes.Expr, execInfo.isSupported)
          val exprSqlMap = expressionData.getOrElseUpdate(sqlID, mutable.Map())
          val exprData = exprSqlMap.getOrElseUpdate(exprKey, ExpressionData())
          exprData.count += 1
          exprData.stages ++= execInfo.stages
        }
      }
    }
    execInfo.children.foreach(_.foreach(traverse))
  }

  /**
   * Represents the aggregated analysis result of an expression within an execution
   * plan per SQL ID.
   *
   * This case class is used to store metadata about expressions encountered during the analysis
   * of execution plans. It holds information such as the expression reference, operation type,
   * support status, occurrence count, and the stages where the expression appears. This
   * data is per SQL ID.
   *
   * This is used within ExecResult to encapsulate expression-specific data
   */
  case class ExpressionResult(
      sqlID: Long,
      exprRef: ExprRef,
      opType: OpTypes.OpType,
      isSupported: Boolean,
      count: Int,
      stages: Set[Int]
  )

  /**
   * Represents the aggregated analysis result of an Exec operator within an execution plan.
   *
   * This case class is used to store metadata about operators encountered during the analysis
   * of execution plans. It includes the sqlID, operator reference, operation type, support status,
   * occurrence count, stages where the operator is used,
   * and a sequence of associated expressions.
   *
   * Used in analysis reports to summarize execution nodes, including their expressions.
   */
  case class ExecResult(
      sqlID: Long,
      execRef: ExecRef,
      opType: OpTypes.OpType,
      isSupported: Boolean,
      count: Int,
      stages: Set[Int]
  )

  def getResults: (Seq[ExecResult], Seq[ExpressionResult]) = {
    val execResults = execData.flatMap { case (sqlID, operatorMap) =>
      operatorMap.map { case (operatorKey, operatorData) =>
        ExecResult(
          sqlID = sqlID,
          execRef = operatorKey.nameRef,
          opType = operatorKey.opType,
          isSupported = operatorKey.isSupported,
          count = operatorData.count,
          stages = operatorData.stages
        )
      }
    }.toSeq

    val exprResults = expressionData.flatMap { case (sqlID, expressionMap) =>
      expressionMap.map { case (exprKey, exprDataEntry) =>
        ExpressionResult(
          sqlID = sqlID,
          exprRef = exprKey.nameRef,
          opType = exprKey.opType,
          isSupported = exprKey.isSupported,
          count = exprDataEntry.count,
          stages = exprDataEntry.stages
        )
      }
    }.toSeq

    (execResults, exprResults)
  }
}
