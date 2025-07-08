/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.qualification.checkers

import scala.util.{Success, Try}

import com.nvidia.spark.rapids.tool.qualification.{QualToolResult, QualToolResultBuilder}

import org.apache.spark.internal.Logging

/**
 * Base trait for all result checkers.
 */
trait QToolResultCheckerTrait extends Logging {
  // Apply the checker when those conditions are satisfied.
  var conditions: Seq[() => (Boolean, String)] = Seq.empty

  // Add runCondition to the checker
  def withRunCondition(runCondition: () => (Boolean, String)): QToolResultCheckerTrait = {
    conditions :+= runCondition
    this
  }

  /**
   * Description of the checker.
   *
   * @return description string.
   */
  def description: String

  /**
   * The actual result of the qualification tool.
   *
   * @return the actual result.
   */
  private var _qRes: QualToolResult = QualToolResultBuilder.emptyResult()

  def qRes: QualToolResult = _qRes

  /**
   * Set the actual result of the qualification tool.
   *
   * @param value the actual result.
   */
  def qRes_=(value: QualToolResult): Unit = {
    _qRes = value
  }

  /**
   * Run the checker with the given context.
   *
   * @param qTestCtxt the test context.
   */
  def doFire(qTestCtxt: QToolTestCtxt): Unit = {
    qRes = qTestCtxt.actualQualRes.get
  }

  def applyCheck(qTestCtxt: QToolTestCtxt): Unit = {
    var checkApplicable = true
    conditions.foreach { condition =>
      val (isAllowed, message) = condition()
      Try(assume(isAllowed)) match {
        case Success(_) =>
          // do Nothing
          logDebug(s"applying this checker: $message")
        case _ =>
          // do not fire the check and log a message if necessary.
          logDebug(s"Skipping this checker: $message")
          checkApplicable = false
      }
    }
    if (checkApplicable) {
      doFire(qTestCtxt)
    }
  }
  def build(): QToolResultCheckerTrait
}

case class QToolResBlockChecker(
    description: String,
    doFire: QualToolResult => Unit)
