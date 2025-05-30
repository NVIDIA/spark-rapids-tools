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

import com.nvidia.spark.rapids.tool.qualification.{QualificationArgs, QualificationMain, QualToolResult}

/**
 * Context to run the qualification tool and run checkers on the result.
 *
 * @param eventLogs the event logs to pass to the tool
 * @param outputDirectory the output directory of the tool
 * @param toolArgs additional args to pass to the tool
 * @param actualQualRes the result of the tool, available after calling [[fire]]
 */
case class QToolTestCtxt(
    var eventLogs: Array[String],
    var outputDirectory: String,
    var toolArgs: Array[String] = Array.empty,
    var actualQualRes: Option[QualToolResult] = Option.empty[QualToolResult]) {

  /**
   * Build the arguments to pass to the tool
   *
   * @return the arguments
   */
  def buildArgs(): Array[String] = {
    toolArgs ++ Array(
      "--output-directory",
      outputDirectory) ++ eventLogs
  }

  /**
   * Run the tool and run the checkers on the result
   *
   * @param checkers the checkers to run
   * @return this
   */
  def fire(checkers: Seq[QToolResultCheckerTrait]): QToolTestCtxt = {
    val appArgs = new QualificationArgs(buildArgs())
    actualQualRes = Option(QualificationMain.mainInternal(appArgs))
    checkers.foreach { checker =>
      checker.applyCheck(this)
    }
    this
  }
}
