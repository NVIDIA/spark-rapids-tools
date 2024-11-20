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

package com.nvidia.spark.rapids.tool.planparser

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode

case class WriteFilesExecParser(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long) extends ExecParser {
  // WriteFiles was added in Spark3.4+.
  // The purpose was to create that operator to contain information from V1 write operators.
  // Basically, it is supported IFF its child is supported. The GPU plan will fallBack to the CPU
  // if the child is not supported.
  // For Q tool, we will treat the WriteFilesExec as supported regardless of the child.
  // Then the child is evaluated on its own . This results in the WriteFilesExec being incorrectly
  // marked as supported, but the error is should not a big deal since the operator has no
  // duration associated with it.
  override val fullExecName: String = WriteFilesExecParser.execName + "Exec"
  val execNameRef = ExecRef.getOrCreate(fullExecName)

  override def parse: ExecInfo = {
    // the WriteFiles does not have duration
    val duration = None
    val speedupFactor = checker.getSpeedupFactor(fullExecName)
    ExecInfo.createExecNoNode(
      sqlID,
      WriteFilesExecParser.execName,
      "",
      speedupFactor,
      duration,
      node.id, opType = OpTypes.WriteExec, true, None, execsRef = execNameRef)
  }
}

object WriteFilesExecParser {
  val execName = "WriteFiles"
  def accepts(nodeName: String): Boolean = {
    nodeName.contains(execName)
  }
}
