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

package com.nvidia.spark.rapids.tool.planparser

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.AppBase

trait GroupParserTrait {
  /**
   * Checks whether this parserGroup should handle the given node name.
   * @param nodeName name of the node to check
   * @return true if this parserGroup can handle the node, false otherwise
   */
  def accepts(nodeName: String): Boolean = false

  /**
   * Create an ExecParser for the given node.
   * @param node spark plan graph node
   * @param checker plugin type checker
   * @param sqlID SQL ID
   * @param execName optional exec name override
   * @param opType optional op type override
   * @param app optional AppBase instance
   * @return an ExecParser for the given node
   */
  def createExecParser(
      node: SparkPlanGraphNode,
      checker: PluginTypeChecker,
      sqlID: Long,
      execName: Option[String] = None,
      opType: Option[OpTypes.Value] = None,
      app: Option[AppBase] = None): ExecParser
}
