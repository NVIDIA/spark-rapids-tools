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

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode

// SubQuery is simply a "collect" execution.
// It points to the actual execution. RAPIDS plugin usually skips that exec. Here we
// can represent it to be ignored with shouldRemove set to true.
// The reason we are implementing this as class is for future extensibility and to read the metrics.
case class SubqueryExecParser(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long,
    app: AppBase) extends ExecParser {
  override val fullExecName: String = node.name + "Exec"

  override def parse: ExecInfo = {
    // Note: the name of the metric may not be trailed by "(ms)" So, we only check for the prefix
    val collectTimeId =
      node.metrics.find(_.name.contains("time to collect")).map(_.accumulatorId)
    // TODO: Should we also collect the "data size" metric?
    val duration = SQLPlanParser.getDriverTotalDuration(collectTimeId, app)
    // should remove is kept in 1 place. So no need to set it here.
    ExecInfo(node, sqlID, node.name, "", 1.0, duration, node.id, isSupported = false,
      children = None, expressions = Seq.empty)
  }
}

object SubqueryExecParser extends GroupParserTrait {
  val execName = "Subquery"

  override def accepts(nodeName: String): Boolean = {
    nodeName.equals(execName)
  }

  /**
   * Create an ExecParser for the given node.
   *
   * @param node     spark plan graph node
   * @param checker  plugin type checker
   * @param sqlID    SQL ID
   * @param execName optional exec name override
   * @param opType   optional op type override
   * @param app      optional AppBase instance
   * @return an ExecParser for the given node
   */
  override def createExecParser(
      node: SparkPlanGraphNode,
      checker: PluginTypeChecker,
      sqlID: Long,
      execName: Option[String] = None,
      opType: Option[OpTypes.Value] = None,
      app: Option[AppBase]): ExecParser = {
    SubqueryExecParser(node, checker, sqlID, app.get)
  }
}
