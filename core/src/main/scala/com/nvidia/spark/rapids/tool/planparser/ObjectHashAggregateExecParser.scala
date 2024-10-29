/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.AppBase

case class ObjectHashAggregateExecParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    appParam: AppBase) extends
  GenericExecParser(node, checker, sqlID) with Logging {
  override def computeDuration: Option[Long] = {
    val accumId = node.metrics.find(_.name == "time in aggregation build").map(_.accumulatorId)
    SQLPlanParser.getTotalDuration(accumId, appParam)
  }
}
