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

/**
 * A stub class to hold the static information about a Delta Lake operator.
 *
 * @param nodeName       the exact name of the SparkPlanGraphNode
 * @param isSupported    whether this operator is supported in the plugin
 * @param opType         the type of the operator, default to OpTypes.Exec
 * @param sqlMetricNames the set of SQL metric names to be used to compute the duration
 */
case class SupportedOpStub(
  nodeName: String,
  isSupported: Boolean = true,
  opType: Option[OpTypes.Value] = Option(OpTypes.Exec),
  sqlMetricNames: Set[String] = Set.empty) {

  // The name without the "Execute " prefix if any.
  val execNoPrefix: String = if (nodeName.startsWith("Execute ")) {
    nodeName.stripPrefix("Execute ")
  } else {
    nodeName
  }

  // The ID used by the plugin. This is useful to match with the PluginTypeChecker.
  val execID: String = {
    execNoPrefix + "Exec"
  }

  // Get the OpType, default to OpTypes.Exec
  def pullOpType: OpTypes.Value = {
    opType match {
      case Some(v) => v
      case None => OpTypes.Exec
    }
  }
}
