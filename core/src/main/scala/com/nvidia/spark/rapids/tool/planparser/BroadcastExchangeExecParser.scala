/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

case class BroadcastExchangeExecParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    override val app: Option[AppBase]
) extends GenericExecParser(
    node,
    checker,
    sqlID,
    app = app
) {
  // TODO - check the relation to see if really supported

  /** BroadcastExchange uses driver-side metrics since the broadcast happens on the driver */
  override protected def useDriverMetrics: Boolean = true

  /**
   * Broadcast exchange duration is the sum of driver-side metrics.
   * See [[GenericExecParser.durationSqlMetrics]] for details.
   *
   * Metrics summed:
   * - time to collect: time to collect data on driver
   * - time to build: time to build broadcast relation on driver
   * - time to broadcast: time to broadcast data to executors
   */
  override protected val durationSqlMetrics: Set[String] = Set(
    "time to collect",
    "time to collect (ms)",
    "time to build",
    "time to broadcast"
  )
}
