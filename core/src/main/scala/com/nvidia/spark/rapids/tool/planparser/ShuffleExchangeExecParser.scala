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

import java.util.concurrent.TimeUnit.NANOSECONDS

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode

case class ShuffleExchangeExecParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    override val app: Option[AppBase]
) extends GenericExecParser(
    node,
    checker,
    sqlID,
    execName = Option("ShuffleExchangeExec"),
    app = app
) {

  /**
   * Compute duration by summing:
   * - shuffle write time (in nanoseconds, needs conversion)
   * - fetch wait time (in milliseconds)
   */
  override protected def computeDuration: Option[Long] = {
    app.flatMap { appObj =>
      val writeId = node.metrics.find(_.name == "shuffle write time").map(_.accumulatorId)
      // shuffle write time is in nanoseconds, convert to milliseconds
      val maxWriteTime = SQLPlanParser.getTotalDuration(writeId, appObj).map(NANOSECONDS.toMillis)
      val fetchId = node.metrics.find(_.name == "fetch wait time").map(_.accumulatorId)
      val maxFetchTime = SQLPlanParser.getTotalDuration(fetchId, appObj)
      (maxWriteTime ++ maxFetchTime).reduceOption(_ + _)
    }
  }
}
