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

package org.apache.spark.sql.rapids.tool.util.stubs.db

import scala.reflect.runtime.universe.Mirror

import org.apache.spark.sql.execution.ui.SQLPlanMetric
import org.apache.spark.sql.rapids.tool.util.stubs.GraphReflectionEntry

case class DBGraphSQLMetricStub(m: Mirror)
  extends GraphReflectionEntry[org.apache.spark.sql.execution.ui.SQLPlanMetric](
    m, "org.apache.spark.sql.execution.ui.SQLPlanMetric") {
  // DataBricks has different constructor of the sparkPlanGraphNode
  //Array(final java.lang.String name, final long accumulatorId,
  // final java.lang.String metricType, final boolean experimental)

  // for 10.4 it is only one constructor with 3 arguments.
  // final java.lang.String name, final long accumulatorId, final java.lang.String metricType
  private val isDB104OrOlder: Boolean = constr.paramLists.flatten.size < 4

  def createInstance(name: String,
      accumulatorId: Long,
      metricType: String): SQLPlanMetric = {
    val argValues = if (isDB104OrOlder) {
      List(name, accumulatorId, metricType)
    } else {
      List(name, accumulatorId, metricType, false)
    }
    createInstanceFromList(argValues)
  }
}
