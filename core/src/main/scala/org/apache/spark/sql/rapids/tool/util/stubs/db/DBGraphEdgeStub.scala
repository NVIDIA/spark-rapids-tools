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

import org.apache.spark.sql.execution.ui.SparkPlanGraphEdge
import org.apache.spark.sql.rapids.tool.annotation.ToolsReflection
import org.apache.spark.sql.rapids.tool.util.stubs.GraphReflectionEntry

@ToolsReflection("DataBricks",
  "All DB accept 2 parameters. We enforce that constructor by passing (2) to the parent class")
case class DBGraphEdgeStub(m: Mirror)
  extends GraphReflectionEntry[org.apache.spark.sql.execution.ui.SparkPlanGraphEdge](
    m, "org.apache.spark.sql.execution.ui.SparkPlanGraphEdge", Option(2)) {
  // DataBricks has different constructor of the sparkPlanGraphEdge
  // (final long fromId, final long toId,
  // final scala.Option<java.lang.Object> numOutputRowsId)
  //
  // for 10.4 only one constructor with two arguments
  // final long fromId, final long toId)

  def createInstance(fromId: Long, toId: Long): SparkPlanGraphEdge = {
    val argValues = List(fromId, toId)
    createInstanceFromList(argValues)
  }
}
