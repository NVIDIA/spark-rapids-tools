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

package org.apache.spark.sql.rapids.tool.util.stubs.bd

import scala.reflect.runtime.universe.Mirror

import org.apache.spark.sql.execution.ui.{SparkPlanGraphNode, SQLPlanMetric}
import org.apache.spark.sql.rapids.tool.annotation.ToolsReflection
import org.apache.spark.sql.rapids.tool.util.stubs.GraphReflectionEntry

case class BDGraphNodeStub(m: Mirror)
  extends GraphReflectionEntry[org.apache.spark.sql.execution.ui.SparkPlanGraphNode](
    m, "org.apache.spark.sql.execution.ui.SparkPlanGraphNode") {

  @ToolsReflection("BD-3.2.1", "Defines an extra argument planId: Int in the constructor")
  def createInstance(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    // Define argument values
    createInstanceFromList(List(id, name, desc, metrics, 0))
  }
}
