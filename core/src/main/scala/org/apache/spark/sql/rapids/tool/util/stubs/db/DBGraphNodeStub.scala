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

import org.apache.spark.sql.execution.ui.{SparkPlanGraphNode, SQLPlanMetric}
import org.apache.spark.sql.rapids.tool.util.stubs.GraphReflectionEntry

case class DBGraphNodeStub(m: Mirror)
  extends GraphReflectionEntry[org.apache.spark.sql.execution.ui.SparkPlanGraphNode](
    m, "org.apache.spark.sql.execution.ui.SparkPlanGraphNode") {
  // DataBricks has different constructor of the sparkPlanGraphNode
  // [(long,java.lang.String,java.lang.String,scala.collection.Seq,java.lang.String,
  // boolean,scala.Option,scala.Option)] and
  // [final long id, final java.lang.String name, final java.lang.String desc,
  // final scala.collection.Seq<org.apache.spark.sql.execution.ui.SQLPlanMetric> metrics,
  // final java.lang.String rddScopeId, final boolean started,
  // final scala.Option<scala.math.BigInt> estRowCount)

  // For 10.4 --> only 1 constructor and has 6 arguments (not 7)
  // (final long id, final java.lang.String name, final java.lang.String desc,
  // final scala.collection.Seq<org.apache.spark.sql.execution.ui.SQLPlanMetric> metrics,
  // final java.lang.String rddScopeId, final scala.Option<scala.math.BigInt> estRowCount

  // DB10.4 has constructor with 6 arguments.
  private val isDB104OrOlder: Boolean = constr.paramLists.flatten.size < 7

  def createInstance(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    // Define argument values
    val argValues = if (isDB104OrOlder) {
      List(id, name, desc, metrics, "", None)
    } else {
      List(id, name, desc, metrics, "", false, None, None)
    }
    createInstanceFromList(argValues)
  }
}
