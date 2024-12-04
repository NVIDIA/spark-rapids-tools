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

package com.nvidia.spark.rapids.tool.planparser.ops

import java.util.concurrent.ConcurrentHashMap

import com.nvidia.spark.rapids.tool.planparser.OpTypes


/**
 * A reference to an operator(either Exec operator or expression operator) in a Spark plan.
 *
 * @param value  The name of the operator.
 * @param opType The type of the operator (e.g., Exec, Expr).
 */
case class OpRef(override val value: String,
  override val opType: OpTypes.OpType) extends OperatorRefBase(value, opType)

object OpRef {
  // Dummy OpNameRef to represent None accumulator names. This is an optimization to avoid
  // storing an option[string] for all operator names which leads to "get-or-else" everywhere.
  private val EMPTY_OP_NAME_REF: OpRef = new OpRef("", OpTypes.Exec)
  // A global table to store reference to all operator names. The map is accessible by all
  // threads (different applications) running in parallel. This avoids duplicate work across
  // different threads.
  val OP_NAMES: ConcurrentHashMap[String, OpRef] = {
    val initMap = new ConcurrentHashMap[String, OpRef]()
    initMap.put(EMPTY_OP_NAME_REF.value, EMPTY_OP_NAME_REF)
    // Add the operator to the map because it is being used internally.
    initMap
  }

  /**
   * Retrieves an `OpRef` for an expression operator.
   * If the operator name already exists in the cache, it returns the existing `OpRef`.
   * Otherwise, it creates a new `OpRef` with the given name and `OpTypes.Expr`.
   */
  def fromExpr(name: String): OpRef = {
    OP_NAMES.computeIfAbsent(name, OpRef.apply(_, OpTypes.Expr))
  }

  /**
   * Retrieves an `OpRef` for an exec operator.
   * If the operator name already exists in the cache, it returns the existing `OpRef`.
   * Otherwise, it creates a new `OpRef` with the given name and `OpTypes.Exec`.
   */
  def fromExec(name: String): OpRef = {
    OP_NAMES.computeIfAbsent(name, OpRef.apply(_, OpTypes.Exec))
  }
}
