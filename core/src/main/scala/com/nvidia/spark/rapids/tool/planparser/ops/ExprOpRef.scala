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

package com.nvidia.spark.rapids.tool.planparser.ops

/**
 * Represents a reference to an expression operator that is stored in the ExecInfo expressions
 * @param opRef the opRef to wrap
 * @param count the count of that expression within the exec.
 */
case class ExprOpRef(opRef: OpRef, count: Int = 1) extends OpRefWrapperBase(opRef)

object ExprOpRef extends OpRefWrapperBaseTrait[ExprOpRef] {
  def fromRawExprSeq(exprArr: Seq[String]): Seq[ExprOpRef] = {
    exprArr.groupBy(identity)
      .iterator
      .map { case (k, expr) => k -> ExprOpRef(OpRef.fromExpr(expr.head), expr.size) }
      .toMap
      .values
      .toSeq
  }
}
