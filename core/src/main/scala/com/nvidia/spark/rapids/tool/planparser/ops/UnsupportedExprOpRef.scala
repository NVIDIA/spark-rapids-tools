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


/**
 * Represents a reference to an unsupported expression operator.
 * Extends `OperatorRefBase` and includes a reason why the expression is unsupported.
 *
 * @param opRef             The underlying `OpRef` for the expression.
 * @param unsupportedReason A string describing why the expression is unsupported.
 */
case class UnsupportedExprOpRef(opRef: OpRef,
    unsupportedReason: String) extends OperatorRefBase(opRef.value, opRef.opType)

// Provides a factory method to create an instance from an expression name and unsupported reason.
object UnsupportedExprOpRef {
  /**
   * Creates an `UnsupportedExprOpRef` for the given expression name and unsupported reason.
   *
   * @param exprName          The name of the unsupported expression.
   * @param unsupportedReason A string describing why the expression is unsupported.
   * @return An instance of `UnsupportedExprOpRef`.
   */
  def apply(exprName: String, unsupportedReason: String): UnsupportedExprOpRef = {
    val opRef = OpRef.fromExpr(exprName)
    UnsupportedExprOpRef(opRef, unsupportedReason)
  }
}
