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
 * An instance that wraps OpRef and exposes the OpRef methods.
 * This is used to provide common interface for all classes that wrap OpRef along with other
 * metadata.
 * @param opRef the opRef to wrap
 */
class OpRefWrapperBase(opRef: OpRef) extends OperatorRefTrait {
  override def getOpName: String = opRef.getOpName

  override def getOpNameCSV: String = opRef.getOpNameCSV

  override def getOpType: String = opRef.getOpType

  override def getOpTypeCSV: String = opRef.getOpTypeCSV

  def getOpRef: OpRef = opRef
}

/**
 * A trait that provides a factory method to create instances of OpRefWrapperBase from a sequence of
 * @tparam R the type of the OpRefWrapperBase
 */
trait OpRefWrapperBaseTrait[R <: OpRefWrapperBase] {
  /**
   * Create instances of OpRefWrapperBase from a sequence of expressions.
   * The expressions are grouped by their value and the count of each expression is stored in the
   * OpRefWrapperBase entry.
   * @param exprArr the sequence of expressions
   * @return a sequence of OpRefWrapperBase instances
   */
  def fromRawExprSeq(exprArr: Seq[String]): Seq[R]
}
