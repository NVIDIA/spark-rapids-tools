/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

import org.apache.spark.sql.rapids.tool.UnsupportedExpr

trait ExecParser {
  def parse: ExecInfo
  val fullExecName: String

  /**
   * Returns a sequence of UnsupportedExpr for given expressions if they are not supported
   * by the exec node.
   * This default implementation assumes all expressions are supported and returns an empty
   * sequence. Specific Exec parsers should override this method to provide the list of
   * unsupported expressions if required.
   *
   * @param expressions Array of expression strings to evaluate for support.
   * @return Empty Seq[UnsupportedExpr], indicating no unsupported expressions by default.
   */
  def getUnsupportedExprReasonsForExec(expressions: Array[String]): Seq[UnsupportedExpr] = Seq.empty
}
