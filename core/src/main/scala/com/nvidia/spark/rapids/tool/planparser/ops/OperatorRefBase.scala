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

import com.nvidia.spark.rapids.tool.planparser.OpTypes

import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * Base class representing a reference to an operator (either exec operator or expression operator).
 * It provides methods to retrieve the operator's name and type in both raw and
 * CSV-friendly formats.
 *
 * @param value  The name of the operator.
 * @param opType The type of the operator (e.g., Exec, Expr).
 */

class OperatorRefBase(val value: String, val opType: OpTypes.OpType) extends OperatorRefTrait {
  // Preformatted values for CSV output to avoid reformatting multiple times.
  lazy val csvValue: String = StringUtils.reformatCSVString(value)
  lazy val csvOpType: String = StringUtils.reformatCSVString(opType.toString)

  override def getOpName: String = value
  override def getOpNameCSV: String = csvValue
  override def getOpType: String = opType.toString
  override def getOpTypeCSV: String = csvOpType
}
