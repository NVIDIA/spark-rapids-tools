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

package com.nvidia.spark.rapids.tool.planparser

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.rapids.tool.util.StringUtils

case class ExecRef(value: String) {
  val csvValue: String = StringUtils.reformatCSVString(value)
}

object ExecRef {
  private val namesTable: ConcurrentHashMap[String, ExecRef] = new ConcurrentHashMap()

  def getOrCreate(name: String): ExecRef = {
    namesTable.computeIfAbsent(name, ExecRef.apply)
  }
  val Empty: ExecRef = getOrCreate("")
}

case class ExprRef(value: String) {
  val csvValue: String = StringUtils.reformatCSVString(value)
}

object ExprRef {
  private val namesTable: ConcurrentHashMap[String, ExprRef] = new ConcurrentHashMap()

  def getOrCreate(name: String): ExprRef = {
    namesTable.computeIfAbsent(name, ExprRef.apply)
  }

  val Empty: ExprRef = getOrCreate("")
}
