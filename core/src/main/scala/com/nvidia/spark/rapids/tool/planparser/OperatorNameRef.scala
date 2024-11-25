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

/**
 * Represents a reference to an execution node in a Spark query plan.
 * This class provides a way to uniquely identify and store execution node references
 *
 * @param value The string value representing the execution node
 */
case class ExecRef(value: String) {
  val csvValue: String = StringUtils.reformatCSVString(value)
}

/**
 * Companion object for ExecRef, providing a caching mechanism to reuse instances.
 */
object ExecRef {
  private val namesTable: ConcurrentHashMap[String, ExecRef] = new ConcurrentHashMap()

  /**
   * Retrieves an existing ExecRef instance with the given name,
   * or creates a new one if not present.
   *
   * @param name The name of the execution operator.
   * @return An ExecRef instance corresponding to the given name.
   */
  def getOrCreate(name: String): ExecRef = {
    namesTable.computeIfAbsent(name, ExecRef.apply)
  }
  val EMPTY: ExecRef = getOrCreate("")
}

/**
 * Represents a reference to an expression in a Spark query plan.
 * Similar to ExecRef, but specifically for expressions rather than execution nodes.
 *
 * @param value The string value representing the expression
 */
case class ExprRef(value: String) {
  val csvValue: String = StringUtils.reformatCSVString(value)
}

/**
 * Companion object for ExprRef, providing a caching mechanism to reuse instances.
 */
object ExprRef {
  private val namesTable: ConcurrentHashMap[String, ExprRef] = new ConcurrentHashMap()

  /**
   * Retrieves an existing ExprRef instance with the given name,
   * or creates a new one if not present.
   *
   * @param name The name of the expression.
   * @return An ExprRef instance corresponding to the given name.
   */
  def getOrCreate(name: String): ExprRef = {
    namesTable.computeIfAbsent(name, ExprRef.apply)
  }

  val EMPTY: ExprRef = getOrCreate("")
}
