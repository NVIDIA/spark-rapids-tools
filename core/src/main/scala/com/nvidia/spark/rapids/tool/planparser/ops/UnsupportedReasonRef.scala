/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.rapids.tool.util.CachedRefBase

case class UnsupportedReasonRef(value: String) extends CachedRefBase[UnsupportedReasonRef]

object UnsupportedReasonRef {
  private val table: ConcurrentHashMap[String, UnsupportedReasonRef] =
    new ConcurrentHashMap[String, UnsupportedReasonRef]()

  // Pre-defined constants using createDefault to ensure they're in the cache
  val EMPTY_REASON: UnsupportedReasonRef =
    CachedRefBase.createDefault("Unsupported", table, UnsupportedReasonRef(_))
  val IS_UDF: UnsupportedReasonRef = getOrCreate("Is UDF")
  val CONTAINS_UDF: UnsupportedReasonRef = getOrCreate("Contains UDF")
  val IS_DATASET: UnsupportedReasonRef = getOrCreate("Is Dataset or RDD")
  val CONTAINS_DATASET: UnsupportedReasonRef = getOrCreate("Contains Dataset or RDD")
  val CONTAINS_UNSUPPORTED_EXPR: UnsupportedReasonRef = getOrCreate("Contains unsupported expr")
  val UNSUPPORTED_IO_FORMAT: UnsupportedReasonRef = getOrCreate("Unsupported IO format")
  val UNSUPPORTED_DELTA_LAKE_LOG: UnsupportedReasonRef =
    getOrCreate("Delta Lake metadata scans are not supported")
  val UNSUPPORTED_DELTA_META_QUERY: UnsupportedReasonRef =
    getOrCreate("Exec is part of Delta Lake metadata query")
  val UNSUPPORTED_COMPRESSION: UnsupportedReasonRef = getOrCreate("Unsupported compression")
  val UNSUPPORTED_CATALOG: UnsupportedReasonRef = getOrCreate("Unsupported catalog")
  val UNSUPPORTED_JOIN_TYPE: UnsupportedReasonRef = getOrCreate("Unsupported join type")

  def getOrCreate(reason: String): UnsupportedReasonRef = {
    // Make sure empty reason is always the same instance and we do not store empty values in the
    // map.
    reason match {
      case "" => EMPTY_REASON
      case _ => CachedRefBase.getOrCreate(reason, table, UnsupportedReasonRef(_))
    }
  }
}
