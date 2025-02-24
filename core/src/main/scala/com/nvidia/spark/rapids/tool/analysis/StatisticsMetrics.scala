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

package com.nvidia.spark.rapids.tool.analysis

import org.apache.spark.sql.rapids.tool.util.InPlaceMedianArrView.{chooseMidpointPivotInPlace, findMedianInPlace}

// Store (min, median, max, total) for a given metric
case class StatisticsMetrics(min: Long, med: Long, max: Long, count: Long, total: Long)

object StatisticsMetrics {
  // a static variable used to represent zero-statistics instead of allocating a dummy record
  // on every calculation.
  val ZERO_RECORD: StatisticsMetrics = StatisticsMetrics(0L, 0L, 0L, 0L, 0L)

  def createFromArr(arr: Array[Long]): StatisticsMetrics = {
    if (arr.isEmpty) {
      return ZERO_RECORD
    }
    val medV = findMedianInPlace(arr)(chooseMidpointPivotInPlace)
    var minV = Long.MaxValue
    var maxV = Long.MinValue
    var totalV = 0L
    arr.foreach { v =>
      if (v < minV) {
        minV = v
      }
      if (v > maxV) {
        maxV = v
      }
      totalV += v
    }
    StatisticsMetrics(minV, medV, maxV, arr.length, totalV)
  }

  def createOptionalFromArr(arr: Array[Long]): Option[StatisticsMetrics] = {
    if (arr.isEmpty) {
      return None
    }
    Some(createFromArr(arr))
  }
}
