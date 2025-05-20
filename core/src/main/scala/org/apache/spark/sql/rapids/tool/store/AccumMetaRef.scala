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

package org.apache.spark.sql.rapids.tool.store


/**
 * Accumulator Meta Reference
 * This maintains the reference to the metadata associated with an accumulable
 *
 * @param id - Accumulable id
 * @param name - Reference to the accumulator name
 */
case class AccumMetaRef(id: Long, name: AccumNameRef) {
  // An integer bitWise operator that represents different classification of the metric.
  // For metrics that are representing max values across agregates, it will be classified as 1;
  // otherwise 0.
  // Later, we can expand this classification to other categories if needed.
  val metricCategory: Int =
    if (AccumMetaRef.isMetricAggregateByMax(name.value)) {
      1
    } else {
      0
    }
  def isAggregateByMax: Boolean = metricCategory == 1
  def getName(): String = name.value
}

object AccumMetaRef {
  // define a list of metrics that does not aggregate by sum. Instead it aggregates as maximum.
  private val METRICS_WITH_MAX_VALUES = Set(
    "gpuMaxPageableMemoryBytes",
    "gpuMaxDeviceMemoryBytes",
    "gpuMaxHostMemoryBytes",
    "gpuMaxPinnedMemoryBytes",
    "internal.metrics.peakExecutionMemory"
  )
  val EMPTY_ACCUM_META_REF: AccumMetaRef = new AccumMetaRef(0L, AccumNameRef.EMPTY_ACC_NAME_REF)

  // Used to decide on setting the metricCategory
  private def isMetricAggregateByMax(metricName: String): Boolean = {
    METRICS_WITH_MAX_VALUES.contains(metricName)
  }

  def apply(id: Long, name: Option[String]): AccumMetaRef =
    new AccumMetaRef(id, AccumNameRef.getOrCreateAccumNameRef(name))
}
