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

package com.nvidia.spark.rapids.tool.analysis

object StageAccumDiagnosticMetrics {
  val MEMORY_SPILLED_METRIC = "internal.metrics.memoryBytesSpilled"
  val DISK_SPILLED_METRIC = "internal.metrics.diskBytesSpilled"
  val INPUT_BYTES_READ_METRIC = "internal.metrics.input.bytesRead"
  val OUTPUT_BYTES_WRITTEN_METRIC = "internal.metrics.output.bytesWritten"
  val SW_TOTAL_BYTES_METRIC = "internal.metrics.shuffle.write.bytesWritten"
  val SR_FETCH_WAIT_TIME_METRIC = "internal.metrics.shuffle.read.fetchWaitTime"
  val SW_WRITE_TIME_METRIC = "internal.metrics.shuffle.write.writeTime"
  val GPU_SEMAPHORE_WAIT_METRIC = "gpuSemaphoreWait"

  /**
   * Get all diagnostic metrics
   */
  def getAllDiagnosticMetrics: Set[String] = Set(MEMORY_SPILLED_METRIC,
    DISK_SPILLED_METRIC, INPUT_BYTES_READ_METRIC, OUTPUT_BYTES_WRITTEN_METRIC,
    SW_TOTAL_BYTES_METRIC, SR_FETCH_WAIT_TIME_METRIC, SW_WRITE_TIME_METRIC,
    GPU_SEMAPHORE_WAIT_METRIC)
}

object IOAccumDiagnosticMetrics {
  val OUTPUT_ROWS_METRIC = "output rows" // other names: join output rows, number of output rows
  val SCAN_TIME_METRIC = "scan time"
  val OUTPUT_BATCHES_METRIC = "output columnar batches" // other names: number of output batches
  val BUFFER_TIME_METRIC = "buffer time"
  val SHUFFLE_WRITE_TIME_METRIC = "shuffle write time"
  val FETCH_WAIT_TIME_METRIC = "fetch wait time"
  val GPU_DECODE_TIME_METRIC = "GPU decode time"

  /**
   * Get all IO diagnostic metrics names
   */
  def getAllIODiagnosticMetrics: Set[String] = Set(
    OUTPUT_ROWS_METRIC,
    SCAN_TIME_METRIC,
    OUTPUT_BATCHES_METRIC,
    BUFFER_TIME_METRIC,
    SHUFFLE_WRITE_TIME_METRIC,
    FETCH_WAIT_TIME_METRIC,
    GPU_DECODE_TIME_METRIC)

  /**
   * Check if a metric name belongs to IO diagnostic metrics
   */
  def isIODiagnosticMetricName(metric: String): Boolean = {
    getAllIODiagnosticMetrics.contains(metric) || metric.contains(OUTPUT_ROWS_METRIC)
  }

  /**
   * Normalize a metric name to its IO diagnostic metric constant because we want to
   * support variations in metric naming, e.g. "join output rows", "number of output rows"
   * are different names for output rows metric.
   */
  def normalizeToIODiagnosticMetric(metric: String): String = {
    if (metric.contains(OUTPUT_ROWS_METRIC)) {
      OUTPUT_ROWS_METRIC
    } else {
      metric
    }
  }
}
