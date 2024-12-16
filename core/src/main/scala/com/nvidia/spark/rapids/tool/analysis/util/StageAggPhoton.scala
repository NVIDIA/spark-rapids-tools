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

package com.nvidia.spark.rapids.tool.analysis.util

import java.util.concurrent.TimeUnit

import com.nvidia.spark.rapids.tool.profiling.StageAggTaskMetricsProfileResult

/**
 * Implementation of Accumulator object for Photon.
 * It takes the shuffleWriteValues and peakMemValues Accumulables as an argument because those
 * values are not available in the TaskModel.
 */
case class StageAggPhoton(
    shuffleWriteValues: Iterable[Long],
    peakMemValues: Iterable[Long]) extends TaskMetricsAccumRec {

  override def addRecord(rec: StageAggTaskMetricsProfileResult): Unit = {
    throw new UnsupportedOperationException("Not implemented: Cannot use cached results to" +
      "calculate stage aggregates")
  }

  override def finalizeAggregation(): Unit = {
    // Fix the shuffleWriteTimes and the peakMemoryValues to use the shuffleWriteValues and
    // the peakMemValues.
    swWriteTimeSum = 0
    peakExecutionMemoryMax = 0
    if (shuffleWriteValues.nonEmpty) {
      swWriteTimeSum = TimeUnit.NANOSECONDS.toMillis(shuffleWriteValues.sum)
    }
    if (peakMemValues.nonEmpty) {
      peakExecutionMemoryMax = peakMemValues.max
    }
    super.finalizeAggregation()
  }
}
