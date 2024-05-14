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

package com.nvidia.spark.rapids.tool.views

import com.nvidia.spark.rapids.tool.profiling.{IOAnalysisProfileResult, JobStageAggTaskMetricsProfileResult, ShuffleSkewProfileResult, SQLDurationExecutorTimeProfileResult, SQLTaskAggMetricsProfileResult}

/**
 * Contains the sort logic for the aggregated Spark RawMetrics.
 * Note: This implementation needs to be cleaner and to allow for different sort strategies to
 * be used by overriding the sorting methods.
 * Having that in one place has the following pros:
 * 1- makes the code easier to read, because otherwise the sorting code was spread everywhere in
 *    the aggregation methods.
 * 2- Easier to maintain the code and extend it to implement different sort strategies for different
 *    tools/reports. For example, the qualification tool can define object that executes a different
 *    sorting logic compared to the profiler.
 */
object AggMetricsResultSorter {
  // TODO: The implementation needs to be cleaner to allow for different sort strategies.
  //      Also, it will be better to use some generics to allow for different argument types.
  def sortJobSparkMetrics(
      rows: Seq[JobStageAggTaskMetricsProfileResult]): Seq[JobStageAggTaskMetricsProfileResult] = {
    if (rows.isEmpty) {
      Seq.empty
    } else {
      rows.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, -sortDur, cols.id)
      }
    }
  }

  def sortSqlAgg(
      rows: Seq[SQLTaskAggMetricsProfileResult]): Seq[SQLTaskAggMetricsProfileResult] = {
    if (rows.isEmpty) {
      Seq.empty
    } else {
      rows.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, -sortDur, cols.sqlId, cols.executorCpuTime)
      }
    }
  }

  def sortSqlDurationAgg(
      rows: Seq[SQLDurationExecutorTimeProfileResult]):
  Seq[SQLDurationExecutorTimeProfileResult] = {
    if (rows.isEmpty) {
      Seq.empty
    } else {
      rows.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, cols.sqlID, sortDur)
      }
    }
  }

  def sortShuffleSkew(
      rows: Seq[ShuffleSkewProfileResult]):
  Seq[ShuffleSkewProfileResult] = {
    if (rows.isEmpty) {
      Seq.empty
    } else {
      rows.sortBy { cols =>
        (cols.appIndex, cols.stageId, cols.stageAttemptId, cols.taskId, cols.taskAttemptId)
      }
    }
  }

  def sortIO(
      rows: Seq[IOAnalysisProfileResult]):
  Seq[IOAnalysisProfileResult] = {
    if (rows.isEmpty) {
      Seq.empty
    } else {
      rows.sortBy { cols =>
        (cols.appIndex, cols.sqlId)
      }
    }
  }
}
