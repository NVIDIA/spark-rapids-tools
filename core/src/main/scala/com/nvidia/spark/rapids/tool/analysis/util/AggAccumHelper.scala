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

import com.nvidia.spark.rapids.tool.profiling.StageAggTaskMetricsProfileResult

import org.apache.spark.sql.rapids.tool.store.TaskModel

/**
 * A helper class to facilitate the accumulation of aggregate metrics.
 * This is a separate class to allow further customization in the future. For example,
 * a parellel processor can be used to split the iterables without changing the caller side.
 */
class AggAccumHelper {

  private def accumCachedRecords[R <: TaskMetricsAccumRec](
      stageRecords: Iterable[StageAggTaskMetricsProfileResult],
      rec: R): Unit = {
    stageRecords.foreach(rec.addRecord)
    rec.finalizeAggregation()
  }

  protected def createStageAccumRecord(): TaskMetricsAccumRec = {
    StageAggAccum()
  }

  def accumPerStage(taskRecords: Iterable[TaskModel]): TaskMetricsAccumRec = {
    val resRec = createStageAccumRecord()
    taskRecords.foreach(resRec.addRecord)
    resRec.finalizeAggregation()
    resRec
  }

  def accumPerSQL(stageRecords: Iterable[StageAggTaskMetricsProfileResult]): SQLAggAccum = {
    val resRec = SQLAggAccum()
    accumCachedRecords(stageRecords, resRec)
    resRec
  }

  def accumPerJob(stageRecords: Iterable[StageAggTaskMetricsProfileResult]): JobAggAccum = {
    val resRec = JobAggAccum()
    accumCachedRecords(stageRecords, resRec)
    resRec
  }
}
