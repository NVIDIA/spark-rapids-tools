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

import com.nvidia.spark.rapids.tool.analysis.{ProfAppIndexMapperTrait, QualAppIndexMapperTrait}
import com.nvidia.spark.rapids.tool.profiling.{FailedTaskProfileResults, ProfileUtils}

import org.apache.spark.sql.rapids.tool.AppBase


trait AppFailedTaskViewTrait extends ViewableTrait[FailedTaskProfileResults] {
  override def getLabel: String = "Failed Tasks"

  override def getRawView(app: AppBase, index: Int): Seq[FailedTaskProfileResults] = {
    app.taskManager.getAllFailedTasks.map { t =>
      FailedTaskProfileResults(index, t.stageId, t.stageAttemptId,
        t.taskId, t.attempt, ProfileUtils.truncateFailureStr(t.endReason))
    }.toSeq
  }

  override def sortView(
      rows: Seq[FailedTaskProfileResults]): Seq[FailedTaskProfileResults] = {
    rows.sortBy(
      cols => (cols.appIndex, cols.stageId, cols.stageAttemptId, cols.taskId, cols.taskAttemptId))
  }
}

object QualFailedTaskView extends AppFailedTaskViewTrait with QualAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object ProfFailedTaskView extends AppFailedTaskViewTrait with ProfAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

