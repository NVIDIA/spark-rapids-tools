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
import com.nvidia.spark.rapids.tool.profiling.{FailedJobsProfileResults, JobInfoProfileResult, ProfileUtils}

import org.apache.spark.sql.rapids.tool.AppBase


trait AppJobsViewTrait extends ViewableTrait[JobInfoProfileResult] {
  override def getLabel: String = "Job Information"

  def getRawView(app: AppBase, index: Int): Seq[JobInfoProfileResult] = {
    app.jobIdToInfo.map { case (_, j) =>
      JobInfoProfileResult(index, j.jobID, j.stageIds, j.sqlID, j.startTime, j.endTime)
    }.toSeq
  }
  override def sortView(rows: Seq[JobInfoProfileResult]): Seq[JobInfoProfileResult] = {
    rows.sortBy(cols => (cols.appIndex, cols.jobID))
  }
}

trait AppFailedJobsViewTrait extends ViewableTrait[FailedJobsProfileResults] {
  override def getLabel: String = "Failed Jobs"

  def getRawView(app: AppBase, index: Int): Seq[FailedJobsProfileResults] = {
    val jobsFailed = app.jobIdToInfo.filter { case (_, jc) =>
      jc.jobResult.nonEmpty && !jc.jobResult.get.equals("JobSucceeded")
    }
    jobsFailed.map { case (id, jc) =>
      val failureStr = jc.failedReason.getOrElse("")
      FailedJobsProfileResults(index, id, jc.jobResult.getOrElse("Unknown"),
        ProfileUtils.truncateFailureStr(failureStr))
    }.toSeq
  }

  override def sortView(rows: Seq[FailedJobsProfileResults]): Seq[FailedJobsProfileResults] = {
    rows.sortBy(cols => (cols.appIndex, cols.jobId, cols.jobResult))
  }
}

object QualAppJobView extends AppJobsViewTrait with QualAppIndexMapperTrait {
 // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object ProfJobsView extends AppJobsViewTrait with ProfAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object QualAppFailedJobView extends AppFailedJobsViewTrait with QualAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object ProfFailedJobsView extends AppFailedJobsViewTrait with ProfAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}
