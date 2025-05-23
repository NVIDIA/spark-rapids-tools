/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.profiling

import com.nvidia.spark.rapids.tool.views.{ProfFailedJobsView, ProfFailedStageView, ProfFailedTaskView, ProfRemovedBLKMgrView, ProfRemovedExecutorView}

import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * HealthCheck defined health check rules
 */
class HealthCheck(apps: Seq[ApplicationInfo]) {

  // Function to list all failed tasks , stages and jobs.
  def getFailedTasks: Seq[FailedTaskProfileResults] = {
    ProfFailedTaskView.getRawView(apps)
  }

  def getFailedStages: Seq[FailedStagesProfileResults] = {
    ProfFailedStageView.getRawView(apps)
  }

  def getFailedJobs: Seq[FailedJobsProfileResults] = {
    ProfFailedJobsView.getRawView(apps)
  }

  def getRemovedBlockManager: Seq[BlockManagerRemovedProfileResult] = {
    ProfRemovedBLKMgrView.getRawView(apps)
  }

  def getRemovedExecutors: Seq[ExecutorsRemovedProfileResult] = {
    ProfRemovedExecutorView.getRawView(apps)
  }

  // Function to list all *possible* not-supported plan nodes if GPU Mode=on
  def getPossibleUnsupportedSQLPlan: Seq[UnsupportedOpsProfileResult] = {
    val res = apps.flatMap { app =>
      app.planMetricProcessor.unsupportedSQLPlan.map { unsup =>
        // truncates the node description to 100 characters
        val renderedNodeDesc = StringUtils.renderStr(unsup.nodeDesc,
          doEscapeMetaCharacters = false)
        UnsupportedOpsProfileResult(unsup.sqlID, unsup.nodeID, unsup.nodeName,
          renderedNodeDesc, unsup.reason)
      }
    }
    if (res.nonEmpty) {
      res.sortBy(cols => (cols.sqlID, cols.nodeID))
    } else {
      Seq.empty
    }
  }
}
