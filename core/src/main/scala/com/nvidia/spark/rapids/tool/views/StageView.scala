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

package com.nvidia.spark.rapids.tool.views

import com.nvidia.spark.rapids.tool.analysis.{AppSQLPlanAnalyzer, ProfAppIndexMapperTrait, QualAppIndexMapperTrait}
import com.nvidia.spark.rapids.tool.profiling.{FailedStagesProfileResults, SQLStageInfoProfileResult}

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.util.StringUtils

trait AppFailedStageViewTrait extends ViewableTrait[FailedStagesProfileResults] {
  override def getLabel: String = "Failed Stages"

  override def getRawView(app: AppBase, index: Int): Seq[FailedStagesProfileResults] = {
    app.stageManager.getFailedStages.map { fsm =>
      FailedStagesProfileResults(fsm.stageInfo.stageId,
        fsm.stageInfo.attemptNumber(),
        fsm.stageInfo.name, fsm.stageInfo.numTasks,
        StringUtils.renderStr(fsm.getFailureReason, doEscapeMetaCharacters = false, maxLength = 0))
    }.toSeq
  }

  override def sortView(
      rows: Seq[FailedStagesProfileResults]): Seq[FailedStagesProfileResults] = {
    rows.sortBy(cols => (cols.stageId, cols.stageAttemptId))
  }
}


trait AppSQLToStageViewTrait extends ViewableTrait[SQLStageInfoProfileResult] {
  override def getLabel: String = "SQL to Stage Information"

  override def sortView(
      rows: Seq[SQLStageInfoProfileResult]): Seq[SQLStageInfoProfileResult] = {
    case class Reverse[T](t: T)
    implicit def ReverseOrdering[T: Ordering]: Ordering[Reverse[T]] =
      Ordering[T].reverse.on(_.t)

    // intentionally sort this table by the duration to be able to quickly
    // see the stage that took the longest
    rows.sortBy(cols => (Reverse(cols.duration)))
  }
}


object QualFailedStageView extends AppFailedStageViewTrait with QualAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object ProfFailedStageView extends AppFailedStageViewTrait with ProfAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object QualSQLToStageView extends AppSQLToStageViewTrait with QualAppIndexMapperTrait {
  override def getRawView(app: AppBase, index: Int): Seq[SQLStageInfoProfileResult] = {
    // TODO: Fix this implementation when we have a better way to get bind between App and
    //  SQLProcessor but for now, we do not store SQLPlan Processor in QualificationSummaryInfo
    // to save
    Seq.empty
  }

  def getRawViewFromSqlProcessor(
      sqlAnalyzer: AppSQLPlanAnalyzer): Seq[SQLStageInfoProfileResult] = {
    sortView(sqlAnalyzer.aggregateSQLStageInfo)
  }
}

object ProfSQLToStageView extends AppSQLToStageViewTrait with ProfAppIndexMapperTrait {
  override def getRawView(app: AppBase, index: Int): Seq[SQLStageInfoProfileResult] = {
    app.asInstanceOf[ApplicationInfo].planMetricProcessor.aggregateSQLStageInfo
  }
}
