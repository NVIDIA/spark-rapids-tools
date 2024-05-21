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

import com.nvidia.spark.rapids.tool.analysis.{AggRawMetricsResult, AppSQLPlanAnalyzer, QualSparkMetricsAnalyzer}
import com.nvidia.spark.rapids.tool.profiling.{ProfileOutputWriter, ProfileResult}

import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo

/**
 * This object generates the raw metrics view for the qualification tool. It is used to generate
 * the CSV files without applying any heuristics or estimation.
 */
object QualRawReportGenerator {

  private def constructLabelsMaps(
      aggRawResult: AggRawMetricsResult): Map[String, Seq[ProfileResult]] = {
    val sortedRes = AggRawMetricsResult(
      AggMetricsResultSorter.sortJobSparkMetrics(aggRawResult.jobAggs),
      AggMetricsResultSorter.sortJobSparkMetrics(aggRawResult.stageAggs),
      AggMetricsResultSorter.sortShuffleSkew(aggRawResult.taskShuffleSkew),
      AggMetricsResultSorter.sortSqlAgg(aggRawResult.sqlAggs),
      AggMetricsResultSorter.sortIO(aggRawResult.ioAggs),
      AggMetricsResultSorter.sortSqlDurationAgg(aggRawResult.sqlDurAggs),
      aggRawResult.maxTaskInputSizes)
    Map(
      STAGE_AGG_LABEL -> sortedRes.stageAggs,
      JOB_AGG_LABEL -> sortedRes.jobAggs,
      TASK_SHUFFLE_SKEW -> sortedRes.taskShuffleSkew,
      SQL_AGG_LABEL -> sortedRes.sqlAggs,
      IO_LABEL -> sortedRes.ioAggs,
      SQL_DUR_LABEL -> sortedRes.sqlDurAggs)
  }

  private def generateSQLProcessingView(
      pWriter: ProfileOutputWriter, app: QualificationAppInfo,
      appIndex: Int): Unit = {
    // We only need the SQL analyzer here to generate the output. It is not saved in the AppBase to
    // save memory
    val sqlPlanAnalyzer = AppSQLPlanAnalyzer(app, appIndex)
    pWriter.write(QualSQLToStageView.getLabel,
      QualSQLToStageView.getRawViewFromSqlProcessor(sqlPlanAnalyzer))
    pWriter.write(QualSQLPlanMetricsView.getLabel,
      QualSQLPlanMetricsView.getRawViewFromSqlProcessor(sqlPlanAnalyzer),
      Some(QualSQLPlanMetricsView.getDescription))
    pWriter.write(QualSQLCodeGenView.getLabel,
      QualSQLCodeGenView.getRawViewFromSqlProcessor(sqlPlanAnalyzer),
      Some(QualSQLCodeGenView.getDescription))
  }

  def generateRawMetricQualView(
      rootDir: String,
      app: QualificationAppInfo,
      appIndex: Int = 1): Unit = {
    val metricsDirectory = s"$rootDir/raw_metrics/${app.appId}"
    val pWriter =
      new ProfileOutputWriter(metricsDirectory, "profile", 10000000, outputCSV = true)
    try {
      pWriter.writeText("### A. Information Collected ###")
      pWriter.write(QualExecutorView.getLabel, QualExecutorView.getRawView(Seq(app)))
      pWriter.write(QualAppJobView.getLabel, QualAppJobView.getRawView(Seq(app)))
      generateSQLProcessingView(pWriter, app, appIndex)
      pWriter.writeText("\n### B. Analysis ###\n")
      constructLabelsMaps(
        QualSparkMetricsAnalyzer.getAggRawMetrics(app, appIndex)).foreach { case (label, metrics) =>
        pWriter.write(label,
          metrics,
          AGG_DESCRIPTION.get(label))
      }
      pWriter.writeText("\n### C. Health Check###\n")
      pWriter.write(QualFailedTaskView.getLabel, QualFailedTaskView.getRawView(Seq(app)))
      pWriter.write(QualFailedStageView.getLabel, QualFailedStageView.getRawView(Seq(app)))
      pWriter.write(QualAppFailedJobView.getLabel, QualAppFailedJobView.getRawView(Seq(app)))
      pWriter.write(QualRemovedBLKMgrView.getLabel, QualRemovedBLKMgrView.getRawView(Seq(app)))
      pWriter.write(QualRemovedExecutorView.getLabel, QualRemovedExecutorView.getRawView(Seq(app)))
    } catch {
      case e: Exception =>
        println(s"Error generating raw metrics for ${app.appId}: ${e.getMessage}")
    } finally {
      pWriter.close()
    }
  }
}
