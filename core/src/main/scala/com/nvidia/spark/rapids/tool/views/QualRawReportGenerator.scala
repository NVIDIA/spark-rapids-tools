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

import com.nvidia.spark.rapids.tool.analysis.{AggRawMetricsResult, AppSQLPlanAnalyzer, QualSparkMetricsAnalyzer}
import com.nvidia.spark.rapids.tool.profiling.{DataSourceProfileResult, ProfileOutputWriter, ProfileResult, SQLAccumProfileResults}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo

/**
 * This object generates the raw metrics view for the qualification tool. It is used to generate
 * the CSV files without applying any heuristics or estimation.
 */
object QualRawReportGenerator extends Logging {

  private def constructLabelsMaps(
      aggRawResult: AggRawMetricsResult): Map[String, Seq[ProfileResult]] = {
    val sortedRes = AggRawMetricsResult(
      AggMetricsResultSorter.sortJobSparkMetrics(aggRawResult.jobAggs),
      AggMetricsResultSorter.sortJobSparkMetrics(aggRawResult.stageAggs),
      AggMetricsResultSorter.sortShuffleSkew(aggRawResult.taskShuffleSkew),
      AggMetricsResultSorter.sortSqlAgg(aggRawResult.sqlAggs),
      AggMetricsResultSorter.sortIO(aggRawResult.ioAggs),
      AggMetricsResultSorter.sortSqlDurationAgg(aggRawResult.sqlDurAggs),
      aggRawResult.maxTaskInputSizes,
      AggMetricsResultSorter.sortStageDiagnostics(aggRawResult.stageDiagnostics))
    Map(
      STAGE_AGG_LABEL -> sortedRes.stageAggs,
      JOB_AGG_LABEL -> sortedRes.jobAggs,
      TASK_SHUFFLE_SKEW -> sortedRes.taskShuffleSkew,
      SQL_AGG_LABEL -> sortedRes.sqlAggs,
      IO_LABEL -> sortedRes.ioAggs,
      SQL_DUR_LABEL -> sortedRes.sqlDurAggs,
      STAGE_DIAGNOSTICS_LABEL -> sortedRes.stageDiagnostics)
  }

  private def generateSQLProcessingView(
      pWriter: ProfileOutputWriter,
      sqlPlanAnalyzer: AppSQLPlanAnalyzer): Seq[SQLAccumProfileResults] = {
    pWriter.writeTable(QualSQLToStageView.getLabel,
      QualSQLToStageView.getRawViewFromSqlProcessor(sqlPlanAnalyzer))
    val sqlPlanMetrics = QualSQLPlanMetricsView.getRawViewFromSqlProcessor(sqlPlanAnalyzer)
    pWriter.writeTable(QualSQLPlanMetricsView.getLabel,
      sqlPlanMetrics,
      Some(QualSQLPlanMetricsView.getDescription))
    pWriter.writeTable(QualSQLCodeGenView.getLabel,
      QualSQLCodeGenView.getRawViewFromSqlProcessor(sqlPlanAnalyzer),
      Some(QualSQLCodeGenView.getDescription))
    sqlPlanMetrics
  }

  def generateRawMetricQualViewAndGetDataSourceInfo(
      rootDir: String,
      app: QualificationAppInfo): Seq[DataSourceProfileResult] = {
    val metricsDirectory = s"$rootDir/raw_metrics/${app.appId}"
    val sqlPlanAnalyzer = AppSQLPlanAnalyzer(app)
    var dataSourceInfo: Seq[DataSourceProfileResult] = Seq.empty
    val pWriter =
      new ProfileOutputWriter(metricsDirectory, "profile", 10000000, outputCSV = true)
    try {
      pWriter.writeText("### A. Information Collected ###")
      pWriter.writeTable(
        QualInformationView.getLabel, QualInformationView.getRawView(Seq(app)))
      pWriter.writeTable(QualLogPathView.getLabel, QualLogPathView.getRawView(Seq(app)))
      val sqlPlanMetricsResults = generateSQLProcessingView(pWriter, sqlPlanAnalyzer)
      pWriter.writeJson(
        QualAppSQLPlanInfoView.getLabel,
        QualAppSQLPlanInfoView.getRawView(Seq(app)),
        pretty = false)
      // Skipping writing to profile file as it would be too large
      dataSourceInfo = QualDataSourceView.getRawView(Seq(app), sqlPlanMetricsResults)
      pWriter.writeTable(QualDataSourceView.getLabel, dataSourceInfo)
      pWriter.writeTable(QualExecutorView.getLabel, QualExecutorView.getRawView(Seq(app)))
      pWriter.writeTable(QualAppJobView.getLabel, QualAppJobView.getRawView(Seq(app)))
      pWriter.writeTable(QualStageMetricView.getLabel,
        QualStageMetricView.getRawViewFromSqlProcessor(sqlPlanAnalyzer),
        Some(QualStageMetricView.getDescription))
      pWriter.writeTable(RapidsQualPropertiesView.getLabel,
        RapidsQualPropertiesView.getRawView(Seq(app)),
        Some(RapidsQualPropertiesView.getDescription))
      pWriter.writeTable(SparkQualPropertiesView.getLabel,
        SparkQualPropertiesView.getRawView(Seq(app)),
        Some(SparkQualPropertiesView.getDescription))
      pWriter.writeTable(SystemQualPropertiesView.getLabel,
        SystemQualPropertiesView.getRawView(Seq(app)),
        Some(SystemQualPropertiesView.getDescription))
      pWriter.writeText("\n### B. Analysis ###\n")
      constructLabelsMaps(QualSparkMetricsAnalyzer.
        getAggRawMetrics(
          app, sqlAnalyzer = Some(sqlPlanAnalyzer))).foreach { case (label, metrics) =>
          if (label == STAGE_DIAGNOSTICS_LABEL) {
            pWriter.writeCSVTable(label, metrics)
          } else {
            pWriter.writeTable(label, metrics, AGG_DESCRIPTION.get(label))
          }
      }
      pWriter.writeText("\n### C. Health Check###\n")
      pWriter.writeTable(QualFailedTaskView.getLabel, QualFailedTaskView.getRawView(Seq(app)))
      pWriter.writeTable(
        QualFailedStageView.getLabel, QualFailedStageView.getRawView(Seq(app)))
      pWriter.writeTable(
        QualAppFailedJobView.getLabel, QualAppFailedJobView.getRawView(Seq(app)))
      pWriter.writeTable(
        QualRemovedBLKMgrView.getLabel, QualRemovedBLKMgrView.getRawView(Seq(app)))
      pWriter.writeTable(
        QualRemovedExecutorView.getLabel, QualRemovedExecutorView.getRawView(Seq(app)))
      // we only need to write the CSV report of the WriteOps
      pWriter.writeCSVTable(QualWriteOpsView.getLabel, QualWriteOpsView.getRawView(Seq(app)))
    } catch {
      case e: Exception =>
        logError(s"Error generating raw metrics for ${app.appId}: ${e.getMessage}")
    } finally {
      pWriter.close()
    }
    dataSourceInfo
  }
}
