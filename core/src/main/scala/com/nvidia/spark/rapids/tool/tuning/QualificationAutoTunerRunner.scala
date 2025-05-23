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

package com.nvidia.spark.rapids.tool.tuning

import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.tool.{AppSummaryInfoBaseProvider, Platform, ToolTextFileWriter}
import com.nvidia.spark.rapids.tool.analysis.AggRawMetricsResult
import com.nvidia.spark.rapids.tool.profiling.{DataSourceProfileResult, Profiler}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification.{QualificationAppInfo, QualificationSummaryInfo}

/**
 * A wrapper class to run the AutoTuner for Qualification Tool.
 * @param appInfoProvider Provider of the qualification analysis data
 * @param tunerContext Container which holds the arguments passed to the AutoTuner execution
 */
class QualificationAutoTunerRunner(val appInfoProvider: QualAppSummaryInfoProvider,
    val tunerContext: TunerContext) {

  private def writeTuningReport(tuningResult: TuningResult,
      outputDir: String, hadoopConf: Configuration): Unit = {
    // First, write down the recommendations and the comments
    val textFileWriter = new ToolTextFileWriter(outputDir,
      s"${tuningResult.appID}.log", s"Tuning Qual App - ${tuningResult.appID}", Option(hadoopConf))
    try {
      textFileWriter.write(
        s"### Recommended SPARK Configuration on GPU Cluster for App: ${tuningResult.appID} ###\n")
      textFileWriter.write(Profiler.getAutoTunerResultsAsString(
        tuningResult.recommendations, tuningResult.comments))
    } finally {
      textFileWriter.close()
    }
    // Write down the recommended properties
    val bootstrapReport = new BootstrapReport(tuningResult, outputDir, hadoopConf)
    bootstrapReport.generateReport()
    // Write down the combined configurations
    tuningResult.combinedProps.collect {
      case combinedProps =>
        val textFileWriter = new ToolTextFileWriter(outputDir,
          s"${tuningResult.appID}.conf",
          s"Qual combined configurations for App - ${tuningResult.appID}", Option(hadoopConf))
        try {
          textFileWriter.write(combinedProps.map(_.toString).reduce(_ + "\n" + _))
        } finally {
          textFileWriter.close()
        }
    }
  }

  def runAutoTuner(platform: Platform): TuningResult = {
    val autoTuner: AutoTuner =
      QualificationAutoTunerConfigsProvider.buildAutoTuner(appInfoProvider, platform)
    val (recommendations, comments) =
      autoTuner.getRecommendedProperties(showOnlyUpdatedProps =
        QualificationAutoTunerRunner.filterByUpdatedPropsEnabled)
    // Combine the GPU recommendations with all others.
    // There are two ways we can do that:
    // 1- Combine them from the beginning; Or
    // 2- At the end, get the union of the two properties.
    // The 2nd needs more effort but it favourite because it keeps two separate lists.
    // Otherwise, it is difficult to separate them logically.
    val combinedProps = autoTuner.combineSparkProperties(recommendations)
    val resultRecord = TuningResult(appInfoProvider.getAppID, recommendations,
      comments, combinedProps = Option(combinedProps))
    writeTuningReport(resultRecord, tunerContext.getOutputPath, tunerContext.hadoopConf)
    resultRecord
  }
}

object QualificationAutoTunerRunner extends Logging {

  // When enabled, the profiler recommendations should only include updated settings.
  val filterByUpdatedPropsEnabled: Boolean = false

  def apply(appInfo: QualificationAppInfo,
      appAggStats: Option[QualificationSummaryInfo],
      tunerContext: TunerContext,
      rawAggMetrics: AggRawMetricsResult,
      dsInfo: Seq[DataSourceProfileResult]): Option[QualificationAutoTunerRunner] = {
    Try {
      val qualInfoProvider: QualAppSummaryInfoProvider =
        AppSummaryInfoBaseProvider.fromQualAppInfo(appInfo, appAggStats, rawAggMetrics, dsInfo)
          .asInstanceOf[QualAppSummaryInfoProvider]
      new QualificationAutoTunerRunner(qualInfoProvider, tunerContext)
    } match {
      case Success(q) => Some(q)
      case Failure(e) =>
        logError(
          s"Failed to create Qualification tuning object for application ${appInfo.appId}", e)
        None
    }
  }
}
