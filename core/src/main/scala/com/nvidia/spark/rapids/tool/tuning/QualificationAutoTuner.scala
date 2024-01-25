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

package com.nvidia.spark.rapids.tool.tuning

import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import com.nvidia.spark.rapids.tool.profiling.{AppSummaryInfoBaseProvider, AutoTuner, Profiler}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification.{QualificationAppInfo, QualificationSummaryInfo}

/**
 * Implementation of the AutoTuner for Qualification.
 * @param appInfoProvider Provider of the qualification analysis data
 * @param tunerContext Container which holds the arguments passed to the AutoTuner execution
 */
class QualificationAutoTuner(val appInfoProvider: QualAppSummaryInfoProvider,
    val tunerContext: TunerContext) {

  private def writeTuningReport(tuningResult: TuningResult,
      outputDir: String, hadoopConf: Configuration): Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir,
      s"${tuningResult.appID}.log", s"Tuning Qual app - ${tuningResult.appID}", Option(hadoopConf))
    try {
      textFileWriter.write(s"### Recommended Configuration for App: ${tuningResult.appID} ###\n")
      textFileWriter.write(Profiler.getAutoTunerResultsAsString(
        tuningResult.recommendations, tuningResult.comments))
    } finally {
      textFileWriter.close()
    }
  }
  def runAutoTuner(): TuningResult = {
    val autoTuner: AutoTuner = AutoTuner.buildAutoTuner(
      tunerContext.workerInfoPath, appInfoProvider, tunerContext.platform)
    val (recommendations, comments) = autoTuner.getRecommendedProperties()
    val resultRecord = TuningResult(appInfoProvider.getAppID, recommendations, comments)
    writeTuningReport(resultRecord, tunerContext.getOutputPath, tunerContext.hadoopConf)
    resultRecord
  }
}

object QualificationAutoTuner extends Logging {
  def apply(appInfo: QualificationAppInfo,
      appAggStats: Option[QualificationSummaryInfo],
      tunerContext: TunerContext): Option[QualificationAutoTuner] = {
    Try {
      val qualInfoProvider: QualAppSummaryInfoProvider =
        AppSummaryInfoBaseProvider.fromQualAppInfo(appInfo, appAggStats)
          .asInstanceOf[QualAppSummaryInfoProvider]
      new QualificationAutoTuner(qualInfoProvider, tunerContext)
    } match {
      case Success(q) => Some(q)
      case Failure(e) =>
        logError(
          s"Failed to create Qualification tuning object for application ${appInfo.appId}", e)
        None
    }
  }
}
