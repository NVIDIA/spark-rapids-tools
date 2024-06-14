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

import com.nvidia.spark.rapids.tool.Platform
import com.nvidia.spark.rapids.tool.analysis.QualSparkMetricsAnalyzer
import com.nvidia.spark.rapids.tool.profiling.{DataSourceProfileResult, RecommendedCommentResult, RecommendedPropertyResult}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification.{QualificationAppInfo, QualificationSummaryInfo}
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil

case class TuningResult(
    appID: String,
    recommendations: Seq[RecommendedPropertyResult],
    comments: Seq[RecommendedCommentResult],
    combinedProps: Option[Seq[RecommendedPropertyResult]] = None)

/**
 * Container which holds metadata and arguments specific to the execution of the AutoTuner.
 * TODO: we need to use the same class in constructing the AutoTuner in the Profiling tools.
 * @param platform object representing the host platform on which the application was executed.
 * @param outputRootDir the output directory to dump the recommendation/comments.
 * @param hadoopConf optional configuration to access the remote storage.
 */
case class TunerContext (
    platform: Platform,
    workerInfoPath: String,
    outputRootDir: String,
    hadoopConf: Configuration) extends Logging {

  def getOutputPath: String = {
    s"$outputRootDir/rapids_4_spark_qualification_output/tuning"
  }

  def tuneApplication(
      appInfo: QualificationAppInfo,
      appAggStats: Option[QualificationSummaryInfo],
      appIndex: Int = 1,
      dsInfo: Seq[DataSourceProfileResult]): Option[TuningResult] = {
    val rawAggMetrics = QualSparkMetricsAnalyzer.getAggRawMetrics(appInfo, appIndex)
    QualificationAutoTuner(appInfo, appAggStats, this, rawAggMetrics, dsInfo).collect {
      case qualTuner =>
        Try {
          qualTuner.runAutoTuner()
        } match {
          case Success(r) => r
          case Failure(e) =>
            logError(s"Failed to generate tuning recommendations for app: ${appInfo.appId}", e)
            null
        }
    }
  }
}

object TunerContext extends Logging {
  def apply(platform: Platform,
      workerInfoPath: String,
      outputRootDir: String,
      hadoopConf: Option[Configuration] = None): Option[TunerContext] = {
    Try {
      val hConf = hadoopConf.getOrElse(RapidsToolsConfUtil.newHadoopConf())
      TunerContext(platform, workerInfoPath,
        outputRootDir, hConf)
    } match {
      case Success(c) => Some(c)
      case Failure(e) =>
        logError("Could not create Tuner Context", e)
        None
    }
  }
}
