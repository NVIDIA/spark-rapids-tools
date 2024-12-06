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

package org.apache.spark.sql.rapids.tool.util

import com.nvidia.spark.rapids.tool.profiling.AppStatusResult
import com.nvidia.spark.rapids.tool.qualification.AppSubscriber
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging

trait RuntimeReporter extends Logging {
  val outputDir: String
  def generateRuntimeReport(hadoopConf: Option[Configuration] = None): Unit = {
    RuntimeUtil.generateReport(outputDir, hadoopConf)
  }
  def logOutputPath(): Unit = {
    logInfo(s"Tools output directory: $outputDir")
  }

  /**
   * Updates the status of "SUCCESS" applications to "SKIPPED" if newer attempts with
   * the same appId exist.
   */
  private def skipAppsWithOlderAttempts(appStatuses: Seq[AppResult]): Seq[AppResult] = {
    appStatuses map {
      case successApp: SuccessAppResult =>
        AppSubscriber.withUnsafeValidAttempt(successApp.appId, successApp.attemptId) { () =>
          successApp
        }.getOrElse(SkippedAppResult.fromAppAttempt(successApp.path, successApp.appId,
          successApp.attemptId))
      case otherApp: AppResult => otherApp
    }
  }

  /**
   * For each app status report, generate an AppStatusResult.
   * If appId is empty, convert to "N/A" in the output.
   * @return Seq[AppStatusResult] - Seq[(path, status, appId, message)]
   */
  def generateStatusResults(appStatuses: Seq[AppResult]): Seq[AppStatusResult] = {
    skipAppsWithOlderAttempts(appStatuses).map {
      case FailureAppResult(path, message) =>
        AppStatusResult(path, "FAILURE", "N/A", message)
      case SkippedAppResult(path, message) =>
        AppStatusResult(path, "SKIPPED", "N/A", message)
      case SuccessAppResult(path, appId, _, message) =>
        AppStatusResult(path, "SUCCESS", appId, message)
      case UnknownAppResult(path, appId, message) =>
        val finalAppId = if (appId.isEmpty) "N/A" else appId
        AppStatusResult(path, "UNKNOWN", finalAppId, message)
      case profAppResult: AppResult =>
        throw new UnsupportedOperationException(s"Invalid status for $profAppResult")
    }
  }
}
