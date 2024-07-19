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

import java.io.{PrintWriter, StringWriter}

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import com.nvidia.spark.rapids.tool.profiling.AppStatusResult
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.ToolUtils


trait RuntimeReporter extends Logging {
  val outputDir: String
  def generateRuntimeReport(hadoopConf: Option[Configuration] = None): Unit = {
    RuntimeReportGenerator.generateReport(outputDir, hadoopConf)
  }

  /**
   * For each app status report, generate an AppStatusResult.
   * If appId is empty, convert to "N/A" in the output.
   * @return Seq[AppStatusResult] - Seq[(path, status, appId, message)]
   */
  def generateStatusResults(appStatuses: Seq[AppResult]): Seq[AppStatusResult] = {
    appStatuses.map {
      case FailureAppResult(path, message) =>
        AppStatusResult(path, "FAILURE", "N/A", message)
      case SkippedAppResult(path, message) =>
        AppStatusResult(path, "SKIPPED", "N/A", message)
      case SuccessAppResult(path, appId, message) =>
        AppStatusResult(path, "SUCCESS", appId, message)
      case UnknownAppResult(path, appId, message) =>
        val finalAppId = if (appId.isEmpty) "N/A" else appId
        AppStatusResult(path, "UNKNOWN", finalAppId, message)
      case profAppResult: AppResult =>
        throw new UnsupportedOperationException(s"Invalid status for $profAppResult")
    }
  }
}

/**
 * Generates a file containing the properties of the build loaded.
 * In addition, it concatenates properties from the runtime (i.e., SparkVersion).
 * It is expected that the list of properties in that file will grow depending on whether a
 * property helps understanding and investigating the tools output.
 * @param outputDir the directory where the report is generated.
 * @param hadoopConf the hadoop configuration object used to access the HDFS if any.
 */
object RuntimeReportGenerator extends Logging {
  private val REPORT_LABEL = "RAPIDS Accelerator for Apache Spark's Build/Runtime Information"
  private val REPORT_FILE_NAME = "runtime.properties"
  def generateReport(outputDir: String, hadoopConf: Option[Configuration] = None): Unit = {
    val buildProps = RapidsToolsConfUtil.loadBuildProperties
    // Add the Spark version used in runtime.
    // Note that it is different from the Spark version used in the build.
    buildProps.setProperty("runtime.spark.version", ToolUtils.sparkRuntimeVersion)
    if (!ToolUtils.sparkRapidsRuntimeVersion.isEmpty) {
      buildProps.setProperty("runtime.sparkRapids.version", ToolUtils.sparkRapidsRuntimeVersion.get)
    }
    if (!ToolUtils.jniRuntimeVersion.isEmpty) {
      buildProps.setProperty("runtime.sparkRapidsJNI.version", ToolUtils.jniRuntimeVersion.get)
    }
    if (!ToolUtils.cudfRuntimeVersion.isEmpty) {
      buildProps.setProperty("runtime.sparkRapidsCUDF.version", ToolUtils.cudfRuntimeVersion.get)
    }
    val reportWriter = new ToolTextFileWriter(outputDir, REPORT_FILE_NAME, REPORT_LABEL, hadoopConf)
    try {
      reportWriter.writeProperties(buildProps, REPORT_LABEL)
    } finally {
      reportWriter.close()
    }
    // Write the properties to the log
    val writer = new StringWriter
    buildProps.list(new PrintWriter(writer))
    logInfo(s"\n$REPORT_LABEL\n${writer.getBuffer.toString}")
  }
}


