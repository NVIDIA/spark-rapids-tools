/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.udf

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, PlatformFactory}
import com.nvidia.spark.rapids.tool.profiling.AutoTuner.loadClusterProps
import com.nvidia.spark.rapids.tool.qualification.{PluginTypeChecker, Qualification}
import com.nvidia.spark.rapids.tool.qualification.QualificationMain.logError
import org.apache.hadoop.hive.ql.exec.{Description, UDF}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification.QualificationSummaryInfo
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil

@Description(name = "estimate_event_rapids",
  value = "_FUNC_(output_path, event_path) ",
  extended = "Example:\n  > SELECT estimate_event_rapids(hdfs://..., app_1, hdfs://...)")
class EstimateEventRapidsUDF extends UDF with Logging {
  def evaluate(outputDir: String, applicationId: String, eventDir: String,
      scoreFile: String = "onprem"): Int = {
    val (exitCode, _) =
      EstimateEventRapidsUDF.estimateLog(outputDir, applicationId, eventDir, scoreFile)
    exitCode
  }
}

object EstimateEventRapidsUDF extends Logging {
  def estimateLog(outputDir: String, applicationId: String, eventDir: String,
      scoreFile: String = "onprem"): (Int, Seq[QualificationSummaryInfo]) = {
    val eventPath = eventDir + "/" + applicationId + "_1"
    val outputDirectory = outputDir + "/" + applicationId
    val numOutputRows = 1000
    val hadoopConf = RapidsToolsConfUtil.newHadoopConf()
    // timeout: 20min
    val timeout = Option(1200L)
    val nThreads = 1
    val order = "desc"
    // val pluginTypeChecker = try {
    //   new PluginTypeChecker("onprem", None)
    // } catch {
    //   case ie: IllegalStateException =>
    //     logError("Error creating the plugin type checker!", ie)
    //     return (1, Seq[QualificationSummaryInfo]())
    // }

    val platform = try {
      val clusterPropsOpt = loadClusterProps("")
      PlatformFactory.createInstance("", clusterPropsOpt)
    } catch {
      case NonFatal(e) =>
        logError("Error creating the platform", e)
        return (1, Seq[QualificationSummaryInfo]())
    }
    val pluginTypeChecker = try {
      new PluginTypeChecker(
        platform,
        None)
    } catch {
      case ie: IllegalStateException =>
        logError("Error creating the plugin type checker!", ie)
        return (1, Seq[QualificationSummaryInfo]())
    }

    val (eventLogFsFiltered, allEventLogs) = EventLogPathProcessor.processAllPaths(
      None, None, List(eventPath), hadoopConf)
    val filteredLogs = eventLogFsFiltered

    // uiEnabled = false
    val qual = new Qualification(outputDirectory, numOutputRows, hadoopConf,
      timeout, nThreads, order, pluginTypeChecker = pluginTypeChecker, reportReadSchema = false,
      printStdout = false, enablePB = true, reportSqlLevel = false, maxSQLDescLength = 100, mlOpsEnabled = false)
    try {
      val res = qual.qualifyApps(filteredLogs)
      (0, res)
    } catch {
      case NonFatal(e) =>
        logError(s"Error when analyze ${applicationId}, path is ${eventPath}.")
        e.printStackTrace()
        (1, Seq[QualificationSummaryInfo]())
    }
  }
}
