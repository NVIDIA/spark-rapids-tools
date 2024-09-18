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

import java.io.{BufferedReader, File, InputStreamReader}

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, PlatformFactory}
import com.nvidia.spark.rapids.tool.profiling.AutoTuner.loadClusterProps
import com.nvidia.spark.rapids.tool.qualification.{PluginTypeChecker, Qualification}
import org.apache.hadoop.hive.ql.exec.{Description, UDF}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification.QualificationSummaryInfo
import org.apache.spark.sql.rapids.tool.util.{RapidsToolsConfUtil, UTF8Source}

@Description(name = "estimate_event_rapids",
  value = "_FUNC_(output_path, event_path) ",
  extended = "Example:\n  > SELECT estimate_event_rapids(hdfs://..., app_1, hdfs://...)")
class EstimateEventRapidsUDF extends UDF with Logging {
  def evaluate(outputDir: String, applicationId: String, eventDir: String,
      scoreFile: String = "onprem"): Int = {
    val (exitCode, _, _) =
      EstimateEventRapidsUDF.estimateLog(outputDir, applicationId, eventDir, scoreFile)
    exitCode
  }
}

object EstimateEventRapidsUDF extends Logging {
  def estimateLog(outputDir: String, applicationId: String, eventDir: String,
      scoreFile: String = "onprem",
      enabledML: Boolean = false): (Int, Seq[QualificationSummaryInfo], String) = {
    val eventPath = eventDir + "/" + applicationId + "_1"
    val outputDirectory = outputDir + "/" + applicationId
    val numOutputRows = 1000
    val hadoopConf = RapidsToolsConfUtil.newHadoopConf()
    // timeout: 20min
    val timeout = Option(1200L)
    val nThreads = 1
    val order = "desc"

    val platform = try {
      val clusterPropsOpt = loadClusterProps("./worker_info.yaml")
      PlatformFactory.createInstance("", clusterPropsOpt)
    } catch {
      case NonFatal(e) =>
        logError("Error creating the platform", e)
        return (1, Seq[QualificationSummaryInfo](), "")
    }
    val pluginTypeChecker = try {
      new PluginTypeChecker(
        platform,
        None)
    } catch {
      case ie: IllegalStateException =>
        logError("Error creating the plugin type checker!", ie)
        return (1, Seq[QualificationSummaryInfo](), "")
    }

    val (eventLogFsFiltered, _) = EventLogPathProcessor.processAllPaths(
      None, None, List(eventPath), hadoopConf)
    val filteredLogs = eventLogFsFiltered

    // uiEnabled = false
    val qual = new Qualification(
      outputPath=outputDirectory,
      numRows=numOutputRows,
      hadoopConf=hadoopConf,
      timeout= timeout,
      nThreads=nThreads,
      order=order,
      pluginTypeChecker=pluginTypeChecker,
      reportReadSchema=false,
      printStdout=false,
      enablePB=true,
      reportSqlLevel=false,
      maxSQLDescLength=100,
      mlOpsEnabled=false,
      penalizeTransitions=true,
      tunerContext=None,
      clusterReport = false
    )
    try {
      val res: Seq[QualificationSummaryInfo] = qual.qualifyApps(filteredLogs)
      val predictScore = if (enabledML && res.nonEmpty) {
        // 传python
        // outputDirectory
        execMLPredict(applicationId, outputDirectory)
      } else ""
      (0, res, predictScore)
    } catch {
      case NonFatal(e) =>
        logError(s"Error when analyze ${applicationId}, path is ${eventPath}.", e)
        (1, Seq[QualificationSummaryInfo](), "")
    }
  }

  private def execMLPredict(applicationId: String, outputDirectory: String): String = {
    var proc: Process = null
    val mlOutputPrefix = s"${applicationId}_pre"
    val mlOutput = s"$mlOutputPrefix/ml_qualx_output"
    val command = "spark_rapids prediction --platform onprem " +
        s"--qual_output $outputDirectory" +
        "--output_folder " +
        s"./$mlOutput"
    proc = Runtime.getRuntime.exec(command)
    val in = new BufferedReader(new InputStreamReader(proc.getInputStream))
    Iterator.continually(in.readLine()).takeWhile(_ != null).foreach(println)
    in.close()
    proc.waitFor

    val dir = new File(s"./$mlOutput")
    // 过滤出以"predict"开头的文件
    val predictFiles = dir.listFiles.find(_.getName.startsWith("prediction"))
    assert(predictFiles.nonEmpty, "can not find prediction file")

    // read ml output
    var predictScore = "0.0"
    val filePath = s"./$mlOutput/${predictFiles.head}"
    val source = UTF8Source.fromFile(filePath)
    try {
      for (line <- source.getLines()) {
        if (line.startsWith("Overall estimated speedup")) {
          predictScore = line.split("\\s+").last
        }
      }
    } finally {
      source.close()
    }
    predictScore
  }
}
