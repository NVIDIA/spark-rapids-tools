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
import java.util

import org.apache.hadoop.hive.ql.exec.{Description, UDF}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.util.UTF8Source

@Description(name = "estimate_event_rapids_ml",
  value = "_FUNC_(applicationId, event_path) ",
  extended = "Example:\n  > SELECT estimate_event_rapids(app_1, hdfs://...)" +
      " - Returns rapids-tool ml score.")
class EstimateEventRapids2MapMLUDF extends UDF with Logging {

  private def runProc(applicationId: String, command: String): Unit = {
    logInfo(s"Running rapids-tools for $applicationId: $command")
    val proc = Runtime.getRuntime.exec(command)
    val in = new BufferedReader(new InputStreamReader(proc.getInputStream))
    Iterator.continually(in.readLine()).takeWhile(_ != null).foreach(println)
    in.close()
    proc.waitFor
  }

  private def readMlOutput(applicationId: String, mlOutputFolder: String): String = {
    val dir = new File(s"$mlOutputFolder")
    val predictFiles = dir.listFiles.find(_.getName.startsWith("prediction"))
    var predictScore = "0.0"
    if (predictFiles.nonEmpty) {
      val filePath = s"$mlOutputFolder/${predictFiles.head}"
      val source = UTF8Source.fromFile(filePath)
      try {
        for (line <- source.getLines()) {
          if (line.startsWith("Overall estimated speedup")) {
            predictScore = line.split("\\s+").last
            logInfo(s"predict score for $applicationId is $predictScore")
          }
        }
      } finally {
        source.close()
      }
    } else {
      logError(s"Can not find prediction file in $mlOutputFolder")
    }
    predictScore
  }

  def evaluate(applicationId: String, eventDir: String): java.util.Map[String, String] = {
    val eventPath = eventDir + "/" + applicationId + "_1"
    val outputPrefix = "/opt/tiger/yodel/container/rapids_tools"
    val outputFolder = s"file://$outputPrefix/$applicationId"
    val command1 = s"spark_rapids qualification --platform onprem --eventlogs $eventPath " +
        s"--output_folder $outputFolder"
    runProc(applicationId, command1)

    val mlOutputFolder = s"$outputPrefix/${applicationId}_ml"
    val command2 = "spark_rapids prediction --platform onprem " +
        s"--qual_output ${outputFolder.substring(7)}" +
        s"--output_folder $mlOutputFolder"
    runProc(applicationId, command2)

    val score = readMlOutput(applicationId, mlOutputFolder)
    val javaMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    logInfo(s"applicationId ml predict score is $score")
    javaMap.put("rapids_ml_score", score)
    javaMap
  }
}
