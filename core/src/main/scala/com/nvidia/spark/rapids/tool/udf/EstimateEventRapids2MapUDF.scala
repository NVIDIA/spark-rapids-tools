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

import java.util

import org.apache.hadoop.hive.ql.exec.{Description, UDF}

import org.apache.spark.internal.Logging

@Description(name = "estimate_event_rapids_2_map",
  value = "_FUNC_(output_path, event_path) ",
  extended = "Example:\n  > SELECT estimate_event_rapids(hdfs://..., app_1, hdfs://...)" +
    " - Returns QualificationSummaryInfo map.")
class EstimateEventRapids2MapUDF extends UDF with Logging {
  def evaluate(outputDir: String, applicationId: String, eventDir: String,
      scoreFile: String = "onprem"): java.util.Map[String, String] = {
    val estimateOutput =
      EstimateEventRapidsUDF.estimateLog(outputDir, applicationId, eventDir, scoreFile)
    val firstQualificationSummaryInfo = estimateOutput._2.headOption
    firstQualificationSummaryInfo.map { q =>
      val javaMap: util.HashMap[String, String] = new util.HashMap[String, String]()
      javaMap.put("appName", q.appName)
      javaMap.put("appId", q.appId)
      javaMap.put("Recommendation", q.estimatedInfo.recommendation)
      javaMap.put("Speedup", q.taskSpeedupFactor.toString)
      javaMap.put("LongestSQLDuration", q.longestSqlDuration.toString)
      javaMap.put("UnsupportedExecs", q.unSupportedExecs)
      javaMap.put("UnsupportedExpressions", q.unSupportedExprs)
      javaMap
    }.getOrElse(new util.HashMap[String, String]())
  }
}
