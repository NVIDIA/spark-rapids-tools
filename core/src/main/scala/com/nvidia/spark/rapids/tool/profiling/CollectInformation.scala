/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.profiling

import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.nvidia.spark.rapids.SparkRapidsBuildInfoEvent
import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import com.nvidia.spark.rapids.tool.views._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

case class StageMetrics(numTasks: Int, duration: String)

/**
 * CollectInformation mainly gets information based on this event log:
 * Such as executors, parameters, etc.
 */
class CollectInformation(apps: Seq[ApplicationInfo]) extends Logging {

  def getAppInfo: Seq[AppInfoProfileResults] = {
    ProfInformationView.getRawView(apps)
  }

  def getAppLogPath: Seq[AppLogPathProfileResults] = {
    ProfLogPathView.getRawView(apps)
  }

  // get rapids-4-spark and cuDF jar if CPU Mode is on.
  def getRapidsJARInfo: Seq[RapidsJarProfileResult] = {
    ProfRapidsJarView.getRawView(apps)
  }

  // get read data schema information
  def getDataSourceInfo: Seq[DataSourceProfileResult] = {
    ProfDataSourceView.getRawView(apps)
  }

  def getDataSourceInfo(
      cachedSqlAccum: Seq[SQLAccumProfileResults]): Seq[DataSourceProfileResult] = {
    ProfDataSourceView.getRawView(apps, cachedSqlAccum)
  }

  // get the write records information
  def getWriteOperationInfo: Seq[WriteOpProfileResult] = {
    ProfWriteOpsView.getRawView(apps)
  }

  // get executor related information
  def getExecutorInfo: Seq[ExecutorInfoProfileResult] = {
    ProfExecutorView.getRawView(apps)
  }

  // get job related information
  def getJobInfo: Seq[JobInfoProfileResult] = {
    ProfJobsView.getRawView(apps)
  }

  def getSQLToStage: Seq[SQLStageInfoProfileResult] = {
    ProfSQLToStageView.getRawView(apps)
  }

  def getRapidsProperties: Seq[RapidsPropertyProfileResult] = {
    RapidsProfPropertiesView.getRawView(apps)
  }
  def getSparkProperties: Seq[RapidsPropertyProfileResult] = {
    SparkProfPropertiesView.getRawView(apps)
  }
  def getSystemProperties: Seq[RapidsPropertyProfileResult] = {
    SystemProfPropertiesView.getRawView(apps)
  }

  // Print SQL whole stage code gen mapping
  def getWholeStageCodeGenMapping: Seq[WholeStageCodeGenResults] = {
    ProfSQLCodeGenView.getRawView(apps)
  }

  // Print SQL Plan Metrics
  def getSQLPlanMetrics: Seq[SQLAccumProfileResults] = {
    ProfSQLPlanMetricsView.getRawView(apps)
  }

  def getSQLPlanInfoTruncated: Seq[SQLPlanInfoProfileResult] = {
    ProfAppSQLPlanInfoView.getRawView(apps)
  }

  // Print all Stage level Metrics
  def getStageLevelMetrics: Seq[AccumProfileResults] = {
    ProfStageMetricView.getRawView(apps)
  }

  def getIODiagnosticMetrics: Seq[IODiagnosticResult] = {
    ProfIODiagnosticMetricsView.getRawView(apps)
  }

  /**
   * This function is meant to clean up Delta log execs so that you could align
   * SQL ids between CPU and GPU eventlogs. It attempts to remove any delta log
   * SQL ids. This includes reading checkpoints, delta_log json files,
   * updating Delta state cache/table.
   */
  def getSQLCleanAndAligned: Seq[SQLCleanAndAlignIdsProfileResult] = {
    ProfSQLPlanAlignedView.getRawView(apps)
  }

  def getSparkRapidsInfo: Seq[SparkRapidsBuildInfoEvent] = {
    apps.map(_.sparkRapidsBuildInfo)
  }
}

object CollectInformation extends Logging {

  def generateSQLAccums(apps: Seq[ApplicationInfo]): Seq[SQLAccumProfileResults] = {
    apps.flatMap { app =>
      app.planMetricProcessor.generateSQLAccums()
    }
  }

  def generateStageLevelAccums(apps: Seq[ApplicationInfo]): Seq[AccumProfileResults] = {
    apps.flatMap { app =>
      app.planMetricProcessor.generateStageLevelAccums()
    }
  }

  def printSQLPlans(apps: Seq[ApplicationInfo], outputDir: String): Unit = {
    for (app <- apps) {
      val planFileWriter = new ToolTextFileWriter(s"$outputDir/${app.appId}",
        "planDescriptions.log", "SQL Plan")
      try {
        for ((sqlID, planDesc) <- app.sqlManager.getPhysicalPlans) {
          planFileWriter.write("\n=============================\n")
          planFileWriter.write(s"Plan for SQL ID : $sqlID")
          planFileWriter.write("\n=============================\n")
          planFileWriter.write(planDesc)
        }
      } finally {
        planFileWriter.close()
      }
    }
  }

  // Update processed properties hashmap based on the new rapids related
  // properties for a new application passed in. This will updated the
  // processedProps hashmap in place to make sure each key in the hashmap
  // has the same number of elements in the ArrayBuffer.
  // It handles 3 cases:
  // 1) key in newRapidsRelated already existed in processedProps
  // 2) this app doesn't contain a key in newRapidsRelated that other apps had
  // 3) new key in newRapidsRelated that wasn't in processedProps for the apps already processed
  def addNewProps(newRapidsRelated: Map[String, String],
      processedProps: HashMap[String, ArrayBuffer[String]],
      numApps: Int): Unit = {

    val inter = processedProps.keys.toSeq.intersect(newRapidsRelated.keys.toSeq)
    val existDiff = processedProps.keys.toSeq.diff(inter)
    val newDiff = newRapidsRelated.keys.toSeq.diff(inter)

    // first update intersecting
    inter.foreach { k =>
      // note, this actually updates processProps as it goes
      val appVals = processedProps.getOrElse(k, ArrayBuffer[String]())
      appVals += newRapidsRelated.getOrElse(k, "null")
    }

    // this app doesn't contain a key that was in another app
    existDiff.foreach { k =>
      val appVals = processedProps.getOrElse(k, ArrayBuffer[String]())
      appVals += "null"
    }

    // this app contains a key not in other apps
    newDiff.foreach { k =>
      // we need to fill if some apps didn't have it
      val appVals = ArrayBuffer[String]()
      appVals ++= Seq.fill(numApps - 1)("null")
      appVals += newRapidsRelated.getOrElse(k, "null")

      processedProps.put(k, appVals)
    }
  }
}
