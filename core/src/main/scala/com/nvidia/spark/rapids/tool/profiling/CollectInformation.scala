/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import com.nvidia.spark.rapids.tool.views.{ProfExecutorView, ProfJobsView, ProfSQLCodeGenView, ProfSQLPlanMetricsView, ProfSQLToStageView}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

case class StageMetrics(numTasks: Int, duration: String)

/**
 * CollectInformation mainly gets information based on this event log:
 * Such as executors, parameters, etc.
 */
class CollectInformation(apps: Seq[ApplicationInfo]) extends Logging {

  def getAppInfo: Seq[AppInfoProfileResults] = {
    val allRows = apps.collect {
      case app if app.isAppMetaDefined =>
        val a = app.appMetaData.get
        AppInfoProfileResults(app.index, a.appName, a.appId,
          a.sparkUser,  a.startTime, a.endTime, app.getAppDuration,
          a.getDurationString, app.sparkVersion, app.gpuMode)
    }
    if (allRows.nonEmpty) {
      allRows.sortBy(cols => (cols.appIndex))
    } else {
      Seq.empty
    }
  }

  def getAppLogPath: Seq[AppLogPathProfileResults] = {
    val allRows = apps.collect {
      case app if app.isAppMetaDefined => val a = app.appMetaData.get
      AppLogPathProfileResults(app.index, a.appName, a.appId, app.getEventLogPath)
    }
    if (allRows.nonEmpty) {
      allRows.sortBy(cols => cols.appIndex)
    } else {
      Seq.empty
    }
  }

  // get rapids-4-spark and cuDF jar if CPU Mode is on.
  def getRapidsJARInfo: Seq[RapidsJarProfileResult] = {
    val allRows = apps.flatMap { app =>
      if (app.gpuMode) {
        // Look for rapids-4-spark and cuDF jar in classPathEntries
        val rapidsJars = app.classpathEntries.filterKeys(_ matches ToolUtils.RAPIDS_JAR_REGEX.regex)
        if (rapidsJars.nonEmpty) {
          val cols = rapidsJars.keys.toSeq
          cols.map(jar => RapidsJarProfileResult(app.index, jar))
        } else {
          // Look for the rapids-4-spark and cuDF jars in Spark Properties
          ToolUtils.extractRAPIDSJarsFromProps(app.sparkProperties).map {
            jar => RapidsJarProfileResult(app.index, jar)
          }
        }
      } else {
        Seq.empty
      }
    }
    if (allRows.size > 0) {
      allRows.sortBy(cols => (cols.appIndex, cols.jar))
    } else {
      Seq.empty
    }
  }

  // get read data schema information
  def getDataSourceInfo: Seq[DataSourceProfileResult] = {
    val dataSourceApps = apps.filter(_.dataSourceInfo.nonEmpty)
    val sqlAccums = CollectInformation.generateSQLAccums(dataSourceApps)

    // Metrics to capture from event log to the result
    val buffer_time: String = "buffer time"
    val scan_time = "scan time"
    val data_size = "size of files read"
    val decode_time = "GPU decode time"

    // This is to save the metrics which will be extracted while creating the result.
    case class IoMetrics(
        var buffer_time: Long,
        var scan_time: Long,
        var data_size: Long,
        var decode_time: Long)

    def getIoMetrics(sqlAccums: Seq[SQLAccumProfileResults]): IoMetrics = {
      val finalRes = IoMetrics(0, 0, 0, 0)
      sqlAccums.map(accum => accum.name match {
        case `buffer_time` => finalRes.buffer_time = accum.total
        case `scan_time` => finalRes.scan_time = accum.total
        case `data_size` => finalRes.data_size = accum.total
        case `decode_time` => finalRes.decode_time = accum.total
      })
      finalRes
    }

    val allRows = dataSourceApps.flatMap { app =>
      val appSqlAccums = sqlAccums.filter(sqlAccum => sqlAccum.appIndex == app.index)

      // Filter appSqlAccums to get only required metrics
      val dataSourceMetrics = appSqlAccums.filter(sqlAccum => sqlAccum.name.contains(buffer_time)
        || sqlAccum.name.contains(scan_time) || sqlAccum.name.contains(decode_time)
        || sqlAccum.name.equals(data_size))

      app.dataSourceInfo.map { ds =>
        val sqlIdtoDs = dataSourceMetrics.filter(
          sqlAccum => sqlAccum.sqlID == ds.sqlID && sqlAccum.nodeID == ds.nodeId)
        if (!sqlIdtoDs.isEmpty) {
          val ioMetrics = getIoMetrics(sqlIdtoDs)
          DataSourceProfileResult(app.index, ds.sqlID, ds.nodeId,
            ds.format, ioMetrics.buffer_time, ioMetrics.scan_time, ioMetrics.data_size,
            ioMetrics.decode_time, ds.location, ds.pushedFilters, ds.schema)
        } else {
          DataSourceProfileResult(app.index, ds.sqlID, ds.nodeId,
            ds.format, 0, 0, 0, 0, ds.location, ds.pushedFilters, ds.schema)
        }
      }
    }
    if (allRows.size > 0) {
      allRows.sortBy(cols => (cols.appIndex, cols.sqlID, cols.location, cols.schema))
    } else {
      Seq.empty
    }
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

  /**
   * Print RAPIDS related or all Spark Properties when the propSource is set to "rapids".
   * Note that RAPIDS related properties are not necessarily starting with prefix 'spark.rapids'.
   * This table is inverse of the other tables where the row keys are property keys and the columns
   * are the application values. So column1 would be all the key values for app index 1.
   * @param propSource defines which type of properties to be retrieved the properties from.
   *                   It can be: rapids, spark, or system
   * @return List of properties relevant to the source.
   */
  private def getProperties(propSource: String): Seq[RapidsPropertyProfileResult] = {
    val outputHeaders = ArrayBuffer("propertyName")
    val props = HashMap[String, ArrayBuffer[String]]()
    var numApps = 0
    apps.foreach { app =>
      numApps += 1
      outputHeaders += s"appIndex_${app.index}"
      val propsToKeep = if (propSource.equals("rapids")) {
        app.sparkProperties.filterKeys { ToolUtils.isRapidsPropKey(_) }
      } else if (propSource.equals("spark")) {
        // remove the rapids related ones
        app.sparkProperties.filterKeys(key => !key.contains(ToolUtils.PROPS_RAPIDS_KEY_PREFIX))
      } else {
        // get the system properties
        app.systemProperties
      }
      CollectInformation.addNewProps(propsToKeep, props, numApps)
    }
    val allRows = props.map { case (k, v) => Seq(k) ++ v }.toSeq
    if (allRows.nonEmpty) {
      val resRows = allRows.map(r => RapidsPropertyProfileResult(r(0), outputHeaders, r))
      resRows.sortBy(cols => cols.key)
    } else {
      Seq.empty
    }
  }

  def getRapidsProperties: Seq[RapidsPropertyProfileResult] = {
    getProperties("rapids")
  }
  def getSparkProperties: Seq[RapidsPropertyProfileResult] = {
    getProperties("spark")
  }
  def getSystemProperties: Seq[RapidsPropertyProfileResult] = {
    getProperties("system")
  }

  // Print SQL whole stage code gen mapping
  def getWholeStageCodeGenMapping: Seq[WholeStageCodeGenResults] = {
    ProfSQLCodeGenView.getRawView(apps)
  }

  // Print SQL Plan Metrics
  def getSQLPlanMetrics: Seq[SQLAccumProfileResults] = {
    ProfSQLPlanMetricsView.getRawView(apps)
  }
}

object CollectInformation extends Logging {

  def generateSQLAccums(apps: Seq[ApplicationInfo]): Seq[SQLAccumProfileResults] = {
    apps.flatMap { app =>
      app.planMetricProcessor.generateSQLAccums()
    }
  }

  def printSQLPlans(apps: Seq[ApplicationInfo], outputDir: String): Unit = {
    for (app <- apps) {
      val planFileWriter = new ToolTextFileWriter(s"$outputDir/${app.appId}",
        "planDescriptions.log", "SQL Plan")
      try {
        for ((sqlID, planDesc) <- app.physicalPlanDescription.toSeq.sortBy(_._1)) {
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
