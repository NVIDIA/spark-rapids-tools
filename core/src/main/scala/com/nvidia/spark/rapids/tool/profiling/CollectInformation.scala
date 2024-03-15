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

import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.rapids.tool.{SQLMetricsStats, ToolUtils}
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

case class StageMetrics(numTasks: Int, duration: String)

/**
 * CollectInformation mainly gets information based on this event log:
 * Such as executors, parameters, etc.
 */
class CollectInformation(apps: Seq[ApplicationInfo]) extends Logging {

  def getAppInfo: Seq[AppInfoProfileResults] = {
    val allRows = apps.collect {
      case app if app.appInfo != null => val a = app.appInfo
      AppInfoProfileResults(app.index, a.appName, a.appId,
        a.sparkUser,  a.startTime, a.endTime, a.duration,
        a.durationStr, a.sparkVersion, a.pluginEnabled)
    }
    if (allRows.nonEmpty) {
      allRows.sortBy(cols => (cols.appIndex))
    } else {
      Seq.empty
    }
  }

  def getAppLogPath: Seq[AppLogPathProfileResults] = {
    val allRows = apps.collect {
      case app if app.appInfo != null => val a = app.appInfo
      AppLogPathProfileResults(app.index, a.appName, a.appId, app.eventLogPath)
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
    val dataSourceApps = apps.filter(_.dataSourceInfo.size > 0)
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
    val allRows = apps.flatMap { app =>
      // first see if any executors have different resourceProfile ids
      val groupedExecs = app.executorIdToInfo.groupBy(_._2.resourceProfileId)
      groupedExecs.map { case (rpId, execs) =>
        val rp = app.resourceProfIdToInfo.get(rpId)
        val execMem = rp.map(_.executorResources.get(ResourceProfile.MEMORY)
          .map(_.amount).getOrElse(0L))
        val execGpus = rp.map(_.executorResources.get("gpu")
          .map(_.amount).getOrElse(0L))
        val taskCpus = rp.map(_.taskResources.get(ResourceProfile.CPUS)
          .map(_.amount).getOrElse(0.toDouble))
        val taskGpus = rp.map(_.taskResources.get("gpu").map(_.amount).getOrElse(0.toDouble))
        val execOffHeap = rp.map(_.executorResources.get(ResourceProfile.OFFHEAP_MEM)
          .map(_.amount).getOrElse(0L))

        val numExecutors = execs.size
        val exec = execs.head._2
        // We could print a lot more information here if we decided, more like the Spark UI
        // per executor info.
        ExecutorInfoProfileResult(app.index, rpId, numExecutors,
          exec.totalCores, exec.maxMemory, exec.totalOnHeap,
          exec.totalOffHeap, execMem, execGpus, execOffHeap, taskCpus, taskGpus)
      }
    }

    if (allRows.size > 0) {
      allRows.sortBy(cols => (cols.appIndex, cols.numExecutors))
    } else {
      Seq.empty
    }
  }

  // get job related information
  def getJobInfo: Seq[JobInfoProfileResult] = {
    val allRows = apps.flatMap { app =>
      app.jobIdToInfo.map { case (_, j) =>
        JobInfoProfileResult(app.index, j.jobID, j.stageIds, j.sqlID, j.startTime, j.endTime)
      }
    }
    if (allRows.size > 0) {
      allRows.sortBy(cols => (cols.appIndex, cols.jobID))
    } else {
      Seq.empty
    }
  }

  def getSQLToStage: Seq[SQLStageInfoProfileResult] = {
    val allRows = apps.flatMap { app =>
      app.aggregateSQLStageInfo
    }
    if (allRows.size > 0) {
      case class Reverse[T](t: T)
      implicit def ReverseOrdering[T: Ordering]: Ordering[Reverse[T]] =
        Ordering[T].reverse.on(_.t)

      // intentionally sort this table by the duration to be able to quickly
      // see the stage that took the longest
      allRows.sortBy(cols => (cols.appIndex, Reverse(cols.duration)))
    } else {
      Seq.empty
    }
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
    val allWholeStages = apps.flatMap { app =>
      app.wholeStage
    }
    if (allWholeStages.nonEmpty) {
      allWholeStages.sortBy(cols => (cols.appIndex, cols.sqlID, cols.nodeID))
    } else {
      Seq.empty
    }
  }

  // Print SQL Plan Metrics
  def getSQLPlanMetrics: Seq[SQLAccumProfileResults] = {
    val sqlAccums = CollectInformation.generateSQLAccums(apps)
    if (sqlAccums.nonEmpty) {
      sqlAccums.sortBy(cols => (cols.appIndex, cols.sqlID, cols.nodeID,
        cols.nodeName, cols.accumulatorId, cols.metricType))
    } else {
      Seq.empty
    }
  }
}

object CollectInformation extends Logging {

 // Store (min, median, max, total) for a given metric
  case class statisticsMetrics(min: Long, med:Long, max:Long, total: Long)

  def generateSQLAccums(apps: Seq[ApplicationInfo]): Seq[SQLAccumProfileResults] = {
    val allRows = apps.flatMap { app =>
      app.allSQLMetrics.map { metric =>
        val jobsForSql = app.jobIdToInfo.filter { case (_, jc) =>
          // Avoid getOrElse to reduce memory allocations
          jc.sqlID.isDefined && jc.sqlID.get == metric.sqlID
        }
        val stageIdsForSQL = jobsForSql.flatMap(_._2.stageIds).toSet
        val accumsOpt = app.taskStageAccumMap.get(metric.accumulatorId)
        val taskMax = accumsOpt match {
          case Some(accums) =>
            val filtered = accums.filter { a =>
              stageIdsForSQL.contains(a.stageId)
            }
            // If metricType is size, average or timing, we want to read field `update` value
            // to get the min, median, max, and total. Otherwise, we want to use field `value`.
            if (SQLMetricsStats.hasStats(metric.metricType)) {
              val accumValues = filtered.map(_.update.getOrElse(0L)).sortWith(_ < _)
              if (accumValues.isEmpty) {
                None
              }
              else if (accumValues.length <= 1) {
                Some(statisticsMetrics(0L, 0L, 0L, accumValues.sum))
              } else {
                Some(statisticsMetrics(accumValues(0), accumValues(accumValues.size / 2),
                  accumValues(accumValues.size - 1), accumValues.sum))
              }
            } else {
              val accumValues = filtered.map(_.value.getOrElse(0L))
              if (accumValues.isEmpty) {
                None
              } else {
                Some(statisticsMetrics(0L, 0L, 0L, accumValues.max))
              }
            }
          case None => None
        }

        // local mode driver gets updates
        val driverAccumsOpt = app.driverAccumMap.get(metric.accumulatorId)
        val driverMax = driverAccumsOpt match {
          case Some(accums) =>
            val filtered = accums.filter { a =>
              a.sqlID == metric.sqlID
            }
            val accumValues = filtered.map(_.value).sortWith(_ < _)
            if (accumValues.isEmpty) {
              None
            } else if (accumValues.length <= 1) {
              Some(statisticsMetrics(0L, 0L, 0L, accumValues.sum))
            } else {
              Some(statisticsMetrics(accumValues(0), accumValues(accumValues.size / 2),
                accumValues(accumValues.size - 1), accumValues.sum))
            }
          case None =>
            None
        }

        if (taskMax.isDefined || driverMax.isDefined) {
          val taskInfo = taskMax match {
            case Some(task) => task
            case None => statisticsMetrics(0L, 0L, 0L, 0L)
          }
          val driverInfo = driverMax match {
            case Some(driver) => driver
            case None => statisticsMetrics(0L, 0L, 0L, 0L)
          }

          val max = Math.max(taskInfo.max, driverInfo.max)
          val min = Math.max(taskInfo.min, driverInfo.min)
          val med = Math.max(taskInfo.med, driverInfo.med)
          val total = Math.max(taskInfo.total, driverInfo.total)

          Some(SQLAccumProfileResults(app.index, metric.sqlID,
            metric.nodeID, metric.nodeName, metric.accumulatorId, metric.name,
            min, med, max, total, metric.metricType, metric.stageIds.mkString(",")))
        } else {
          None
        }
      }
    }
    allRows.filter(_.isDefined).map(_.get)
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
