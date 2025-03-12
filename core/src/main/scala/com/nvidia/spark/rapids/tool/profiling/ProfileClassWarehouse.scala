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

import scala.collection.Map

import com.nvidia.spark.rapids.tool.analysis.StatisticsMetrics

import org.apache.spark.resource.{ExecutorResourceRequest, TaskResourceRequest}
import org.apache.spark.sql.rapids.tool.store.AccumMetaRef
import org.apache.spark.sql.rapids.tool.util.{SparkRuntime, StringUtils}

/**
 * This is a warehouse to store all Classes
 * used for profiling and qualification.
 */

trait ProfileResult {
  val outputHeaders: Seq[String]
  def convertToSeq: Seq[String]
  def convertToCSVSeq: Seq[String]
}

case class ExecutorInfoProfileResult(appIndex: Int, resourceProfileId: Int,
    numExecutors: Int, executorCores: Int, maxMem: Long, maxOnHeapMem: Long,
    maxOffHeapMem: Long, executorMemory: Option[Long], numGpusPerExecutor: Option[Long],
    executorOffHeap: Option[Long], taskCpu: Option[Double],
    taskGpu: Option[Double]) extends ProfileResult {

  override val outputHeaders: Seq[String] = {
    Seq("appIndex", "resourceProfileId", "numExecutors", "executorCores",
      "maxMem", "maxOnHeapMem", "maxOffHeapMem", "executorMemory", "numGpusPerExecutor",
      "executorOffHeap", "taskCpu", "taskGpu")
  }
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, resourceProfileId.toString, numExecutors.toString,
      executorCores.toString, maxMem.toString, maxOnHeapMem.toString,
      maxOffHeapMem.toString, executorMemory.map(_.toString).getOrElse(null),
      numGpusPerExecutor.map(_.toString).getOrElse(null),
      executorOffHeap.map(_.toString).getOrElse(null), taskCpu.map(_.toString).getOrElse(null),
      taskGpu.map(_.toString).getOrElse(null))
  }
  override def convertToCSVSeq: Seq[String] = convertToSeq

}

class JobInfoClass(val jobID: Int,
    val stageIds: Seq[Int],
    val sqlID: Option[Long],
    val properties: scala.collection.Map[String, String],
    val startTime: Long,
    var endTime: Option[Long],
    var jobResult: Option[String],
    var failedReason: Option[String],
    var duration: Option[Long],
    var gpuMode: Boolean)

case class JobInfoProfileResult(
    appIndex: Int,
    jobID: Int,
    stageIds: Seq[Int],
    sqlID: Option[Long],
    startTime: Long,
    endTime: Option[Long]) extends ProfileResult {

  override val outputHeaders = {
    Seq("appIndex",
      "jobID",
      "stageIds",
      "sqlID",
      "startTime",
      "endTime")
  }

  override def convertToSeq: Seq[String] = {
    val stageIdStr = s"[${stageIds.mkString(",")}]"
    Seq(appIndex.toString,
      jobID.toString,
      stageIdStr,
      sqlID.map(_.toString).getOrElse(null),
      startTime.toString,
      endTime.map(_.toString).getOrElse(null))
  }

  override def convertToCSVSeq: Seq[String] = {
    val stageIdStr = s"[${stageIds.mkString(",")}]"
    Seq(appIndex.toString,
      jobID.toString,
      StringUtils.reformatCSVString(stageIdStr),
      sqlID.map(_.toString).getOrElse(null),
      startTime.toString,
      endTime.map(_.toString).getOrElse(null))
  }
}

case class SQLCleanAndAlignIdsProfileResult(
    appIndex: Int,
    sqlID: Long) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "sqlID")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString)
  }
  override def convertToCSVSeq: Seq[String] = convertToSeq
}

case class SQLStageInfoProfileResult(
    appIndex: Int,
    sqlID: Long,
    jobID: Int,
    stageId: Int,
    stageAttemptId: Int,
    duration: Option[Long],
    nodeNames: Seq[String]) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "sqlID", "jobID", "stageId",
    "stageAttemptId", "Stage Duration", "SQL Nodes(IDs)")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, jobID.toString, stageId.toString,
      stageAttemptId.toString, duration.map(_.toString).getOrElse(null),
      nodeNames.mkString(","))
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, jobID.toString, stageId.toString,
      stageAttemptId.toString, duration.map(_.toString).getOrElse(null),
      StringUtils.reformatCSVString(nodeNames.mkString(",")))
  }
}

case class RapidsJarProfileResult(appIndex: Int, jar: String)  extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "Rapids4Spark jars")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, jar)
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString, StringUtils.reformatCSVString(jar))
  }
}

case class DataSourceProfileResult(appIndex: Int, sqlID: Long, version: Int, nodeId: Long,
    format: String, buffer_time: Long, scan_time: Long, data_size: Long,
    decode_time: Long, location: String, pushedFilters: String, schema: String,
    dataFilters: String, partitionFilters: String, fromFinalPlan: Boolean)
extends ProfileResult {
  override val outputHeaders =
    Seq("appIndex", "sqlID", "sql_plan_version", "nodeId", "format", "buffer_time", "scan_time",
      "data_size", "decode_time", "location", "pushedFilters", "schema", "data_filters",
      "partition_filters", "from_final_plan")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, version.toString, nodeId.toString, format,
      buffer_time.toString, scan_time.toString, data_size.toString, decode_time.toString,
      location, pushedFilters, schema, dataFilters, partitionFilters, fromFinalPlan.toString)
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, version.toString, nodeId.toString,
      StringUtils.reformatCSVString(format), buffer_time.toString, scan_time.toString,
      data_size.toString, decode_time.toString, StringUtils.reformatCSVString(location),
      StringUtils.reformatCSVString(pushedFilters), StringUtils.reformatCSVString(schema),
      StringUtils.reformatCSVString(dataFilters), StringUtils.reformatCSVString(partitionFilters),
      fromFinalPlan.toString)
  }
}

case class DriverLogUnsupportedOperators(
    operatorName: String, count: Int, reason: String) extends ProfileResult {
  override val outputHeaders = Seq("operatorName", "count", "reason")

  override def convertToSeq: Seq[String] = {
    Seq(operatorName, count.toString, reason)
  }

  override def convertToCSVSeq: Seq[String] = {
    Seq(StringUtils.reformatCSVString(operatorName), count.toString,
      StringUtils.reformatCSVString(reason))
  }
}

// Case class representing status summary information for a particular application.
case class AppStatusResult(
    path: String,
    status: String,
    appId: String = "",
    message: String = "") extends ProfileResult {
  override val outputHeaders: Seq[String] = Seq("Event Log", "Status", "AppID", "Description")

  override def convertToSeq: Seq[String] = {
    Seq(path, status, appId, message)
  }

  override def convertToCSVSeq: Seq[String] = convertToSeq
}

// note that some things might not be set until after sqlMetricsAggregation called
class SQLExecutionInfoClass(
    val sqlID: Long,
    val rootExecutionID: Option[Long],
    val description: String,
    val details: String,
    val startTime: Long,
    var endTime: Option[Long],
    var duration: Option[Long],
    var hasDatasetOrRDD: Boolean,
    var sqlCpuTimePercent: Double = -1) {
  def setDsOrRdd(value: Boolean): Unit = {
    hasDatasetOrRDD = value
  }
}

case class SQLAccumProfileResults(
    appIndex: Int,
    sqlID: Long,
    nodeID: Long,
    nodeName: String,
    accumulatorId: Long,
    name: String,
    min: Long,
    median: Long,
    max: Long,
    total: Long,
    metricType: String,
    stageIds: Set[Int]) extends ProfileResult {

  private val stageIdsStr = stageIds.mkString(",")

  override val outputHeaders = {
    Seq("appIndex",
      "sqlID",
      "nodeID",
      "nodeName",
      "accumulatorId",
      "name",
      "min",
      "median",
      "max",
      "total",
      "metricType",
      "stageIds")
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      sqlID.toString,
      nodeID.toString,
      nodeName,
      accumulatorId.toString,
      name,
      min.toString,
      median.toString,
      max.toString,
      total.toString,
      metricType,
      stageIdsStr)
  }

  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString,
      sqlID.toString,
      nodeID.toString,
      StringUtils.reformatCSVString(nodeName),
      accumulatorId.toString,
      StringUtils.reformatCSVString(name),
      min.toString,
      median.toString,
      max.toString,
      total.toString,
      StringUtils.reformatCSVString(metricType),
      StringUtils.reformatCSVString(stageIdsStr))
  }
}

case class AccumProfileResults(
    appIndex: Int,
    stageId: Int,
    accMetaRef: AccumMetaRef,
    min: Long,
    median: Long,
    max: Long,
    total: Long) extends ProfileResult {

  override val outputHeaders = {
    Seq("appIndex",
      "stageId",
      "accumulatorId",
      "name",
      "min",
      "median",
      "max",
      "total")
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      stageId.toString,
      accMetaRef.id.toString,
      accMetaRef.getName(),
      min.toString,
      median.toString,
      max.toString,
      total.toString)
  }

  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString,
      stageId.toString,
      accMetaRef.id.toString,
      accMetaRef.name.csvValue,
      min.toString,
      median.toString,
      max.toString,
      total.toString)
  }
}

case class ResourceProfileInfoCase(
    val resourceProfileId: Int,
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest])

case class BlockManagerRemovedCase(
    executorId: String, host: String, port: Int, time: Long)

case class BlockManagerRemovedProfileResult(appIndex: Int,
    executorId: String, time: Long) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "executorId", "time")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, executorId, time.toString)
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString, StringUtils.reformatCSVString(executorId), time.toString)
  }
}

case class ExecutorsRemovedProfileResult(appIndex: Int,
    executorId: String, time: Long, reason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "executorId", "time", "reason")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, executorId, time.toString, reason)
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString, StringUtils.reformatCSVString(executorId), time.toString,
      StringUtils.reformatCSVString(reason))
  }
}

case class UnsupportedOpsProfileResult(appIndex: Int,
    sqlID: Long, nodeID: Long, nodeName: String, nodeDescription: String,
    reason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "sqlID", "nodeID", "nodeName",
    "nodeDescription", "reason")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, nodeID.toString, nodeName,
      nodeDescription, reason)
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, nodeID.toString, StringUtils.reformatCSVString(nodeName),
      StringUtils.reformatCSVString(nodeDescription), StringUtils.reformatCSVString(reason))
  }
}

case class AppInfoProfileResults(appIndex: Int, appName: String,
    appId: Option[String], sparkUser: String,
    startTime: Long, endTime: Option[Long], duration: Option[Long],
    durationStr: String, sparkRuntime: SparkRuntime.SparkRuntime, sparkVersion: String,
    pluginEnabled: Boolean)  extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "appName", "appId",
    "sparkUser", "startTime", "endTime", "duration", "durationStr",
    "sparkRuntime", "sparkVersion", "pluginEnabled")

  def endTimeToStr: String = {
    endTime match {
      case Some(t) => t.toString
      case None => ""
    }
  }

  def durToStr: String = {
    duration match {
      case Some(t) => t.toString
      case None => ""
    }
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, appName, appId.getOrElse(""),
      sparkUser, startTime.toString, endTimeToStr, durToStr,
      durationStr, sparkRuntime.toString, sparkVersion, pluginEnabled.toString)
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString, StringUtils.reformatCSVString(appName),
      StringUtils.reformatCSVString(appId.getOrElse("")), StringUtils.reformatCSVString(sparkUser),
      startTime.toString, endTimeToStr, durToStr, StringUtils.reformatCSVString(durationStr),
      StringUtils.reformatCSVString(sparkRuntime.toString),
      StringUtils.reformatCSVString(sparkVersion), pluginEnabled.toString)
  }
}

case class AppLogPathProfileResults(appIndex: Int, appName: String,
    appId: Option[String], eventLogPath: String)  extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "appName", "appId",
    "eventLogPath")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, appName, appId.getOrElse(""),
      eventLogPath)
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString, StringUtils.reformatCSVString(appName),
      StringUtils.reformatCSVString(appId.getOrElse("")),
      StringUtils.reformatCSVString(eventLogPath))
  }
}

case class SQLPlanMetricsCase(
    sqlID: Long,
    name: String,
    accumulatorId: Long,
    metricType: String)

case class SQLMetricInfoCase(
    sqlID: Long,
    name: String,
    accumulatorId: Long,
    metricType: String,
    nodeID: Long,
    nodeName: String,
    nodeDesc: String,
    stageIds: Set[Int])

case class DriverAccumCase(
    sqlID: Long,
    accumulatorId: Long,
    value: Long)

case class UnsupportedSQLPlan(sqlID: Long, nodeID: Long, nodeName: String,
    nodeDesc: String, reason: String)

case class FailedTaskProfileResults(appIndex: Int, stageId: Int, stageAttemptId: Int,
    taskId: Long, taskAttemptId: Int, endReason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "stageId", "stageAttemptId", "taskId",
    "attempt", "failureReason")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, stageId.toString, stageAttemptId.toString, taskId.toString,
      taskAttemptId.toString, StringUtils.renderStr(endReason, doEscapeMetaCharacters = true))
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString, stageId.toString, stageAttemptId.toString, taskId.toString,
      taskAttemptId.toString,
      StringUtils.reformatCSVString(
        StringUtils.renderStr(endReason, doEscapeMetaCharacters = true, maxLength = 0)))
  }
}

case class FailedStagesProfileResults(appIndex: Int, stageId: Int, stageAttemptId: Int,
    name: String, numTasks: Int, endReason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "stageId", "attemptId", "name",
    "numTasks", "failureReason")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, stageId.toString, stageAttemptId.toString,
      name, numTasks.toString,
      StringUtils.renderStr(endReason, doEscapeMetaCharacters = true))
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString, stageId.toString, stageAttemptId.toString,
      StringUtils.reformatCSVString(name), numTasks.toString,
      StringUtils.reformatCSVString(StringUtils.renderStr(endReason, doEscapeMetaCharacters = true,
        maxLength = 0)))
  }
}

case class FailedJobsProfileResults(
    appIndex: Int,
    jobId: Int,
    sqlID: Option[Long],  // sqlID is optional because Jobs might not have a SQL (i.e., RDDs)
    jobResult: String,
    endReason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "jobID", "sqlID", "jobResult", "failureReason")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      jobId.toString,
      sqlID.map(_.toString).getOrElse(null),
      jobResult,
      StringUtils.renderStr(endReason, doEscapeMetaCharacters = true))
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString,
      jobId.toString,
      sqlID.map(_.toString).getOrElse(null),
      StringUtils.reformatCSVString(jobResult),
      StringUtils.reformatCSVString(
        StringUtils.renderStr(endReason, doEscapeMetaCharacters = true, maxLength = 0)))
  }
}

trait BaseJobStageAggTaskMetricsProfileResult extends ProfileResult {
  def appIndex: Int
  def id: Long
  def numTasks: Int
  def duration: Option[Long]
  def diskBytesSpilledSum: Long
  def durationSum: Long
  def durationMax: Long
  def durationMin: Long
  def durationAvg: Double
  def executorCPUTimeSum: Long // milliseconds
  def executorDeserializeCpuTimeSum: Long // milliseconds
  def executorDeserializeTimeSum: Long
  def executorRunTimeSum: Long
  def inputBytesReadSum: Long
  def inputRecordsReadSum: Long
  def jvmGCTimeSum: Long
  def memoryBytesSpilledSum: Long
  def outputBytesWrittenSum: Long
  def outputRecordsWrittenSum: Long
  def peakExecutionMemoryMax: Long
  def resultSerializationTimeSum: Long
  def resultSizeMax: Long
  def srFetchWaitTimeSum: Long
  def srLocalBlocksFetchedSum: Long
  def srcLocalBytesReadSum: Long
  def srRemoteBlocksFetchSum: Long
  def srRemoteBytesReadSum: Long
  def srRemoteBytesReadToDiskSum: Long
  def srTotalBytesReadSum: Long
  def swBytesWrittenSum: Long
  def swRecordsWrittenSum: Long
  def swWriteTimeSum: Long // milliseconds

  def idHeader: String

  override val outputHeaders = {
    Seq("appIndex",
      idHeader,
      "numTasks",
      "Duration",
      "diskBytesSpilled_sum",
      "duration_sum",
      "duration_max",
      "duration_min",
      "duration_avg",
      "executorCPUTime_sum",
      "executorDeserializeCPUTime_sum",
      "executorDeserializeTime_sum",
      "executorRunTime_sum",
      "input_bytesRead_sum",
      "input_recordsRead_sum",
      "jvmGCTime_sum",
      "memoryBytesSpilled_sum",
      "output_bytesWritten_sum",
      "output_recordsWritten_sum",
      "peakExecutionMemory_max",
      "resultSerializationTime_sum",
      "resultSize_max",
      "sr_fetchWaitTime_sum",
      "sr_localBlocksFetched_sum",
      "sr_localBytesRead_sum",
      "sr_remoteBlocksFetched_sum",
      "sr_remoteBytesRead_sum",
      "sr_remoteBytesReadToDisk_sum",
      "sr_totalBytesRead_sum",
      "sw_bytesWritten_sum",
      "sw_recordsWritten_sum",
      "sw_writeTime_sum")
  }

  val durStr = duration match {
    case Some(dur) => dur.toString
    case None => "null"
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      id.toString,
      numTasks.toString,
      durStr,
      diskBytesSpilledSum.toString,
      durationSum.toString,
      durationMax.toString,
      durationMin.toString,
      durationAvg.toString,
      executorCPUTimeSum.toString,
      executorDeserializeCpuTimeSum.toString,
      executorDeserializeTimeSum.toString,
      executorRunTimeSum.toString,
      inputBytesReadSum.toString,
      inputRecordsReadSum.toString,
      jvmGCTimeSum.toString,
      memoryBytesSpilledSum.toString,
      outputBytesWrittenSum.toString,
      outputRecordsWrittenSum.toString,
      peakExecutionMemoryMax.toString,
      resultSerializationTimeSum.toString,
      resultSizeMax.toString,
      srFetchWaitTimeSum.toString,
      srLocalBlocksFetchedSum.toString,
      srcLocalBytesReadSum.toString,
      srRemoteBlocksFetchSum.toString,
      srRemoteBytesReadSum.toString,
      srRemoteBytesReadToDiskSum.toString,
      srTotalBytesReadSum.toString,
      swBytesWrittenSum.toString,
      swRecordsWrittenSum.toString,
      swWriteTimeSum.toString)
  }

  override def convertToCSVSeq: Seq[String] = convertToSeq
}

case class JobAggTaskMetricsProfileResult(
    appIndex: Int,
    id: Long,
    numTasks: Int,
    duration: Option[Long],
    diskBytesSpilledSum: Long,
    durationSum: Long,
    durationMax: Long,
    durationMin: Long,
    durationAvg: Double,
    executorCPUTimeSum: Long, // milliseconds
    executorDeserializeCpuTimeSum: Long, // milliseconds
    executorDeserializeTimeSum: Long,
    executorRunTimeSum: Long,
    inputBytesReadSum: Long,
    inputRecordsReadSum: Long,
    jvmGCTimeSum: Long,
    memoryBytesSpilledSum: Long,
    outputBytesWrittenSum: Long,
    outputRecordsWrittenSum: Long,
    peakExecutionMemoryMax: Long,
    resultSerializationTimeSum: Long,
    resultSizeMax: Long,
    srFetchWaitTimeSum: Long,
    srLocalBlocksFetchedSum: Long,
    srcLocalBytesReadSum: Long,
    srRemoteBlocksFetchSum: Long,
    srRemoteBytesReadSum: Long,
    srRemoteBytesReadToDiskSum: Long,
    srTotalBytesReadSum: Long,
    swBytesWrittenSum: Long,
    swRecordsWrittenSum: Long,
    swWriteTimeSum: Long // milliseconds
  ) extends BaseJobStageAggTaskMetricsProfileResult {
  override def idHeader = "jobId"
}

case class StageAggTaskMetricsProfileResult(
    appIndex: Int,
    id: Long,
    numTasks: Int,
    duration: Option[Long],
    diskBytesSpilledSum: Long,
    durationSum: Long,
    durationMax: Long,
    durationMin: Long,
    durationAvg: Double,
    executorCPUTimeSum: Long, // milliseconds
    executorDeserializeCpuTimeSum: Long, // milliseconds
    executorDeserializeTimeSum: Long,
    executorRunTimeSum: Long,
    inputBytesReadSum: Long,
    inputRecordsReadSum: Long,
    jvmGCTimeSum: Long,
    memoryBytesSpilledSum: Long,
    outputBytesWrittenSum: Long,
    outputRecordsWrittenSum: Long,
    peakExecutionMemoryMax: Long,
    resultSerializationTimeSum: Long,
    resultSizeMax: Long,
    srFetchWaitTimeSum: Long,
    srLocalBlocksFetchedSum: Long,
    srcLocalBytesReadSum: Long,
    srRemoteBlocksFetchSum: Long,
    srRemoteBytesReadSum: Long,
    srRemoteBytesReadToDiskSum: Long,
    srTotalBytesReadSum: Long,
    swBytesWrittenSum: Long,
    swRecordsWrittenSum: Long,
    swWriteTimeSum: Long // milliseconds
  ) extends BaseJobStageAggTaskMetricsProfileResult {

  /**
   * Combines two StageAggTaskMetricsProfileResults for the same stage.
   * This method aggregates the metrics from the current instance and the provided `other` instance.
   *
   * Detailed explanation ->
   * 1. A stage can have two successful attempts.
   * 2. We store both of those attempt information using the StageManager
   * 3. During aggregation, we combine the metrics for a stage at a stageID
   *    level
   * 4. For combining aggregated information for multiple stage attempts, we combine the
   *    aggregated per attempt information into one using the below method
   *
   * @param other The StageAggTaskMetricsProfileResult to be combined with the current instance.
   * @return A new StageAggTaskMetricsProfileResult with aggregated metrics.
   */
  def aggregateStageProfileMetric(
      other: StageAggTaskMetricsProfileResult
  ): StageAggTaskMetricsProfileResult = {
    StageAggTaskMetricsProfileResult(
      appIndex = this.appIndex,
      id = this.id,
      numTasks = this.numTasks + other.numTasks,
      duration = Option(this.duration.getOrElse(0L) + other.duration.getOrElse(0L)),
      diskBytesSpilledSum = this.diskBytesSpilledSum + other.diskBytesSpilledSum,
      durationSum = this.durationSum + other.durationSum,
      durationMax = Math.max(this.durationMax, other.durationMax),
      durationMin = Math.min(this.durationMin, other.durationMin),
      durationAvg = (this.durationAvg + other.durationAvg) / 2,
      executorCPUTimeSum = this.executorCPUTimeSum + other.executorCPUTimeSum,
      executorDeserializeCpuTimeSum = this.executorDeserializeCpuTimeSum +
        other.executorDeserializeCpuTimeSum,
      executorDeserializeTimeSum = this.executorDeserializeTimeSum +
        other.executorDeserializeTimeSum,
      executorRunTimeSum = this.executorRunTimeSum + other.executorRunTimeSum,
      inputBytesReadSum = this.inputBytesReadSum + other.inputBytesReadSum,
      inputRecordsReadSum = this.inputRecordsReadSum + other.inputRecordsReadSum,
      jvmGCTimeSum = this.jvmGCTimeSum + other.jvmGCTimeSum,
      memoryBytesSpilledSum = this.memoryBytesSpilledSum + other.memoryBytesSpilledSum,
      outputBytesWrittenSum = this.outputBytesWrittenSum + other.outputBytesWrittenSum,
      outputRecordsWrittenSum = this.outputRecordsWrittenSum + other.outputRecordsWrittenSum,
      peakExecutionMemoryMax = Math.max(this.peakExecutionMemoryMax, other.peakExecutionMemoryMax),
      resultSerializationTimeSum = this.resultSerializationTimeSum +
        other.resultSerializationTimeSum,
      resultSizeMax = Math.max(this.resultSizeMax, other.resultSizeMax),
      srFetchWaitTimeSum = this.srFetchWaitTimeSum + other.srFetchWaitTimeSum,
      srLocalBlocksFetchedSum = this.srLocalBlocksFetchedSum + other.srLocalBlocksFetchedSum,
      srRemoteBlocksFetchSum = this.srRemoteBlocksFetchSum + other.srRemoteBlocksFetchSum,
      srRemoteBytesReadSum = this.srRemoteBytesReadSum + other.srRemoteBytesReadSum,
      srRemoteBytesReadToDiskSum = this.srRemoteBytesReadToDiskSum +
        other.srRemoteBytesReadToDiskSum,
      srTotalBytesReadSum = this.srTotalBytesReadSum + other.srTotalBytesReadSum,
      srcLocalBytesReadSum = this.srcLocalBytesReadSum + other.srcLocalBytesReadSum,
      swBytesWrittenSum = this.swBytesWrittenSum + other.swBytesWrittenSum,
      swRecordsWrittenSum = this.swRecordsWrittenSum + other.swRecordsWrittenSum,
      swWriteTimeSum = this.swWriteTimeSum + other.swWriteTimeSum
    )
  }

  override def idHeader = "stageId"
}

/**
 * Represents diagnostic metrics results at task/stage level in a Spark SQL execution plan.
 * Output file: stage_level_diagnostic_metrics.csv.
 * Collected metrics include:
 * - Memory spilled (MB)
 * - Disk spilled (MB)
 * - Input bytes read
 * - Output bytes written
 * - Shuffle read total bytes (remote + local)
 * - Shuffle write bytes
 * - Shuffle read fetch wait time (ms)
 * - Shuffle write time (ms)
 * - GPU semaphore time (ns)
 */
case class StageDiagnosticResult(
    appIndex: Int,
    appName: String,
    appId: String,
    stageId: Long,
    duration: Option[Long],
    numTasks: Int,
    srTotalBytesReadMin: Long,
    srTotalBytesReadMed: Long,
    srTotalBytesReadMax: Long,
    srTotalBytesReadSum: Long,
    memoryBytesSpilled: AccumProfileResults,
    diskBytesSpilled: AccumProfileResults,
    inputBytesRead: AccumProfileResults,
    outputBytesWritten: AccumProfileResults,
    swBytesWritten: AccumProfileResults,
    srFetchWaitTime: AccumProfileResults,
    swWriteTime: AccumProfileResults,
    gpuSemaphoreWait: AccumProfileResults,
    nodeNames: Seq[String]) extends ProfileResult {

  def bytesToMB(numBytes: Long): Long = numBytes / (1024 * 1024)

  def nanoToMilliSec(numNano: Long): Long = numNano / 1000000

  val durStr = duration match {
    case Some(dur) => dur.toString
    case None => "null"
  }

  override val outputHeaders = {
    Seq("appIndex",
      "appName",
      "appId",
      "stageId",
      "stageDurationMs",
      "numTasks",
      "memoryBytesSpilledMBMin",
      "memoryBytesSpilledMBMedian",
      "memoryBytesSpilledMBMax",
      "memoryBytesSpilledMBTotal",
      "diskBytesSpilledMBMin",
      "diskBytesSpilledMBMedian",
      "diskBytesSpilledMBMax",
      "diskBytesSpilledMBTotal",
      "inputBytesReadMin",
      "inputBytesReadMedian",
      "inputBytesReadMax",
      "inputBytesReadTotal",
      "outputBytesWrittenMin",
      "outputBytesWrittenMedian",
      "outputBytesWrittenMax",
      "outputBytesWrittenTotal",
      "shuffleReadBytesMin",
      "shuffleReadBytesMedian",
      "shuffleReadBytesMax",
      "shuffleReadBytesTotal",
      "shuffleWriteBytesMin",
      "shuffleWriteBytesMedian",
      "shuffleWriteBytesMax",
      "shuffleWriteBytesTotal",
      "shuffleReadFetchWaitTimeMin",
      "shuffleReadFetchWaitTimeMedian",
      "shuffleReadFetchWaitTimeMax",
      "shuffleReadFetchWaitTimeTotal",
      "shuffleWriteWriteTimeMin",
      "shuffleWriteWriteTimeMedian",
      "shuffleWriteWriteTimeMax",
      "shuffleWriteWriteTimeTotal",
      "gpuSemaphoreWaitTimeTotal",
      "SQL Nodes(IDs)")
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      appName,
      appId,
      stageId.toString,
      durStr,
      numTasks.toString,
      bytesToMB(memoryBytesSpilled.min).toString,
      bytesToMB(memoryBytesSpilled.median).toString,
      bytesToMB(memoryBytesSpilled.max).toString,
      bytesToMB(memoryBytesSpilled.total).toString,
      bytesToMB(diskBytesSpilled.min).toString,
      bytesToMB(diskBytesSpilled.median).toString,
      bytesToMB(diskBytesSpilled.max).toString,
      bytesToMB(diskBytesSpilled.total).toString,
      inputBytesRead.min.toString,
      inputBytesRead.median.toString,
      inputBytesRead.max.toString,
      inputBytesRead.total.toString,
      outputBytesWritten.min.toString,
      outputBytesWritten.median.toString,
      outputBytesWritten.max.toString,
      outputBytesWritten.total.toString,
      srTotalBytesReadMin.toString,
      srTotalBytesReadMed.toString,
      srTotalBytesReadMax.toString,
      srTotalBytesReadSum.toString,
      swBytesWritten.min.toString,
      swBytesWritten.median.toString,
      swBytesWritten.max.toString,
      swBytesWritten.total.toString,
      nanoToMilliSec(srFetchWaitTime.min).toString,
      nanoToMilliSec(srFetchWaitTime.median).toString,
      nanoToMilliSec(srFetchWaitTime.max).toString,
      nanoToMilliSec(srFetchWaitTime.total).toString,
      nanoToMilliSec(swWriteTime.min).toString,
      nanoToMilliSec(swWriteTime.median).toString,
      nanoToMilliSec(swWriteTime.max).toString,
      nanoToMilliSec(swWriteTime.total).toString,
      gpuSemaphoreWait.total.toString,
      nodeNames.mkString(","))
  }

  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString,
      appName,
      appId,
      stageId.toString,
      durStr,
      numTasks.toString,
      bytesToMB(memoryBytesSpilled.min).toString,
      bytesToMB(memoryBytesSpilled.median).toString,
      bytesToMB(memoryBytesSpilled.max).toString,
      bytesToMB(memoryBytesSpilled.total).toString,
      bytesToMB(diskBytesSpilled.min).toString,
      bytesToMB(diskBytesSpilled.median).toString,
      bytesToMB(diskBytesSpilled.max).toString,
      bytesToMB(diskBytesSpilled.total).toString,
      inputBytesRead.min.toString,
      inputBytesRead.median.toString,
      inputBytesRead.max.toString,
      inputBytesRead.total.toString,
      outputBytesWritten.min.toString,
      outputBytesWritten.median.toString,
      outputBytesWritten.max.toString,
      outputBytesWritten.total.toString,
      srTotalBytesReadMin.toString,
      srTotalBytesReadMed.toString,
      srTotalBytesReadMax.toString,
      srTotalBytesReadSum.toString,
      swBytesWritten.min.toString,
      swBytesWritten.median.toString,
      swBytesWritten.max.toString,
      swBytesWritten.total.toString,
      nanoToMilliSec(srFetchWaitTime.min).toString,
      nanoToMilliSec(srFetchWaitTime.median).toString,
      nanoToMilliSec(srFetchWaitTime.max).toString,
      nanoToMilliSec(srFetchWaitTime.total).toString,
      nanoToMilliSec(swWriteTime.min).toString,
      nanoToMilliSec(swWriteTime.median).toString,
      nanoToMilliSec(swWriteTime.max).toString,
      nanoToMilliSec(swWriteTime.total).toString,
      gpuSemaphoreWait.total.toString,
      StringUtils.reformatCSVString(nodeNames.mkString(",")))
  }
}

case class SQLMaxTaskInputSizes(
    appIndex: Int,
    appId: String,
    // Not added to the output since it is used only by the AutoTuner
    maxTaskInputBytesRead: Double
)

case class SQLTaskAggMetricsProfileResult(
    appIndex: Int,
    appId: String,
    sqlId: Long,
    description: String,
    numTasks: Int,
    duration: Option[Long],
    executorCpuRatio: Double,
    diskBytesSpilledSum: Long,
    durationSum: Long,
    durationMax: Long,
    durationMin: Long,
    durationAvg: Double,
    executorCPUTimeSum: Long, // milliseconds
    executorDeserializeCpuTimeSum: Long, // milliseconds
    executorDeserializeTimeSum: Long,
    executorRunTimeSum: Long,
    inputBytesReadSum: Long,
    // Not added to the output since it is used only by the AutoTuner
    inputBytesReadAvg: Double,
    inputRecordsReadSum: Long,
    jvmGCTimeSum: Long,
    memoryBytesSpilledSum: Long,
    outputBytesWrittenSum: Long,
    outputRecordsWrittenSum: Long,
    peakExecutionMemoryMax: Long,
    resultSerializationTimeSum: Long,
    resultSizeMax: Long,
    srFetchWaitTimeSum: Long,
    srLocalBlocksFetchedSum: Long,
    srcLocalBytesReadSum: Long,
    srRemoteBlocksFetchSum: Long,
    srRemoteBytesReadSum: Long,
    srRemoteBytesReadToDiskSum: Long,
    srTotalBytesReadSum: Long,
    swBytesWrittenSum: Long,
    swRecordsWrittenSum: Long,
    swWriteTimeSum: Long // milliseconds
  ) extends ProfileResult {

  override val outputHeaders = {
    Seq("appIndex",
      "appID",
      "sqlID",
      "description",
      "numTasks",
      "Duration",
      "executorCPURatio",
      "diskBytesSpilled_sum",
      "duration_sum",
      "duration_max",
      "duration_min",
      "duration_avg",
      "executorCPUTime_sum",
      "executorDeserializeCPUTime_sum",
      "executorDeserializeTime_sum",
      "executorRunTime_sum",
      "input_bytesRead_sum",
      "input_recordsRead_sum",
      "jvmGCTime_sum",
      "memoryBytesSpilled_sum",
      "output_bytesWritten_sum",
      "output_recordsWritten_sum",
      "peakExecutionMemory_max",
      "resultSerializationTime_sum",
      "resultSize_max",
      "sr_fetchWaitTime_sum",
      "sr_localBlocksFetched_sum",
      "sr_localBytesRead_sum",
      "sr_remoteBlocksFetched_sum",
      "sr_remoteBytesRead_sum",
      "sr_remoteBytesReadToDisk_sum",
      "sr_totalBytesRead_sum",
      "sw_bytesWritten_sum",
      "sw_recordsWritten_sum",
      "sw_writeTime_sum")
  }

  val durStr = duration match {
    case Some(dur) => dur.toString
    case None => ""
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      appId,
      sqlId.toString,
      description,
      numTasks.toString,
      durStr,
      executorCpuRatio.toString,
      diskBytesSpilledSum.toString,
      durationSum.toString,
      durationMax.toString,
      durationMin.toString,
      durationAvg.toString,
      executorCPUTimeSum.toString,
      executorDeserializeCpuTimeSum.toString,
      executorDeserializeTimeSum.toString,
      executorRunTimeSum.toString,
      inputBytesReadSum.toString,
      inputRecordsReadSum.toString,
      jvmGCTimeSum.toString,
      memoryBytesSpilledSum.toString,
      outputBytesWrittenSum.toString,
      outputRecordsWrittenSum.toString,
      peakExecutionMemoryMax.toString,
      resultSerializationTimeSum.toString,
      resultSizeMax.toString,
      srFetchWaitTimeSum.toString,
      srLocalBlocksFetchedSum.toString,
      srcLocalBytesReadSum.toString,
      srRemoteBlocksFetchSum.toString,
      srRemoteBytesReadSum.toString,
      srRemoteBytesReadToDiskSum.toString,
      srTotalBytesReadSum.toString,
      swBytesWrittenSum.toString,
      swRecordsWrittenSum.toString,
      swWriteTimeSum.toString)
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString,
      StringUtils.reformatCSVString(appId),
      sqlId.toString,
      StringUtils.reformatCSVString(description),
      numTasks.toString,
      durStr,
      executorCpuRatio.toString,
      diskBytesSpilledSum.toString,
      durationSum.toString,
      durationMax.toString,
      durationMin.toString,
      durationAvg.toString,
      executorCPUTimeSum.toString,
      executorDeserializeCpuTimeSum.toString,
      executorDeserializeTimeSum.toString,
      executorRunTimeSum.toString,
      inputBytesReadSum.toString,
      inputRecordsReadSum.toString,
      jvmGCTimeSum.toString,
      memoryBytesSpilledSum.toString,
      outputBytesWrittenSum.toString,
      outputRecordsWrittenSum.toString,
      peakExecutionMemoryMax.toString,
      resultSerializationTimeSum.toString,
      resultSizeMax.toString,
      srFetchWaitTimeSum.toString,
      srLocalBlocksFetchedSum.toString,
      srcLocalBytesReadSum.toString,
      srRemoteBlocksFetchSum.toString,
      srRemoteBytesReadSum.toString,
      srRemoteBytesReadToDiskSum.toString,
      srTotalBytesReadSum.toString,
      swBytesWrittenSum.toString,
      swRecordsWrittenSum.toString,
      swWriteTimeSum.toString)
  }
}

/**
 * Represents IO-related diagnostic metrics results in a Spark SQL execution plan.
 * Output file: io_diagnostic_metrics.csv.
 * Collected metrics include:
 * - Output rows
 * - Scan time (ns)
 * - Output batches
 * - Buffer time (ns)
 * - Shuffle write time (ns)
 * - Fetch wait time (ns)
 * - GPU decode time (ns)
 */
case class IODiagnosticResult(
    appIndex: Int,
    appName: String,
    appId: String,
    sqlId: Long,
    stageId: Long,
    duration: Long,
    nodeId: Long,
    nodeName: String,
    outputRows: StatisticsMetrics,
    scanTime: StatisticsMetrics,
    outputBatches: StatisticsMetrics,
    bufferTime: StatisticsMetrics,
    shuffleWriteTime: StatisticsMetrics,
    fetchWaitTime: StatisticsMetrics,
    gpuDecodeTime: StatisticsMetrics) extends ProfileResult {

  override val outputHeaders = {
    Seq("appIndex",
      "appName",
      "appId",
      "sqlId",
      "stageId",
      "stageDurationMs",
      "nodeId",
      "nodeName",
      "outputRowsMin",
      "outputRowsMedian",
      "outputRowsMax",
      "outputRowsTotal",
      "scanTimeMin",
      "scanTimeMedian",
      "scanTimeMax",
      "scanTimeTotal",
      "outputBatchesMin",
      "outputBatchesMedian",
      "outputBatchesMax",
      "outputBatchesTotal",
      "bufferTimeMin",
      "bufferTimeMedian",
      "bufferTimeMax",
      "bufferTimeTotal",
      "shuffleWriteTimeMin",
      "shuffleWriteTimeMedian",
      "shuffleWriteTimeMax",
      "shuffleWriteTimeTotal",
      "fetchWaitTimeMin",
      "fetchWaitTimeMedian",
      "fetchWaitTimeMax",
      "fetchWaitTimeTotal",
      "gpuDecodeTimeMin",
      "gpuDecodeTimeMedian",
      "gpuDecodeTimeMax",
      "gpuDecodeTimeTotal")
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      appName,
      appId,
      sqlId.toString,
      stageId.toString,
      duration.toString,
      nodeId.toString,
      nodeName,
      outputRows.min.toString,
      outputRows.med.toString,
      outputRows.max.toString,
      outputRows.total.toString,
      scanTime.min.toString,
      scanTime.med.toString,
      scanTime.max.toString,
      scanTime.total.toString,
      outputBatches.min.toString,
      outputBatches.med.toString,
      outputBatches.max.toString,
      outputBatches.total.toString,
      bufferTime.min.toString,
      bufferTime.med.toString,
      bufferTime.max.toString,
      bufferTime.total.toString,
      shuffleWriteTime.min.toString,
      shuffleWriteTime.med.toString,
      shuffleWriteTime.max.toString,
      shuffleWriteTime.total.toString,
      fetchWaitTime.min.toString,
      fetchWaitTime.med.toString,
      fetchWaitTime.max.toString,
      fetchWaitTime.total.toString,
      gpuDecodeTime.min.toString,
      gpuDecodeTime.med.toString,
      gpuDecodeTime.max.toString,
      gpuDecodeTime.total.toString)
  }

  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString,
      appName,
      appId,
      sqlId.toString,
      stageId.toString,
      duration.toString,
      nodeId.toString,
      StringUtils.reformatCSVString(nodeName),
      outputRows.min.toString,
      outputRows.med.toString,
      outputRows.max.toString,
      outputRows.total.toString,
      scanTime.min.toString,
      scanTime.med.toString,
      scanTime.max.toString,
      scanTime.total.toString,
      outputBatches.min.toString,
      outputBatches.med.toString,
      outputBatches.max.toString,
      outputBatches.total.toString,
      bufferTime.min.toString,
      bufferTime.med.toString,
      bufferTime.max.toString,
      bufferTime.total.toString,
      shuffleWriteTime.min.toString,
      shuffleWriteTime.med.toString,
      shuffleWriteTime.max.toString,
      shuffleWriteTime.total.toString,
      fetchWaitTime.min.toString,
      fetchWaitTime.med.toString,
      fetchWaitTime.max.toString,
      fetchWaitTime.total.toString,
      gpuDecodeTime.min.toString,
      gpuDecodeTime.med.toString,
      gpuDecodeTime.max.toString,
      gpuDecodeTime.total.toString)
  }
}

case class IOAnalysisProfileResult(
    appIndex: Int,
    appId: String,
    sqlId: Long,
    inputBytesReadSum: Long,
    inputRecordsReadSum: Long,
    outputBytesWrittenSum: Long,
    outputRecordsWrittenSum: Long,
    diskBytesSpilledSum: Long,
    memoryBytesSpilledSum: Long,
    srTotalBytesReadSum: Long,
    swTotalBytesWriteSum: Long) extends ProfileResult {

  override val outputHeaders = Seq("appIndex", "appID", "sqlID", "input_bytesRead_sum",
    "input_recordsRead_sum", "output_bytesWritten_sum", "output_recordsWritten_sum",
    "diskBytesSpilled_sum", "memoryBytesSpilled_sum", "sr_totalBytesRead_sum",
    "sw_bytesWritten_sum")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      appId,
      sqlId.toString,
      inputBytesReadSum.toString,
      inputRecordsReadSum.toString,
      outputBytesWrittenSum.toString,
      outputRecordsWrittenSum.toString,
      diskBytesSpilledSum.toString,
      memoryBytesSpilledSum.toString,
      srTotalBytesReadSum.toString,
      swTotalBytesWriteSum.toString)
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString,
      StringUtils.reformatCSVString(appId),
      sqlId.toString,
      inputBytesReadSum.toString,
      inputRecordsReadSum.toString,
      outputBytesWrittenSum.toString,
      outputRecordsWrittenSum.toString,
      diskBytesSpilledSum.toString,
      memoryBytesSpilledSum.toString,
      srTotalBytesReadSum.toString,
      swTotalBytesWriteSum.toString)
  }
}

case class SQLDurationExecutorTimeProfileResult(
    appIndex: Int,
    appId: String,
    rootsqlID: Option[Long],
    sqlID: Long,
    duration: Option[Long],
    containsDataset: Boolean,
    appDuration: Option[Long],
    potentialProbs: String,
    executorCpuRatio: Double) extends ProfileResult {
  override val outputHeaders = {
    Seq("appIndex",
      "App ID",
      "RootSqlID",
      "sqlID",
      "SQL Duration",
      "Contains Dataset or RDD Op",
      "App Duration",
      "Potential Problems",
      "Executor CPU Time Percent")
  }
  val durStr = duration match {
    case Some(dur) => dur.toString
    case None => ""
  }
  val appDurStr = appDuration match {
    case Some(dur) => dur.toString
    case None => ""
  }
  val execCpuTimePercent = if (executorCpuRatio == -1.0) {
    "null"
  } else {
    executorCpuRatio.toString
  }
  val potentialStr = if (potentialProbs.isEmpty) {
    "null"
  } else {
    potentialProbs
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      rootsqlID.getOrElse("").toString,
      appId,
      sqlID.toString,
      durStr,
      containsDataset.toString,
      appDurStr,
      potentialStr,
      execCpuTimePercent)
  }

  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString,
      StringUtils.reformatCSVString(appId),
      rootsqlID.getOrElse("").toString,
      sqlID.toString,
      durStr,
      containsDataset.toString,
      appDurStr,
      StringUtils.reformatCSVString(potentialStr),
      execCpuTimePercent)
  }
}

case class ShuffleSkewProfileResult(appIndex: Int, stageId: Long, stageAttemptId: Long,
    taskId: Long, taskAttemptId: Long, taskDuration: Long, avgDuration: Double,
    taskShuffleReadMB: Long, avgShuffleReadMB: Double, taskPeakMemoryMB: Long,
    successful: Boolean, reason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "stageId", "stageAttemptId", "taskId", "attempt",
    "taskDurationSec", "avgDurationSec", "taskShuffleReadMB", "avgShuffleReadMB",
    "taskPeakMemoryMB", "successful", "reason")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      stageId.toString,
      stageAttemptId.toString,
      taskId.toString,
      taskAttemptId.toString,
      f"${taskDuration.toDouble / 1000}%1.2f",
      f"${avgDuration / 1000}%1.1f",
      f"${taskShuffleReadMB.toDouble / 1024 / 1024}%1.2f",
      f"${avgShuffleReadMB / 1024 / 1024}%1.2f",
      f"${taskPeakMemoryMB.toDouble / 1024 / 1024}%1.2f",
      successful.toString,
      StringUtils.renderStr(reason, doEscapeMetaCharacters = true))
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString,
      stageId.toString,
      stageAttemptId.toString,
      taskId.toString,
      taskAttemptId.toString,
      f"${taskDuration.toDouble / 1000}%1.2f",
      f"${avgDuration / 1000}%1.1f",
      f"${taskShuffleReadMB.toDouble / 1024 / 1024}%1.2f",
      f"${avgShuffleReadMB / 1024 / 1024}%1.2f",
      f"${taskPeakMemoryMB.toDouble / 1024 / 1024}%1.2f",
      successful.toString,
      // truncate this field because it might be a very long string and it is already
      // fully displayed in failed_* files
      StringUtils.reformatCSVString(
        StringUtils.renderStr(reason, doEscapeMetaCharacters = true)))
  }
}

case class RapidsPropertyProfileResult(key: String, outputHeadersIn: Seq[String],
    rows: Seq[String]) extends ProfileResult {

  override val outputHeaders: Seq[String] = outputHeadersIn
  override def convertToSeq: Seq[String] = rows
  override def convertToCSVSeq: Seq[String] = {
    rows.map(StringUtils.reformatCSVString(_))
  }
}

case class CompareProfileResults(outputHeadersIn: Seq[String],
    rows: Seq[String]) extends ProfileResult {

  override val outputHeaders: Seq[String] = outputHeadersIn
  override def convertToSeq: Seq[String] = rows
  override def convertToCSVSeq: Seq[String] = {
    rows.map(StringUtils.reformatCSVString(_))
  }
}

case class WholeStageCodeGenResults(
    appIndex: Int,
    sqlID: Long,
    nodeID: Long,
    parent: String,
    child: String,
    childNodeID: Long
) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "sqlID", "nodeID", "SQL Node",
    "Child Node", "Child NodeID")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      sqlID.toString,
      nodeID.toString,
      parent,
      child,
      childNodeID.toString)
  }
  override def convertToCSVSeq: Seq[String] = {
    Seq(appIndex.toString,
      sqlID.toString,
      nodeID.toString,
      StringUtils.reformatCSVString(parent),
      StringUtils.reformatCSVString(child),
      childNodeID.toString)
  }
}

case class RecommendedPropertyResult(property: String, value: String) {
  override def toString: String = "--conf %s=%s".format(property, value)
}

case class RecommendedCommentResult(comment: String) {
  override def toString: String = "- %s".format(comment)
}

/**
 * Helper object to store the list of SparkRapids OOM exceptions.
 */
object SparkRapidsOomExceptions {
  val gpuExceptionClassNames: Set[String] = {
    Set("GpuSplitAndRetryOOM", "GpuRetryOOM")
  }
}
