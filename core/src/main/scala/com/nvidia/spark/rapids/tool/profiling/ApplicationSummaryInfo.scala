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

import com.nvidia.spark.rapids.SparkRapidsBuildInfoEvent
import com.nvidia.spark.rapids.tool.AppSummaryInfoBaseProvider
import com.nvidia.spark.rapids.tool.tuning.{SparkMaster, Yarn}
import com.nvidia.spark.rapids.tool.views.{IoMetrics, WriteOpProfileResult}

import org.apache.spark.ExecutorLostFailure
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

case class ApplicationSummaryInfo(
    appInfo: Seq[AppInfoProfileResults],
    dsInfo: Seq[DataSourceProfileResult],
    execInfo: Seq[ExecutorInfoProfileResult],
    jobInfo: Seq[JobInfoProfileResult],
    rapidsProps: Seq[RapidsPropertyProfileResult],
    rapidsJar: Seq[RapidsJarProfileResult],
    sqlMetrics: Seq[SQLAccumProfileResults],
    stageMetrics: Seq[AccumProfileResults],
    jobAggMetrics: Seq[JobAggTaskMetricsProfileResult],
    stageAggMetrics: Seq[StageAggTaskMetricsProfileResult],
    sqlTaskAggMetrics: Seq[SQLTaskAggMetricsProfileResult],
    durAndCpuMet: Seq[SQLDurationExecutorTimeProfileResult],
    skewInfo: Seq[ShuffleSkewProfileResult],
    failedTasks: Seq[FailedTaskProfileResults],
    failedStages: Seq[FailedStagesProfileResults],
    failedJobs: Seq[FailedJobsProfileResults],
    removedBMs: Seq[BlockManagerRemovedProfileResult],
    removedExecutors: Seq[ExecutorsRemovedProfileResult],
    unsupportedOps: Seq[UnsupportedOpsProfileResult],
    sparkProps: Seq[RapidsPropertyProfileResult],
    sqlStageInfo: Seq[SQLStageInfoProfileResult],
    wholeStage: Seq[WholeStageCodeGenResults],
    maxTaskInputBytesRead: Seq[SQLMaxTaskInputSizes],
    appLogPath: Seq[AppLogPathProfileResults],
    ioMetrics: Seq[IOAnalysisProfileResult],
    sysProps: Seq[RapidsPropertyProfileResult],
    sqlCleanedAlignedIds: Seq[SQLCleanAndAlignIdsProfileResult],
    sparkRapidsBuildInfo: Seq[SparkRapidsBuildInfoEvent],
    writeOpsInfo: Seq[WriteOpProfileResult],
    sqlPlanInfo: Seq[SQLPlanInfoProfileResult])

trait AppInfoPropertyGetter {
  // returns all the properties (i.e., spark)
  def getAllProperties: Map[String, String]
  def getSparkProperty(propKey: String): Option[String]
  def getRapidsProperty(propKey: String): Option[String]
  def getSystemProperty(propKey: String): Option[String]
  def getProperty(propKey: String): Option[String]
  def getSparkVersion: Option[String]
  def getRapidsJars: Seq[String]
}

trait AppInfoSqlTaskAggMetricsVisitor {
  def getJvmGCFractions: Seq[Double]
  def getSpilledMetrics: Seq[Long]
}

trait AppInfoJobStageAggMetricsVisitor {
  def getShuffleStagesWithPosSpilling: Set[Long]
  def getShuffleSkewStages: Set[Long]
}

trait AppInfoSQLTaskInputSizes {
  def getMaxInput: Double
  def getMeanInput: Double
  def getMeanShuffleRead: Double
}

trait AppInfoReadMetrics {
  def getDistinctLocationPct: Double
  def getRedundantReadSize: Long
}

trait AppInfoGpuOomCheck {
  def hasScanStagesWithGpuOom: Boolean = false
  def hasShuffleStagesWithOom: Boolean = false
}

/**
 * Base class for Profiling App Summary Info Provider.
 */
class BaseProfilingAppSummaryInfoProvider
  extends AppSummaryInfoBaseProvider with AppInfoGpuOomCheck

/**
 * A wrapper class to process the information embedded in a valid instance of
 * [[ApplicationSummaryInfo]].
 * Note that this class does not handle combined mode and assume that the profiling results belong
 * to a single app.
 * @param appInfo Info object resulting from profiling a single app.
 * @param app Summary object resulting from profiling a single app.
 */
class SingleAppSummaryInfoProvider(
    val appInfo: ApplicationInfo,
    val app: ApplicationSummaryInfo)
  extends BaseProfilingAppSummaryInfoProvider {

  private lazy val distinctLocations = app.dsInfo.groupBy(_.location)
  override def isAppInfoAvailable: Boolean = Option(app).isDefined

  private def findPropertyInProfPropertyResults(
      key: String,
      props: Seq[RapidsPropertyProfileResult]): Option[String] = {
    props.collectFirst {
      case entry: RapidsPropertyProfileResult
        if entry.key == key && entry.rows(1) != "null" => entry.rows(1)
    }
  }

  override def getAllProperties: Map[String, String] = {
    app.sparkProps.collect {
      case entry: RapidsPropertyProfileResult if entry.rows(1) != null =>
        entry
    }.map(r => r.key -> r.rows(1)).toMap
  }

  override def getSparkProperty(propKey: String): Option[String] = {
    findPropertyInProfPropertyResults(propKey, app.sparkProps)
  }

  override def getRapidsProperty(propKey: String): Option[String] = {
    findPropertyInProfPropertyResults(propKey, app.rapidsProps)
  }

  override def getSystemProperty(propKey: String): Option[String] = {
    findPropertyInProfPropertyResults(propKey, app.sysProps)
  }

  override def getSparkVersion: Option[String] = {
    Option(app.appInfo.head.sparkVersion)
  }

  override def getJvmGCFractions: Seq[Double] = {
    app.sqlTaskAggMetrics.map {
      taskMetrics => taskMetrics.jvmGCTimeSum * 1.0 / taskMetrics.executorCPUTimeSum
    }
  }

  override def getSpilledMetrics: Seq[Long] = {
    app.sqlTaskAggMetrics.map { task =>
      task.diskBytesSpilledSum + task.memoryBytesSpilledSum
    }
  }

  // Return shuffle stage(Id)s which have positive spilling metrics
  // The heuristics below assume that these are GPU event logs. ie
  // its ok to add disk bytes spilled with memory bytes spilled. This
  // is not correct if its a CPU event log.
  override def getShuffleStagesWithPosSpilling: Set[Long] = {
    app.stageAggMetrics.collect { case row
      if row.srTotalBytesReadSum + row.swBytesWrittenSum > 0 &&
      row.diskBytesSpilledSum + row.memoryBytesSpilledSum > 0 => row.id
    }.toSet
  }

  override def getShuffleSkewStages: Set[Long] = {
    app.skewInfo.map { row => row.stageId }.toSet
  }

  override def getMaxInput: Double = {
    if (app.maxTaskInputBytesRead.nonEmpty) {
      app.maxTaskInputBytesRead.head.maxTaskInputBytesRead
    } else {
      0.0
    }
  }

  override def getRapidsJars: Seq[String] = {
    app.rapidsJar.map(_.jar).seq
  }

  override def getDistinctLocationPct: Double = {
    100.0 * distinctLocations.size / app.dsInfo.size
  }

  override def getRedundantReadSize: Long = {
    distinctLocations
      .filter { case (_, objects) =>
        objects.size > 1 && objects.exists(_.format.contains("Parquet"))
      }
      .iterator
      .map { case (k, v) => k -> v.map(_.data_size).sum }
      .toMap
      .values
      .sum
  }

  override def getMeanInput: Double = {
    if (app.ioMetrics.nonEmpty) {
      app.ioMetrics.map(_.inputBytesReadSum).sum * 1.0 / app.ioMetrics.size
    } else {
      0.0
    }
  }

  override def getMeanShuffleRead: Double = {
    if (app.ioMetrics.nonEmpty) {
      app.ioMetrics.map(_.srTotalBytesReadSum).sum * 1.0 / app.ioMetrics.size
    } else {
      0.0
    }
  }

  /**
   * Check if there are any scan stages with failed tasks due to GPU OOM errors
   * (GpuRetryOOM and GpuSplitAndRetryOOM).
   */
  override def hasScanStagesWithGpuOom: Boolean = {
    // If the plugin is not enabled (i.e. non-GPU app), return false
    if (!app.appInfo.exists(_.pluginEnabled)) {
      return false
    }

    // Find stages with failed tasks due to GPU OOM errors
    val failedStagesWithGpuOom = app.failedTasks.collect {
      case task if SparkRapidsOomExceptions.gpuExceptionClassNames
        .exists(task.endReason.contains) => task.stageId
    }

    if (failedStagesWithGpuOom.isEmpty) {
      return false
    }

    // Calculate stageIds of scan stages (i.e. stages with 'scan time' metrics)
    val scanStages = app.stageMetrics.collect {
      case metric if metric.accMetaRef.getName() == IoMetrics.SCAN_TIME_LABEL => metric.stageId
    }.toSet

    if (scanStages.isEmpty) {
      return false
    }

    // Check if any failed GPU OOM stage is also a scan stage
    failedStagesWithGpuOom.exists(scanStages.contains)
  }

  /**
   * This method checks for failed shuffle stages with OOM errors in the task's end reason.
   * Note: This check is enabled only if the plugin is enabled (i.e. GPU app) and running on YARN.
   * """
   */
  override def hasShuffleStagesWithOom: Boolean = {
    // If the plugin is not enabled (i.e. non-GPU app) or not running on YARN, return false
    val sparkMaster = SparkMaster(getSparkProperty("spark.master"))
    if (!app.appInfo.exists(_.pluginEnabled) || !sparkMaster.contains(Yarn)) {
      return false
    }

    // Get stage IDs of failed shuffle stages
    // Sample stage name: "submitShuffleJob$ at GpuShuffleExchangeExec.scala:53"
    val failedStagesWithShuffle = app.failedStages.collect {
      case stage if stage.name.contains(SparkRapidsOomExceptions.gpuShuffleClassName) =>
        stage.stageId
    }.toSet

    if (failedStagesWithShuffle.isEmpty) {
      return false
    }

    // scalastyle:off line.size.limit
    // Check if the failed task's end reason contains OOM errors
    // Sample end reason for failed tasks on YARN: "ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Container from a bad node: container_e02_17xxx on host: test-cluster-w-0. Exit status: 137"
    // Reference: https://github.com/apache/spark/blob/master/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/YarnAllocator.scala
    // scalastyle:on line.size.limit
    // Regular expressions to identify OOM failures in task's end reason
    // - ExecutorLostFailure
    // - Exit status: 137 (i.e. process was terminated with SIGKILL)
    val oomFailurePatterns = Seq(
      classOf[ExecutorLostFailure].getSimpleName,
      s"Exit status: ${UnixExitCode.FORCE_KILLED}"
    ).map(_.r)

    // Check if any failed task in shuffle stages have OOM failures
    app.failedTasks.exists { task =>
      if (failedStagesWithShuffle.contains(task.stageId)) {
        // Check if the task failed due to OOM
        oomFailurePatterns.forall(p => p.findFirstIn(task.endReason).isDefined)
      } else {
        // Ignore if the failed task is not in a shuffle stage
        false
      }
    }
  }
}
