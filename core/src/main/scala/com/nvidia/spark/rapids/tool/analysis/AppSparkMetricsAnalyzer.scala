/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.analysis

import scala.collection.breakOut
import scala.collection.mutable.{ArrayBuffer, HashMap, LinkedHashMap}

import com.nvidia.spark.rapids.tool.analysis.util.{AggAccumHelper, AggAccumPhotonHelper}
import com.nvidia.spark.rapids.tool.analysis.util.StageAccumDiagnosticMetrics._
import com.nvidia.spark.rapids.tool.planparser.DatabricksParseHelper
import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.store.{AccumInfo, AccumMetaRef}

/**
 * Does analysis on the DataFrames from object of AppBase.
 * This class does the following:
 * - aggregates SparkMetrics by Job, Stage, and SQL.
 * - checks for shuffle skewness.
 * - Find the max inputSizes for SQL.
 *
 * The implementation is tuned to improve the performance by reducing the number of times the
 * analyzer visits the Tasks.
 * 1- The assumption is that it is unlikely that the analysis will be aggregating metrics only for
 *     one of for SQL, jobs, or stages. Instead, any analysis is likely to do SQL/Stage levels.
 * 2- The analyzer caches the stage level metrics to avoid recalculating the same metrics several
 *    times
 * 3- The cached stage-level metrics are then used to calculate the aggregates for SQLs, and Jobs
 * 4- It can be used by both Qual/Prof tools: this why it takes app-index as an argument to the
 *    aggregator methods. The index is a value used by the Profiler tool to list records from
 *    multiple applications.
 *
 * @param app the AppBase object to analyze
 */
class AppSparkMetricsAnalyzer(app: AppBase) extends AppAnalysisBase(app) {
  // Hashmap to cache the stage level metrics. It is initialized to None just in case the caller
  // does not call methods in order starting with stage level metrics.
  private var stageLevelCache:
    Option[LinkedHashMap[Int, StageAggTaskMetricsProfileResult]] = None

  // Getter method used to protect the cache from out-of-order calls.
  // If the stage-level metrics are not generated yet, generates and add them to the cache
  private def stageLevelSparkMetrics(
      index: Int): LinkedHashMap[Int, StageAggTaskMetricsProfileResult] = {
    if (stageLevelCache.isEmpty) {
      stageLevelCache = Some(LinkedHashMap[Int, StageAggTaskMetricsProfileResult]())
      aggregateSparkMetricsByStageInternal(index)
    }
    stageLevelCache.get
  }

  /**
   *  Aggregate the SparkMetrics by stage
   * @param index the App-index (used by the profiler tool)
   * @return sequence of StageAggTaskMetricsProfileResult that contains only Stage Ids
   */
  def aggregateSparkMetricsByStage(index: Int): Seq[StageAggTaskMetricsProfileResult] = {
    stageLevelSparkMetrics(index).values.toSeq
  }

  /**
   * Aggregate the SparkMetrics by Job
   * @param index the App-index (used by the profiler tool)
   * @return sequence of JobAggTaskMetricsProfileResult that contains only Job Ids
   */
  def aggregateSparkMetricsByJob(index: Int): Seq[JobAggTaskMetricsProfileResult] = {
    app.jobIdToInfo.flatMap { case (id, jc) =>
      if (jc.stageIds.isEmpty) {
        None
      } else {
        val jobAggAccumulator = new AggAccumHelper()
        val perJobRec = jobAggAccumulator.accumPerJob(
          jc.stageIds.collect {
            case stageId if stageLevelSparkMetrics(index).contains(stageId) =>
              stageLevelSparkMetrics(index)(stageId)
          })
        if (perJobRec.isEmptyAggregates) {
          None
        } else {
          Some(JobAggTaskMetricsProfileResult(
            id,
            perJobRec.numTasks,
            jc.duration,
            perJobRec.diskBytesSpilledSum,
            perJobRec.durationSum,
            perJobRec.durationMax,
            perJobRec.durationMin,
            perJobRec.durationAvg,
            perJobRec.executorCPUTimeSum,
            perJobRec.executorDeserializeCpuTimeSum,
            perJobRec.executorDeserializeTimeSum,
            perJobRec.executorRunTimeSum,
            perJobRec.inputBytesReadSum,
            perJobRec.inputRecordsReadSum,
            perJobRec.jvmGCTimeSum,
            perJobRec.memoryBytesSpilledSum,
            perJobRec.outputBytesWrittenSum,
            perJobRec.outputRecordsWrittenSum,
            perJobRec.peakExecutionMemoryMax,
            perJobRec.resultSerializationTimeSum,
            perJobRec.resultSizeMax,
            perJobRec.srFetchWaitTimeSum,
            perJobRec.srLocalBlocksFetchedSum,
            perJobRec.srLocalBytesReadSum,
            perJobRec.srRemoteBlocksFetchSum,
            perJobRec.srRemoteBytesReadSum,
            perJobRec.srRemoteBytesReadToDiskSum,
            perJobRec.srTotalBytesReadSum,
            perJobRec.swBytesWrittenSum,
            perJobRec.swRecordsWrittenSum,
            perJobRec.swWriteTimeSum))
        }
      }
    }(breakOut)
  }

  private case class AverageStageInfo(avgDuration: Double, avgShuffleReadBytes: Double)

  /**
   * Scans tasks to identify if any exhibits shuffle skewness. If a task has input size larger than
   * 3X the average shuffle read size and larger than 100MB, it is considered as a skew task.
   * @param index the App-index (used by the profiler tool)
   * @return sequence of ShuffleSkewProfileResult that contains only the skew tasks
   */
  def shuffleSkewCheck(index: Int): Seq[ShuffleSkewProfileResult] = {
    // TODO: we can add averageShuffleRead as a field in JobStageAggTaskMetricsProfileResult instead
    //       of making an extra path on the StageAttempts
    val avgStageInfos = app.taskManager.stageAttemptToTasks.collect {
      // TODO: Should we only consider successful tasks?
      case (stageId, attemptsToTasks) if attemptsToTasks.nonEmpty =>
        attemptsToTasks.map { case (attemptId, tasks) =>
          val sumDuration = tasks.map(_.duration).sum
          val avgDuration = ToolUtils.calculateAverage(sumDuration, tasks.size, 2)
          val sumShuffleReadBytes = tasks.map(_.sr_totalBytesRead).sum
          val avgShuffleReadBytes = ToolUtils.calculateAverage(sumShuffleReadBytes, tasks.size, 2)
          ((stageId, attemptId), AverageStageInfo(avgDuration, avgShuffleReadBytes))
        }
    }.flatten

    avgStageInfos.flatMap { case ((stageId, attemptId), avg) =>
      val definedTasks =
        app.taskManager.getTasks(stageId, attemptId, Some(
          tc => (tc.sr_totalBytesRead > 3 * avg.avgShuffleReadBytes)
            && (tc.sr_totalBytesRead > 100 * 1024 * 1024)))
      definedTasks.map { tc =>
        ShuffleSkewProfileResult(stageId, attemptId,
          tc.taskId, tc.attempt, tc.duration, avg.avgDuration, tc.sr_totalBytesRead,
          avg.avgShuffleReadBytes, tc.peakExecutionMemory, tc.successful, tc.endReason)
      }
    }(breakOut)
  }

  /**
   * Aggregate the SparkMetrics by SQL
   * @param index the App-index (used by the profiler tool)
   * @return sequence of SQLTaskAggMetricsProfileResult
   */
  def aggregateSparkMetricsBySql(index: Int): Seq[SQLTaskAggMetricsProfileResult] = {
    app.sqlIdToInfo.flatMap { case (sqlId, sqlCase) =>
      if (app.sqlIdToStages.contains(sqlId)) {
        val stagesInSQL = app.sqlIdToStages(sqlId)
        // TODO: Should we only consider successful tasks?
        val sqlAggAccumulator = new AggAccumHelper()
        val preSqlRec = sqlAggAccumulator.accumPerSQL(
          stagesInSQL.collect {
            case stageId if stageLevelSparkMetrics(index).contains(stageId) =>
              stageLevelSparkMetrics(index)(stageId)
          })
        if (preSqlRec.isEmptyAggregates) {
          None
        } else {
          // set this here, so make sure we don't get it again until later
          sqlCase.sqlCpuTimePercent = preSqlRec.executorCpuRatio
          Some(SQLTaskAggMetricsProfileResult(
            app.appId,
            sqlId,
            sqlCase.description,
            preSqlRec.numTasks,
            sqlCase.duration,
            preSqlRec.executorCpuRatio,
            preSqlRec.diskBytesSpilledSum,
            preSqlRec.durationSum,
            preSqlRec.durationMax,
            preSqlRec.durationMin,
            preSqlRec.durationAvg,
            preSqlRec.executorCPUTimeSum,
            preSqlRec.executorDeserializeCpuTimeSum,
            preSqlRec.executorDeserializeTimeSum,
            preSqlRec.executorRunTimeSum,
            preSqlRec.inputBytesReadSum,
            preSqlRec.inputBytesReadAvg,
            preSqlRec.inputRecordsReadSum,
            preSqlRec.jvmGCTimeSum,
            preSqlRec.memoryBytesSpilledSum,
            preSqlRec.outputBytesWrittenSum,
            preSqlRec.outputRecordsWrittenSum,
            preSqlRec.peakExecutionMemoryMax,
            preSqlRec.resultSerializationTimeSum,
            preSqlRec.resultSizeMax,
            preSqlRec.srFetchWaitTimeSum,
            preSqlRec.srLocalBlocksFetchedSum,
            preSqlRec.srLocalBytesReadSum,
            preSqlRec.srRemoteBlocksFetchSum,
            preSqlRec.srRemoteBytesReadSum,
            preSqlRec.srRemoteBytesReadToDiskSum,
            preSqlRec.srTotalBytesReadSum,
            preSqlRec.swBytesWrittenSum,
            preSqlRec.swRecordsWrittenSum,
            preSqlRec.swWriteTimeSum))
        }
      } else {
        None
      }
    }(breakOut)
  }

  /**
   * Aggregates the IO metrics by SQL
   * @param sqlMetricsAggs Spark metrics the aggregated by SQL. This is an optimization tuning to
   *                       avoid recalculating those metrics twice.
   * @return IOAnalysisProfileResult that contains the IO metrics aggregated by SQL
   */
  def aggregateIOMetricsBySql(
      sqlMetricsAggs: Seq[SQLTaskAggMetricsProfileResult]): Seq[IOAnalysisProfileResult] = {
    sqlMetricsAggs.map { sqlAgg =>
      IOAnalysisProfileResult(
        app.appId,
        sqlAgg.sqlId,
        sqlAgg.inputBytesReadSum,
        sqlAgg.inputRecordsReadSum,
        sqlAgg.outputBytesWrittenSum,
        sqlAgg.outputRecordsWrittenSum,
        sqlAgg.diskBytesSpilledSum,
        sqlAgg.memoryBytesSpilledSum,
        sqlAgg.srTotalBytesReadSum,
        sqlAgg.swBytesWrittenSum)
    }(breakOut)
  }

  /**
   * Find the maximum task input size
   * @param index App index  (used by the profiler tool)
   * @return a single SQLMaxTaskInputSizes record that contains the maximum value. If none, it will
   *         be 0L
   */
  def maxTaskInputSizeBytesPerSQL(index: Int): SQLMaxTaskInputSizes = {
    // TODO: We should keep maxInputSize as a field in the stageAggregate to avoid doing an
    //       extra path on the tasks
    val maxOfSqls = app.sqlIdToStages.map { case (_, stageIds) =>
      // TODO: Should we only consider successful tasks?
      val tasksInSQL = app.taskManager.getTasksByStageIds(stageIds)
      if (tasksInSQL.isEmpty) {
        0L
      } else {
        tasksInSQL.map(_.input_bytesRead).max
      }
    }
    val maxVal = if (maxOfSqls.nonEmpty) {
      maxOfSqls.max
    } else {
      0L
    }
    SQLMaxTaskInputSizes(app.appId, maxVal)
  }

  /**
   * Aggregates the duration and CPU time (milliseconds) by SQL
   * @param index App index  (used by the profiler tool)
   * @return a sequence of SQLDurationExecutorTimeProfileResult or Empty if None.
   */
  def aggregateDurationAndCPUTimeBySql(index: Int): Seq[SQLDurationExecutorTimeProfileResult] = {
    app.sqlIdToInfo.map { case (sqlId, sqlCase) =>
      // First, build the SQLIssues string by retrieving the potential issues from the
      // app.sqlIDtoProblematic map.
      val sqlIssues = if (app.sqlIDtoProblematic.contains(sqlId)) {
        ToolUtils.formatPotentialProblems(app.sqlIDtoProblematic(sqlId).toSeq)
      } else {
        ""
      }
      // Then, build the SQLDurationExecutorTimeProfileResult
      SQLDurationExecutorTimeProfileResult(app.appId, sqlCase.rootExecutionID,
        sqlId, sqlCase.duration, sqlCase.hasDatasetOrRDD,
        app.getAppDuration.orElse(Option(0L)), sqlIssues, sqlCase.sqlCpuTimePercent)
    }(breakOut)
  }

  /**
   * Aggregates the diagnostic SparkMetrics by stage.
   * @param index    the App-index (used by the profiler tool)
   * @param analyzer optional AppSQLPlanAnalyzer which is used to pull stage level
   *                 information like node names and diagnostic metrics results, only
   *                 Qualification needs to provide this argument.
   * @return sequence of StageDiagnosticAggTaskMetricsProfileResult
   */
  def aggregateDiagnosticMetricsByStage(index: Int, analyzer: Option[AppSQLPlanAnalyzer] = None):
      Seq[StageDiagnosticResult] = {
    if (!isDiagnosticViewsEnabled) {
      return Seq.empty
    }

    // Then get the appropriate analyzer
    val sqlAnalyzer = analyzer match {
      case Some(analyzer) => analyzer
      case None => app.asInstanceOf[ApplicationInfo].planMetricProcessor
    }

    val zeroAccumProfileResults =
      AccumProfileResults(0, AccumMetaRef.EMPTY_ACCUM_META_REF, 0L, 0L, 0L, 0L)
    val emptyNodeNames = Seq.empty[String]
    val emptyDiagnosticMetrics = HashMap.empty[String, AccumProfileResults]
    app.stageManager.getAllStages.map { sm =>
      val tasksInStage = app.taskManager.getTasks(sm.stageInfo.stageId,
        sm.stageInfo.attemptNumber())
      // count duplicate task attempts
      val numTasks = tasksInStage.size
      val nodeNames = sqlAnalyzer.stageToNodeNames.getOrElse(sm.stageInfo.stageId, emptyNodeNames)
      val diagnosticMetricsMap =
        sqlAnalyzer.stageToDiagnosticMetrics
          .getOrElse(sm.stageInfo.stageId, emptyDiagnosticMetrics)
          .withDefaultValue(zeroAccumProfileResults)
      val srTotalBytesMetrics =
        StatisticsMetrics.createFromArr(tasksInStage.map(_.sr_totalBytesRead)(breakOut))

      StageDiagnosticResult(
        app.getAppName,
        app.appId,
        sm.stageInfo.stageId,
        sm.duration,
        numTasks,
        srTotalBytesMetrics.min,
        srTotalBytesMetrics.med,
        srTotalBytesMetrics.max,
        srTotalBytesMetrics.total,
        diagnosticMetricsMap(MEMORY_SPILLED_METRIC),
        diagnosticMetricsMap(DISK_SPILLED_METRIC),
        diagnosticMetricsMap(INPUT_BYTES_READ_METRIC),
        diagnosticMetricsMap(OUTPUT_BYTES_WRITTEN_METRIC),
        diagnosticMetricsMap(SW_TOTAL_BYTES_METRIC),
        diagnosticMetricsMap(SR_FETCH_WAIT_TIME_METRIC),
        diagnosticMetricsMap(SW_WRITE_TIME_METRIC),
        diagnosticMetricsMap(GPU_SEMAPHORE_WAIT_METRIC),
        nodeNames)
    }(breakOut)
  }

  /**
   * Aggregates the SparkMetrics by completed stage information.
   * This is an internal method to populate the cached metrics
   * to be used by other aggregators.
   * @param index AppIndex (used by the profiler tool)
   */
  private def aggregateSparkMetricsByStageInternal(index: Int): Unit = {
    // For Photon apps, peak memory and shuffle write time need to be calculated from accumulators
    // instead of task metrics.
    // Approach:
    //   1. Collect accumulators for each metric type.
    //   2. For each stage, retrieve the relevant accumulators and calculate aggregated values.
    // Note:
    //  - A HashMap could be used instead of separate mutable.ArrayBuffer for each metric type,
    //    but avoiding it for readability.
    val photonPeakMemoryAccumInfos = ArrayBuffer[AccumInfo]()
    val photonShuffleWriteTimeAccumInfos = ArrayBuffer[AccumInfo]()

    if (app.isPhoton) {
      app.accumManager.applyToAccumInfoMap { accumInfo =>
        accumInfo.infoRef.name.value match {
          case name if name.contains(
            DatabricksParseHelper.PHOTON_METRIC_PEAK_MEMORY_LABEL) =>
              // Collect accumulators for peak memory
              photonPeakMemoryAccumInfos += accumInfo
          case name if name.contains(
            DatabricksParseHelper.PHOTON_METRIC_SHUFFLE_WRITE_TIME_LABEL) =>
              // Collect accumulators for shuffle write time
              photonShuffleWriteTimeAccumInfos += accumInfo
          case _ => // Ignore other accumulators
        }
      }
    }

    app.stageManager.getAllStages.foreach { sm =>
      // TODO: Should we only consider successful tasks?
      val tasksInStage = app.taskManager.getTasks(sm.stageInfo.stageId,
        sm.stageInfo.attemptNumber())

      val accumHelperObj = if (app.isPhoton) { // If this a photon app, use the photonHelper
        // For max peak memory, we need to look at the accumulators at the task level.
        // We leverage the stage level metrics and get the max task update from it
        val peakMemoryValues = photonPeakMemoryAccumInfos.flatMap { accumInfo =>
          accumInfo.getMaxForStage(sm.stageInfo.stageId)
        }
        // For sum of shuffle write time, we need to look at the accumulators at the stage level.
        // We get the values associated with all tasks for a stage
        val shuffleWriteValues = photonShuffleWriteTimeAccumInfos.flatMap { accumInfo =>
          accumInfo.getTotalForStage(sm.stageInfo.stageId)
        }
        new AggAccumPhotonHelper(shuffleWriteValues, peakMemoryValues)
      } else {
        // For non-Photon apps, use the task metrics directly.
        new AggAccumHelper()
      }
      val perStageRec = accumHelperObj.accumPerStage(tasksInStage)
      val stageRow = StageAggTaskMetricsProfileResult(
        sm.stageInfo.stageId,
        // numTasks includes duplicate task attempts
        perStageRec.numTasks,
        sm.duration,
        perStageRec.diskBytesSpilledSum,
        perStageRec.durationSum,
        perStageRec.durationMax,
        perStageRec.durationMin,
        perStageRec.durationAvg,
        perStageRec.executorCPUTimeSum,  // converted to milliseconds by the aggregator
        perStageRec.executorDeserializeCpuTimeSum,  // converted to milliseconds by the aggregator
        perStageRec.executorDeserializeTimeSum,
        perStageRec.executorRunTimeSum,
        perStageRec.inputBytesReadSum,
        perStageRec.inputRecordsReadSum,
        perStageRec.jvmGCTimeSum,
        perStageRec.memoryBytesSpilledSum,
        perStageRec.outputBytesWrittenSum,
        perStageRec.outputRecordsWrittenSum,
        perStageRec.peakExecutionMemoryMax,
        perStageRec.resultSerializationTimeSum,
        perStageRec.resultSizeMax,
        perStageRec.srFetchWaitTimeSum,
        perStageRec.srLocalBlocksFetchedSum,
        perStageRec.srLocalBytesReadSum,
        perStageRec.srRemoteBlocksFetchSum,
        perStageRec.srRemoteBytesReadSum,
        perStageRec.srRemoteBytesReadToDiskSum,
        perStageRec.srTotalBytesReadSum,
        perStageRec.swBytesWrittenSum,
        perStageRec.swRecordsWrittenSum,
        perStageRec.swWriteTimeSum)  // converted to milliseconds by the aggregator
      // This logic is to handle the case where there are multiple attempts for a stage.
      // We check if the StageLevelCache already has a row for the stage.
      // If yes, we aggregate the metrics of the new row with the existing row.
      // If no, we just store the new row.
      val rowToStore = stageLevelSparkMetrics(index)
        .get(sm.stageInfo.stageId)
        .map(_.aggregateStageProfileMetric(stageRow))
        .getOrElse(stageRow)
      stageLevelSparkMetrics(index).put(sm.stageInfo.stageId, rowToStore)
    }
  }
}
