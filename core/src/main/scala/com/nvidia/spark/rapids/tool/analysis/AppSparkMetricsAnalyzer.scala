/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.profiling.{IOAnalysisProfileResult, JobStageAggTaskMetricsProfileResult, ShuffleSkewProfileResult, SQLDurationExecutorTimeProfileResult, SQLMaxTaskInputSizes, SQLTaskAggMetricsProfileResult}

import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}
import org.apache.spark.sql.rapids.tool.store.TaskModel

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
    Option[mutable.LinkedHashMap[Int, JobStageAggTaskMetricsProfileResult]] = None

  // Getter method used to protect the cache from out-of-order calls.
  // If the stage-level metrics are not generated yet, generates and add them to the cache
  private def stageLevelSparkMetrics(
      index: Int): mutable.LinkedHashMap[Int, JobStageAggTaskMetricsProfileResult] = {
    if (stageLevelCache.isEmpty) {
      stageLevelCache = Some(mutable.LinkedHashMap[Int, JobStageAggTaskMetricsProfileResult]())
      aggregateSparkMetricsByStageInternal(index)
    }
    stageLevelCache.get
  }

  /**
   *  Aggregate the SparkMetrics by stage
   * @param index the App-index (used by the profiler tool)
   * @return sequence of JobStageAggTaskMetricsProfileResult that contains only Stage Ids
   */
  def aggregateSparkMetricsByStage(index: Int): Seq[JobStageAggTaskMetricsProfileResult] = {
    stageLevelSparkMetrics(index).values.toSeq
  }

  /**
   * Aggregate the SparkMetrics by Job
   * @param index the App-index (used by the profiler tool)
   * @return sequence of JobStageAggTaskMetricsProfileResult that contains only Job Ids
   */
  def aggregateSparkMetricsByJob(index: Int): Seq[JobStageAggTaskMetricsProfileResult] = {
    val jobRows = app.jobIdToInfo.flatMap { case (id, jc) =>
      if (jc.stageIds.isEmpty) {
        None
      } else {
        val profResultsInJob = stageLevelSparkMetrics(index).filterKeys(jc.stageIds.contains).values
        if (profResultsInJob.isEmpty) {
          None
        } else {
          // Recalculate the duration sum, max, min, avg for the job based on the cached
          // stage Profiling results
          val tasksInJob = profResultsInJob.map(_.numTasks).sum
          val durSum = profResultsInJob.map(_.durationSum).sum
          val durMax =
            AppSparkMetricsAnalyzer.maxWithEmptyHandling(profResultsInJob.map(_.durationMax))
          val durMin =
            AppSparkMetricsAnalyzer.minWithEmptyHandling(profResultsInJob.map(_.durationMin))
          val durAvg = ToolUtils.calculateAverage(durSum, tasksInJob, 1)
          Some(JobStageAggTaskMetricsProfileResult(index,
            s"job_$id",
            tasksInJob,
            jc.duration,
            profResultsInJob.map(_.diskBytesSpilledSum).sum,
            durSum,
            durMax,
            durMin,
            durAvg,
            profResultsInJob.map(_.executorCPUTimeSum).sum,
            profResultsInJob.map(_.executorDeserializeCpuTimeSum).sum,
            profResultsInJob.map(_.executorDeserializeTimeSum).sum,
            profResultsInJob.map(_.executorRunTimeSum).sum,
            profResultsInJob.map(_.inputBytesReadSum).sum,
            profResultsInJob.map(_.inputRecordsReadSum).sum,
            profResultsInJob.map(_.jvmGCTimeSum).sum,
            profResultsInJob.map(_.memoryBytesSpilledSum).sum,
            profResultsInJob.map(_.outputBytesWrittenSum).sum,
            profResultsInJob.map(_.outputRecordsWrittenSum).sum,
            AppSparkMetricsAnalyzer.maxWithEmptyHandling(
              profResultsInJob.map(_.peakExecutionMemoryMax)),
            profResultsInJob.map(_.resultSerializationTimeSum).sum,
            AppSparkMetricsAnalyzer.maxWithEmptyHandling(profResultsInJob.map(_.resultSizeMax)),
            profResultsInJob.map(_.srFetchWaitTimeSum).sum,
            profResultsInJob.map(_.srLocalBlocksFetchedSum).sum,
            profResultsInJob.map(_.srcLocalBytesReadSum).sum,
            profResultsInJob.map(_.srRemoteBlocksFetchSum).sum,
            profResultsInJob.map(_.srRemoteBytesReadSum).sum,
            profResultsInJob.map(_.srRemoteBytesReadToDiskSum).sum,
            profResultsInJob.map(_.srTotalBytesReadSum).sum,
            profResultsInJob.map(_.swBytesWrittenSum).sum,
            profResultsInJob.map(_.swRecordsWrittenSum).sum,
            profResultsInJob.map(_.swWriteTimeSum).sum))
        }
      }
    }
    jobRows.toSeq
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
        ShuffleSkewProfileResult(index, stageId, attemptId,
          tc.taskId, tc.attempt, tc.duration, avg.avgDuration, tc.sr_totalBytesRead,
          avg.avgShuffleReadBytes, tc.peakExecutionMemory, tc.successful, tc.endReason)
      }
    }.toSeq
  }

  /**
   * Aggregate the SparkMetrics by SQL
   * @param index the App-index (used by the profiler tool)
   * @return sequence of SQLTaskAggMetricsProfileResult
   */
  def aggregateSparkMetricsBySql(index: Int): Seq[SQLTaskAggMetricsProfileResult] = {
    val sqlRows = app.sqlIdToInfo.flatMap { case (sqlId, sqlCase) =>
      if (app.sqlIdToStages.contains(sqlId)) {
        val stagesInSQL = app.sqlIdToStages(sqlId)
        // TODO: Should we only consider successful tasks?
        val cachedResBySQL = stageLevelSparkMetrics(index).filterKeys(stagesInSQL.contains).values
        if (cachedResBySQL.isEmpty) {
          None
        } else {
          // Recalculate the duration sum, max, min, avg for the job based on the cached
          // stage Profiling results
          val tasksInSql = cachedResBySQL.map(_.numTasks).sum
          val durSum = cachedResBySQL.map(_.durationSum).sum
          val durMax =
            AppSparkMetricsAnalyzer.maxWithEmptyHandling(cachedResBySQL.map(_.durationMax))
          val durMin =
            AppSparkMetricsAnalyzer.minWithEmptyHandling(cachedResBySQL.map(_.durationMin))
          val durAvg = ToolUtils.calculateAverage(durSum, tasksInSql, 1)
          val diskBytes = cachedResBySQL.map(_.diskBytesSpilledSum).sum
          val execCpuTime = cachedResBySQL.map(_.executorCPUTimeSum).sum
          val execRunTime = cachedResBySQL.map(_.executorRunTimeSum).sum
          val execCPURatio = ToolUtils.calculateDurationPercent(execCpuTime, execRunTime)
          val inputBytesRead = cachedResBySQL.map(_.inputBytesReadSum).sum
          // set this here, so make sure we don't get it again until later
          sqlCase.sqlCpuTimePercent = execCPURatio

          Some(SQLTaskAggMetricsProfileResult(index,
            app.appId,
            sqlId,
            sqlCase.description,
            tasksInSql,
            sqlCase.duration,
            execCpuTime,
            execRunTime,
            execCPURatio,
            diskBytes,
            durSum,
            durMax,
            durMin,
            durAvg,
            execCpuTime,
            cachedResBySQL.map(_.executorDeserializeCpuTimeSum).sum,
            cachedResBySQL.map(_.executorDeserializeTimeSum).sum,
            execRunTime,
            inputBytesRead,
            inputBytesRead * 1.0 / tasksInSql,
            cachedResBySQL.map(_.inputRecordsReadSum).sum,
            cachedResBySQL.map(_.jvmGCTimeSum).sum,
            cachedResBySQL.map(_.memoryBytesSpilledSum).sum,
            cachedResBySQL.map(_.outputBytesWrittenSum).sum,
            cachedResBySQL.map(_.outputRecordsWrittenSum).sum,
            AppSparkMetricsAnalyzer.maxWithEmptyHandling(
              cachedResBySQL.map(_.peakExecutionMemoryMax)),
            cachedResBySQL.map(_.resultSerializationTimeSum).sum,
            AppSparkMetricsAnalyzer.maxWithEmptyHandling(cachedResBySQL.map(_.resultSizeMax)),
            cachedResBySQL.map(_.srFetchWaitTimeSum).sum,
            cachedResBySQL.map(_.srLocalBlocksFetchedSum).sum,
            cachedResBySQL.map(_.srcLocalBytesReadSum).sum,
            cachedResBySQL.map(_.srRemoteBlocksFetchSum).sum,
            cachedResBySQL.map(_.srRemoteBytesReadSum).sum,
            cachedResBySQL.map(_.srRemoteBytesReadToDiskSum).sum,
            cachedResBySQL.map(_.srTotalBytesReadSum).sum,
            cachedResBySQL.map(_.swBytesWrittenSum).sum,
            cachedResBySQL.map(_.swRecordsWrittenSum).sum,
            cachedResBySQL.map(_.swWriteTimeSum).sum))
        }
      } else {
        None
      }
    }
    sqlRows.toSeq
  }

  /**
   * Aggregates the IO metrics by SQL
   * @param sqlMetricsAggs Spark metrics the aggregated by SQL. This is an optimization tuning to
   *                       avoid recalculating those metrics twice.
   * @return IOAnalysisProfileResult that contains the IO metrics aggregated by SQL
   */
  def aggregateIOMetricsBySql(
      sqlMetricsAggs: Seq[SQLTaskAggMetricsProfileResult]): Seq[IOAnalysisProfileResult] = {
    val sqlIORows = sqlMetricsAggs.map { sqlAgg =>
      IOAnalysisProfileResult(sqlAgg.appIndex,
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
    }
    sqlIORows
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
    SQLMaxTaskInputSizes(index, app.appId, maxVal)
  }

  /**
   * Aggregates the duration and CPU time (milliseconds) by SQL
   * @param index App index  (used by the profiler tool)
   * @return a sequence of SQLDurationExecutorTimeProfileResult or Empty if None.
   */
  def aggregateDurationAndCPUTimeBySql(index: Int): Seq[SQLDurationExecutorTimeProfileResult] = {
    val sqlRows = app.sqlIdToInfo.map { case (sqlId, sqlCase) =>
      // First, build the SQLIssues string by retrieving the potential issues from the
      // app.sqlIDtoProblematic map.
      val sqlIssues = if (app.sqlIDtoProblematic.contains(sqlId)) {
        ToolUtils.formatPotentialProblems(app.sqlIDtoProblematic(sqlId).toSeq)
      } else {
        ""
      }
      // Then, build the SQLDurationExecutorTimeProfileResult
      SQLDurationExecutorTimeProfileResult(index, app.appId, sqlCase.rootExecutionID,
        sqlId, sqlCase.duration, sqlCase.hasDatasetOrRDD,
        app.getAppDuration.orElse(Option(0L)), sqlIssues, sqlCase.sqlCpuTimePercent)
    }
    sqlRows.toSeq
  }

  /**
   * Aggregates the SparkMetrics by stage. This is an internal method to populate the cached metrics
   * to be used by other aggregators.
   * @param index AppIndex (used by the profiler tool)
   */
  private def aggregateSparkMetricsByStageInternal(index: Int): Unit = {
    // TODO: this has stage attempts. we should handle different attempts
    app.stageManager.getAllStages.foreach { sm =>
      // TODO: Should we only consider successful tasks?
      val tasksInStage = app.taskManager.getTasks(sm.sId, sm.attemptId)
      // count duplicate task attempts
      val numAttempts = tasksInStage.size
      val (durSum, durMax, durMin, durAvg) = AppSparkMetricsAnalyzer.getDurations(tasksInStage)
      val stageRow = JobStageAggTaskMetricsProfileResult(index,
        s"stage_${sm.sId}",
        numAttempts,
        sm.duration,
        tasksInStage.map(_.diskBytesSpilled).sum,
        durSum,
        durMax,
        durMin,
        durAvg,
        tasksInStage.map(_.executorCPUTime).sum,
        tasksInStage.map(_.executorDeserializeCPUTime).sum,
        tasksInStage.map(_.executorDeserializeTime).sum,
        tasksInStage.map(_.executorRunTime).sum,
        tasksInStage.map(_.input_bytesRead).sum,
        tasksInStage.map(_.input_recordsRead).sum,
        tasksInStage.map(_.jvmGCTime).sum,
        tasksInStage.map(_.memoryBytesSpilled).sum,
        tasksInStage.map(_.output_bytesWritten).sum,
        tasksInStage.map(_.output_recordsWritten).sum,
        AppSparkMetricsAnalyzer.maxWithEmptyHandling(tasksInStage.map(_.peakExecutionMemory)),
        tasksInStage.map(_.resultSerializationTime).sum,
        AppSparkMetricsAnalyzer.maxWithEmptyHandling(tasksInStage.map(_.resultSize)),
        tasksInStage.map(_.sr_fetchWaitTime).sum,
        tasksInStage.map(_.sr_localBlocksFetched).sum,
        tasksInStage.map(_.sr_localBytesRead).sum,
        tasksInStage.map(_.sr_remoteBlocksFetched).sum,
        tasksInStage.map(_.sr_remoteBytesRead).sum,
        tasksInStage.map(_.sr_remoteBytesReadToDisk).sum,
        tasksInStage.map(_.sr_totalBytesRead).sum,
        tasksInStage.map(_.sw_bytesWritten).sum,
        tasksInStage.map(_.sw_recordsWritten).sum,
        tasksInStage.map(_.sw_writeTime).sum
      )
      stageLevelSparkMetrics(index).put(sm.sId, stageRow)
    }
  }
}


object AppSparkMetricsAnalyzer  {
  def getDurations(tcs: Iterable[TaskModel]): (Long, Long, Long, Double) = {
    val durations = tcs.map(_.duration)
    if (durations.nonEmpty) {
      (durations.sum, durations.max, durations.min,
        ToolUtils.calculateAverage(durations.sum, durations.size, 1))
    } else {
      (0L, 0L, 0L, 0.toDouble)
    }
  }

  def maxWithEmptyHandling(arr: Iterable[Long]): Long = {
    if (arr.isEmpty) {
      0L
    } else {
      arr.max
    }
  }

  def minWithEmptyHandling(arr: Iterable[Long]): Long = {
    if (arr.isEmpty) {
      0L
    } else {
      arr.min
    }
  }
}
