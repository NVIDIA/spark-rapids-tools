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

import scala.collection.mutable

import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.profiling._
import org.apache.spark.sql.rapids.tool.store.TaskModel

/**
 * Does analysis on the DataFrames
 * from object of ApplicationInfo
 */
class Analysis(apps: Seq[ApplicationInfo]) {

  def getDurations(tcs: Iterable[TaskModel]): (Long, Long, Long, Double) = {
    val durations = tcs.map(_.duration)
    if (durations.nonEmpty ) {
      (durations.sum, durations.max, durations.min,
        ToolUtils.calculateAverage(durations.sum, durations.size, 1))
    } else {
      (0L, 0L, 0L, 0.toDouble)
    }
  }

  private def maxWithEmptyHandling(arr: Iterable[Long]): Long = {
    if (arr.isEmpty) {
      0L
    } else {
      arr.max
    }
  }

  private def minWithEmptyHandling(arr: Iterable[Long]): Long = {
    if (arr.isEmpty) {
      0L
    } else {
      arr.min
    }
  }

  // Job + Stage Level TaskMetrics Aggregation
  def jobAndStageMetricsAggregation(): Seq[JobStageAggTaskMetricsProfileResult] = {
    // first get all stage aggregated levels
    val allRows = apps.flatMap { app =>
      // create a cache of stage rows to be used by the job aggregator
      val cachedStageRows = new mutable.LinkedHashMap[Int, JobStageAggTaskMetricsProfileResult]()
      // TODO: this has stage attempts. we should handle different attempts
      app.stageManager.getAllStages.foreach { case sm =>
        val tasksInStage = app.taskManager.getTasks(sm.sId, sm.attemptId)
        // count duplicate task attempts
        val numAttempts = tasksInStage.size
        val (durSum, durMax, durMin, durAvg) = getDurations(tasksInStage)
        val stageRow = JobStageAggTaskMetricsProfileResult(app.index,
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
          maxWithEmptyHandling(tasksInStage.map(_.peakExecutionMemory)),
          tasksInStage.map(_.resultSerializationTime).sum,
          maxWithEmptyHandling(tasksInStage.map(_.resultSize)),
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
        // cache the stage row to be used later
        cachedStageRows.put(sm.sId, stageRow)
      }
      // Aggregate all the stages by job
      val jobRows = app.jobIdToInfo.map { case (id, jc) =>
        if (jc.stageIds.isEmpty) {
          None
        } else {
          val profResultsInJob = cachedStageRows.filterKeys(jc.stageIds.contains).values
          if (profResultsInJob.isEmpty) {
            None
          } else {
            // Recalculate the duration sum, max, min, avg for the job based on the cached
            // stage Profiling results
            val tasksInJob = profResultsInJob.map(_.numTasks).sum
            val durSum = profResultsInJob.map(_.durationSum).sum
            val durMax = maxWithEmptyHandling(profResultsInJob.map(_.durationMax))
            val durMin = minWithEmptyHandling(profResultsInJob.map(_.durationMin))
            val durAvg = ToolUtils.calculateAverage(durSum, tasksInJob, 1)
            Some(JobStageAggTaskMetricsProfileResult(app.index,
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
              maxWithEmptyHandling(profResultsInJob.map(_.peakExecutionMemoryMax)),
              profResultsInJob.map(_.resultSerializationTimeSum).sum,
              maxWithEmptyHandling(profResultsInJob.map(_.resultSizeMax)),
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
      cachedStageRows.values ++ jobRows.flatMap(row => row)
    }

    if (allRows.nonEmpty) {
      val sortedRows = allRows.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, -sortDur, cols.id)
      }
      sortedRows
    } else {
      Seq.empty
    }
  }

  // SQL Level TaskMetrics Aggregation(Only when SQL exists)
  def sqlMetricsAggregation(): Seq[SQLTaskAggMetricsProfileResult] = {
    val allRows = apps.flatMap { app =>
      app.sqlIdToInfo.map { case (sqlId, sqlCase) =>
        if (app.sqlIdToStages.contains(sqlId)) {
          val stagesInSQL = app.sqlIdToStages(sqlId)
          val tasksInSQL = app.taskManager.getTasksByStageIds(stagesInSQL)
          if (tasksInSQL.isEmpty) {
            None
          } else {
            // count all attempts
            val numAttempts = tasksInSQL.size

            val diskBytes = tasksInSQL.map(_.diskBytesSpilled).sum
            val execCpuTime = tasksInSQL.map(_.executorCPUTime).sum
            val execRunTime = tasksInSQL.map(_.executorRunTime).sum
            val execCPURatio = ToolUtils.calculateDurationPercent(execCpuTime, execRunTime)

            // set this here, so make sure we don't get it again until later
            sqlCase.sqlCpuTimePercent = execCPURatio

            val (durSum, durMax, durMin, durAvg) = getDurations(tasksInSQL)
            Some(SQLTaskAggMetricsProfileResult(app.index,
              app.appId,
              sqlId,
              sqlCase.description,
              numAttempts,
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
              tasksInSQL.map(_.executorDeserializeCPUTime).sum,
              tasksInSQL.map(_.executorDeserializeTime).sum,
              execRunTime,
              tasksInSQL.map(_.input_bytesRead).sum,
              tasksInSQL.map(_.input_bytesRead).sum * 1.0 / tasksInSQL.size,
              tasksInSQL.map(_.input_recordsRead).sum,
              tasksInSQL.map(_.jvmGCTime).sum,
              tasksInSQL.map(_.memoryBytesSpilled).sum,
              tasksInSQL.map(_.output_bytesWritten).sum,
              tasksInSQL.map(_.output_recordsWritten).sum,
              maxWithEmptyHandling(tasksInSQL.map(_.peakExecutionMemory)),
              tasksInSQL.map(_.resultSerializationTime).sum,
              maxWithEmptyHandling(tasksInSQL.map(_.resultSize)),
              tasksInSQL.map(_.sr_fetchWaitTime).sum,
              tasksInSQL.map(_.sr_localBlocksFetched).sum,
              tasksInSQL.map(_.sr_localBytesRead).sum,
              tasksInSQL.map(_.sr_remoteBlocksFetched).sum,
              tasksInSQL.map(_.sr_remoteBytesRead).sum,
              tasksInSQL.map(_.sr_remoteBytesReadToDisk).sum,
              tasksInSQL.map(_.sr_totalBytesRead).sum,
              tasksInSQL.map(_.sw_bytesWritten).sum,
              tasksInSQL.map(_.sw_recordsWritten).sum,
              tasksInSQL.map(_.sw_writeTime).sum
            ))
          }
        } else {
          None
        }
      }
    }
    val allFiltered = allRows.flatMap(row => row)
    if (allFiltered.size > 0) {
      val sortedRows = allFiltered.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, -(sortDur), cols.sqlId, cols.executorCpuTime)
      }
      sortedRows
    } else {
      Seq.empty
    }
  }

  def ioAnalysis(): Seq[IOAnalysisProfileResult] = {
    val allRows = apps.flatMap { app =>
      app.sqlIdToStages.map {
        case (sqlId, stageIds) =>
          val tasksInSQL = app.taskManager.getTasksByStageIds(stageIds)
          if (tasksInSQL.isEmpty) {
            None
          } else {
            val diskBytes = tasksInSQL.map(_.diskBytesSpilled).sum
            Some(IOAnalysisProfileResult(app.index,
              app.appId,
              sqlId,
              tasksInSQL.map(_.input_bytesRead).sum,
              tasksInSQL.map(_.input_recordsRead).sum,
              tasksInSQL.map(_.output_bytesWritten).sum,
              tasksInSQL.map(_.output_recordsWritten).sum,
              diskBytes,
              tasksInSQL.map(_.memoryBytesSpilled).sum,
              tasksInSQL.map(_.sr_totalBytesRead).sum,
              tasksInSQL.map(_.sw_bytesWritten).sum
            ))
          }
      }
    }
    val allFiltered = allRows.flatMap(row => row)
    if (allFiltered.size > 0) {
      val sortedRows = allFiltered.sortBy { cols =>
        (cols.appIndex, cols.sqlId)
      }
      sortedRows
    } else {
      Seq.empty
    }
  }

  def getMaxTaskInputSizeBytes(): Seq[SQLMaxTaskInputSizes] = {
    apps.map { app =>
      val maxOfSqls = app.sqlIdToStages.map {
        case (_, stageIds) =>
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
        0
      }
      SQLMaxTaskInputSizes(app.index, app.appId, maxVal)
    }
  }

  def sqlMetricsAggregationDurationAndCpuTime(): Seq[SQLDurationExecutorTimeProfileResult] = {
    val allRows = apps.flatMap { app =>
      app.sqlIdToInfo.map { case (sqlId, sqlCase) =>
        SQLDurationExecutorTimeProfileResult(app.index, app.appId, sqlCase.rootExecutionID,
          sqlId, sqlCase.duration, sqlCase.hasDatasetOrRDD,
          app.getAppDuration.orElse(Option(0L)), sqlCase.problematic,
          sqlCase.sqlCpuTimePercent)
      }
    }
    if (allRows.nonEmpty) {
      val sortedRows = allRows.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, cols.sqlID, sortDur)
      }
      sortedRows
    } else {
      Seq.empty
    }
  }

  private case class AverageStageInfo(avgDuration: Double, avgShuffleReadBytes: Double)

  def shuffleSkewCheck(): Seq[ShuffleSkewProfileResult] = {
    val allRows = apps.flatMap { app =>
      val avgsStageInfos = app.taskManager.stageAttemptToTasks.collect {
        case (stageId, attemptsToTasks) if attemptsToTasks.nonEmpty =>
          attemptsToTasks.map { case (attemptId, tasks) =>
            val sumDuration = tasks.map(_.duration).sum
            val avgDuration = ToolUtils.calculateAverage(sumDuration, tasks.size, 2)
            val sumShuffleReadBytes = tasks.map(_.sr_totalBytesRead).sum
            val avgShuffleReadBytes = ToolUtils.calculateAverage(sumShuffleReadBytes, tasks.size, 2)
            ((stageId, attemptId), AverageStageInfo(avgDuration, avgShuffleReadBytes))
          }
      }.flatten

      avgsStageInfos.flatMap { case ((stageId, attemptId), avg) =>
        val definedTasks =
          app.taskManager.getTasks(stageId, attemptId, Some(
            tc => (tc.sr_totalBytesRead > 3 * avg.avgShuffleReadBytes)
              && (tc.sr_totalBytesRead > 100 * 1024 * 1024)))
        definedTasks.map { tc =>
          Some(ShuffleSkewProfileResult(app.index, stageId, attemptId,
            tc.taskId, tc.attempt, tc.duration, avg.avgDuration, tc.sr_totalBytesRead,
            avg.avgShuffleReadBytes, tc.peakExecutionMemory, tc.successful, tc.endReason))
        }
      }
    }
    val allNonEmptyRows = allRows.flatMap(row => row)
    if (allNonEmptyRows.nonEmpty) {
      val sortedRows = allNonEmptyRows.sortBy { cols =>
        (cols.appIndex, cols.stageId, cols.stageAttemptId, cols.taskId, cols.taskAttemptId)
      }
      sortedRows
    } else {
      Seq.empty
    }
  }
}
