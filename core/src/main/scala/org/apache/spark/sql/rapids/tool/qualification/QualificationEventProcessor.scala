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

package org.apache.spark.sql.rapids.tool.qualification

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._
import org.apache.spark.sql.rapids.tool.EventProcessorBase
import org.apache.spark.sql.rapids.tool.util.StringUtils

class QualificationEventProcessor(app: QualificationAppInfo, perSqlOnly: Boolean)
  extends EventProcessorBase[QualificationAppInfo](app) {

  type T = QualificationAppInfo

  override def doSparkListenerTaskEnd(
      app: QualificationAppInfo,
      event: SparkListenerTaskEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerTaskEnd(app, event)
    // keep all stage task times to see for nonsql duration
    val taskSum = app.stageIdToTaskEndSum.getOrElseUpdate(event.stageId, {
      new StageTaskQualificationSummary(event.stageId, event.stageAttemptId, 0, 0, 0, 0)
    })
    taskSum.executorRunTime += event.taskMetrics.executorRunTime
    taskSum.executorCPUTime += NANOSECONDS.toMillis(event.taskMetrics.executorCpuTime)
    taskSum.totalTaskDuration += event.taskInfo.duration
    // Add the total bytes read from the task if it's available. This is from inputMetrics if
    // it is reading from datasource, or shuffleReadMetrics if it is reading from shuffle.
    val inputMetrics = event.taskMetrics.inputMetrics
    if (inputMetrics != null) {
      taskSum.totalbytesRead += inputMetrics.bytesRead
    }
    val shuffleReadMetrics = event.taskMetrics.shuffleReadMetrics
    if (shuffleReadMetrics != null) {
      taskSum.totalbytesRead += shuffleReadMetrics.totalBytesRead
    }

    // Adds in everything (including failures)
    app.stageIdToSqlID.get(event.stageId).foreach { sqlID =>
      val taskSum = app.sqlIDToTaskEndSum.getOrElseUpdate(sqlID, {
        new StageTaskQualificationSummary(event.stageId, event.stageAttemptId, 0, 0, 0, 0)
      })
      taskSum.executorRunTime += event.taskMetrics.executorRunTime
      taskSum.executorCPUTime += NANOSECONDS.toMillis(event.taskMetrics.executorCpuTime)
      taskSum.totalTaskDuration += event.taskInfo.duration
    }
  }

  override def doSparkListenerSQLExecutionEnd(
      app: QualificationAppInfo,
      event: SparkListenerSQLExecutionEnd): Unit = {
    super.doSparkListenerSQLExecutionEnd(app, event)
    logDebug("Processing event: " + event.getClass)
    if (!perSqlOnly) {
      app.lastSQLEndTime = Some(event.time)
    }
    // only include duration if it contains no jobs that failed
    val failures = app.sqlIDtoFailures.get(event.executionId)
    if (event.executionFailure.isDefined || failures.isDefined) {
      logWarning(s"SQL execution id ${event.executionId} had failures, skipping")
      // zero out the cpu and run times since failed
      app.sqlIDToTaskEndSum.get(event.executionId).foreach { sum =>
        sum.executorRunTime = 0
        sum.executorCPUTime = 0
      }
    }
  }

  override def doSparkListenerJobStart(
      app: QualificationAppInfo,
      event: SparkListenerJobStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerJobStart(app, event)
    val sqlIDString = event.properties.getProperty("spark.sql.execution.id")
    StringUtils.stringToLong(sqlIDString).foreach { sqlID =>
      event.stageIds.foreach { stageId =>
        app.stageIdToSqlID.getOrElseUpdate(stageId, sqlID)
      }
    }
    val sqlID = StringUtils.stringToLong(sqlIDString)
    // don't store if we are only processing per sql queries and the job isn't
    // related to a SQL query
    if ((perSqlOnly && sqlID.isDefined) || !perSqlOnly) {
      val thisJob = new JobInfoClass(
        event.jobId,
        event.stageIds,
        sqlID,
        event.properties.asScala,
        event.time,
        None,
        None,
        None,
        None,
        app.isGPUModeEnabledForJob(event)
      )
      app.jobIdToInfo.put(event.jobId, thisJob)
    }
  }

  override def doSparkListenerJobEnd(
      app: QualificationAppInfo,
      event: SparkListenerJobEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerJobEnd(app, event)
    if (!perSqlOnly) {
      app.lastJobEndTime = Some(event.time)
    }
    if (event.jobResult != JobSucceeded) {
      app.jobIdToSqlID.get(event.jobId) match {
        case Some(sqlID) =>
          // zero out the cpu and run times since failed
          app.sqlIDToTaskEndSum.get(sqlID).foreach { sum =>
            sum.executorRunTime = 0
            sum.executorCPUTime = 0
          }
          val failures = app.sqlIDtoFailures.getOrElseUpdate(sqlID, ArrayBuffer.empty[String])
          val jobStr = s"Job${event.jobId}"
          failures += jobStr
        case None =>
      }
    }
  }

  override def doSparkListenerStageCompleted(
      app: QualificationAppInfo,
      event: SparkListenerStageCompleted): Unit = {
    super.doSparkListenerStageCompleted(app, event)
    if (event.stageInfo.failureReason.nonEmpty) {
      app.stageIdToSqlID.get(event.stageInfo.stageId) match {
        case Some(sqlID) =>
          val failures = app.sqlIDtoFailures.getOrElseUpdate(sqlID, ArrayBuffer.empty[String])
          val stageStr = s"Stage${event.stageInfo.stageId}"
          failures += stageStr
        case None =>
      }
    }
  }
}
