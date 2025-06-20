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

import java.io.File

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, ToolTestUtils}
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.tool.profiling._
import org.apache.spark.sql.rapids.tool.util.StringUtils

class HealthCheckSuite extends FunSuite {

  lazy val sparkSession: SparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }

  lazy val hadoopConf: Configuration = sparkSession.sparkContext.hadoopConfiguration

  private val expRoot = ToolTestUtils.getTestResourceFile("ProfilingExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")

  test("test task-stage-job-failures") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/tasks_executors_fail_compressed_eventlog.zstd"))
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1)
    }
    assert(apps.size == 1)

    val healthCheck = new HealthCheck(apps)
    for (_ <- apps) {
      val failedTasks = healthCheck.getFailedTasks
      import sparkSession.implicits._
      val taskAccums = failedTasks.toDF
      val tasksResultExpectation =
        new File(expRoot, "tasks_failure_eventlog_expectation.csv")
      val tasksDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, tasksResultExpectation.getPath)
      ToolTestUtils.compareDataFrames(taskAccums, tasksDfExpect)

      val failedStages = healthCheck.getFailedStages.map { s =>
        val rendered = StringUtils.renderStr(s.endReason, doEscapeMetaCharacters = true,
          maxLength = 0)
        s.copy(endReason = rendered)
      }
      val stageAccums = failedStages.toDF
      val stagesResultExpectation =
        new File(expRoot, "stages_failure_eventlog_expectation.csv")
      val stagesDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, stagesResultExpectation.getPath)
      ToolTestUtils.compareDataFrames(stageAccums, stagesDfExpect)

      val failedJobs = healthCheck.getFailedJobs
      val jobsAccums = failedJobs.toDF
      val jobsResultExpectation =
        new File(expRoot, "jobs_failure_eventlog_expectation.csv")
      val jobsDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, jobsResultExpectation.getPath)
      ToolTestUtils.compareDataFrames(jobsAccums, jobsDfExpect)
    }
  }

  test("test blockManager_executors_failures") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/tasks_executors_fail_compressed_eventlog.zstd"))
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1)
    }
    assert(apps.size == 1)
    val healthCheck = new HealthCheck(apps)
    for (_ <- apps) {
      val removedBMs = healthCheck.getRemovedBlockManager
      import sparkSession.implicits._
      val blockManagersAccums = removedBMs.toDF
      val blockManagersResultExpectation =
        new File(expRoot, "removed_blockManagers_eventlog_expectation.csv")
      val blockManagersDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, blockManagersResultExpectation.getPath)
      ToolTestUtils.compareDataFrames(blockManagersAccums, blockManagersDfExpect)

      val removedExecs = healthCheck.getRemovedExecutors
      val executorRemovedAccums = removedExecs.toDF
      val executorRemovedResultExpectation =
        new File(expRoot, "executors_removed_eventlog_expectation.csv")
      val executorsRemovedDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, executorRemovedResultExpectation.getPath)
      ToolTestUtils.compareDataFrames(executorRemovedAccums, executorsRemovedDfExpect)
    }
  }

  test("test unSupportedSQLPlan") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/dataset_eventlog"))
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1)
    }
    assert(apps.size == 1)

    val healthCheck = new HealthCheck(apps)
    for (_ <- apps) {
      val unsupported = healthCheck.getPossibleUnsupportedSQLPlan
      import sparkSession.implicits._
      val unsupportedPlanAccums = unsupported.toDF
      val unSupportedPlanExpectation =
        new File(expRoot, "unsupported_sql_eventlog_expectation.csv")
      val unSupportedPlanDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, unSupportedPlanExpectation.getPath)
      ToolTestUtils.compareDataFrames(unsupportedPlanAccums, unSupportedPlanDfExpect)
    }
  }
}
