/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.mutable

import com.nvidia.spark.rapids.BaseNoSparkSuite
import com.nvidia.spark.rapids.tool.EventLogPathProcessor

import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.sql.TrampolineUtil
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil

/**
 * Tests for Spark Connect sqlID/jobID correlation indexes on `AppBase`.
 * Covers:
 *  - the reverse-index `HashMap`s exist and are initialized empty on a fresh app,
 *  - `operationIdToSqlIds` is populated from `SparkListenerSQLExecutionStart.jobTags`,
 *  - `operationIdToJobIds` is populated from
 *    `SparkListenerJobStart.properties["spark.job.tags"]`.
 */
class ConnectCorrelationSuite extends BaseNoSparkSuite {

  private val hadoopConf = RapidsToolsConfUtil.newHadoopConf()

  private val logStartEvent =
    """{"Event":"SparkListenerLogStart","Spark Version":"3.5.0"}"""
  private val appStartEvent =
    """{"Event":"SparkListenerApplicationStart","App Name":"CorrelationTest",""" +
      """"App ID":"local-correlation","Timestamp":100000,"User":"testUser"}"""
  private val envUpdateEvent =
    """{"Event":"SparkListenerEnvironmentUpdate","JVM Information":{},""" +
      """"Spark Properties":{"spark.master":"local[*]"},""" +
      """"Hadoop Properties":{},"System Properties":{"file.encoding":"UTF-8"},""" +
      """"Classpath Entries":{}}"""
  private val appEndEvent =
    """{"Event":"SparkListenerApplicationEnd","Timestamp":200000}"""

  private def withEventLog(events: String*)(verify: ApplicationInfo => Unit): Unit = {
    val content = events.mkString("\n")
    TrampolineUtil.withTempDir { tempDir =>
      val path = Paths.get(tempDir.getAbsolutePath, "test_eventlog")
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
      val app = new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path.toString, hadoopConf).head._1)
      verify(app)
    }
  }

  /**
   * True when the running Spark profile's SparkListenerSQLExecutionStart has a
   * jobTags accessor (Spark 3.5+). Used to skip tests on older profiles.
   */
  private def checkJobTagsAvailable(): (Boolean, String) = {
    val available = try {
      classOf[SparkListenerSQLExecutionStart].getMethod("jobTags")
      true
    } catch {
      case _: NoSuchMethodException => false
    }
    (available, "SparkListenerSQLExecutionStart.jobTags requires Spark 3.5+")
  }

  /**
   * Builds a SparkListenerSQLExecutionStart with the given jobTags via reflection,
   * matching the 9-arg constructor introduced in Spark 3.5.
   */
  private def buildSQLStartEvent(executionId: Long, jobTags: Set[String])
      : SparkListenerSQLExecutionStart = {
    val planInfo = new SparkPlanInfo(
      "TestNode", "test", Nil, Map.empty[String, String], Nil)
    val ctors = classOf[SparkListenerSQLExecutionStart].getConstructors
    val ctor = ctors.find(_.getParameterCount == 9).getOrElse(
      throw new AssertionError("Expected 9-arg SparkListenerSQLExecutionStart constructor"))
    ctor.newInstance(
      java.lang.Long.valueOf(executionId),
      None,
      "desc",
      "details",
      "physicalPlan",
      planInfo,
      java.lang.Long.valueOf(123000L),
      Map.empty[String, String],
      jobTags).asInstanceOf[SparkListenerSQLExecutionStart]
  }

  test("operationIdToSqlIds / operationIdToJobIds are initialized empty on AppBase") {
    withEventLog(logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      assert(app.operationIdToSqlIds.isEmpty)
      assert(app.operationIdToJobIds.isEmpty)
    }
  }

  runConditionalTest(
    "operationIdToSqlIds populated from SparkListenerSQLExecutionStart.jobTags",
    checkJobTagsAvailable) {
    withEventLog(logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      // Manually seed Connect state as if a ConnectOperationStarted event had fired.
      val jobTag =
        "SparkConnect_OperationTag_User_alice_Session_sess-1_Operation_op-1"
      app.connectOperations.put("op-1", new ConnectOperationInfo(
        operationId = "op-1",
        sessionId = "sess-1",
        userId = "alice",
        jobTag = jobTag,
        statementText = "range(0, 10)",
        startTime = 110000L))
      app.jobTagToConnectOpId.put(jobTag, "op-1")
      assert(app.isConnectMode, "Should detect Connect mode after seeding")

      // Drive a SparkListenerSQLExecutionStart tagged with the Connect operation.
      val evt = buildSQLStartEvent(executionId = 42L, jobTags = Set(jobTag))
      app.processEvent(evt)

      assert(app.operationIdToSqlIds.contains("op-1"),
        "operationIdToSqlIds should contain op-1 after SQL start")
      assert(app.operationIdToSqlIds("op-1").contains(42L),
        "op-1 should map to executionId 42")

      // An untagged SQL execution should not map to any Connect op.
      val untagged = buildSQLStartEvent(executionId = 43L, jobTags = Set.empty)
      app.processEvent(untagged)
      assert(app.operationIdToSqlIds("op-1") == mutable.HashSet(42L),
        "Untagged execution should not be attributed to op-1")
    }
  }

  test("operationIdToJobIds populated from SparkListenerJobStart spark.job.tags") {
    withEventLog(logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      // Seed Connect state as if a ConnectOperationStarted event had fired.
      val jobTag =
        "SparkConnect_OperationTag_User_u_Session_s_Operation_op-2"
      app.connectOperations.put("op-2", new ConnectOperationInfo(
        operationId = "op-2",
        sessionId = "s",
        userId = "u",
        jobTag = jobTag,
        statementText = "range(0, 10)",
        startTime = 110000L))
      app.jobTagToConnectOpId.put(jobTag, "op-2")
      assert(app.isConnectMode, "Should detect Connect mode after seeding")

      // Simulate a JobStart whose spark.job.tags mixes the Connect tag with a
      // user-supplied tag (e.g., from spark.addTag).
      val props = new java.util.Properties()
      props.setProperty("spark.job.tags", s"$jobTag,custom-user-tag")
      val evt = SparkListenerJobStart(
        jobId = 7, time = 2000L, stageInfos = Nil, properties = props)
      app.processEvent(evt)

      assert(app.operationIdToJobIds("op-2") == mutable.HashSet(7),
        "op-2 should map to exactly jobId 7")
      assert(app.operationIdToJobIds.size == 1,
        "No spurious opIds should be created from user tags")
    }
  }

  test("operationIdToJobIds stays empty when app is not in Connect mode") {
    withEventLog(logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      assert(!app.isConnectMode, "Fresh app should not be in Connect mode")

      val props = new java.util.Properties()
      props.setProperty("spark.job.tags",
        "SparkConnect_OperationTag_User_u_Session_s_Operation_op-x")
      val evt = SparkListenerJobStart(
        jobId = 8, time = 3000L, stageInfos = Nil, properties = props)
      app.processEvent(evt)

      assert(app.operationIdToJobIds.isEmpty,
        "Non-Connect app should not populate operationIdToJobIds")
    }
  }
}
