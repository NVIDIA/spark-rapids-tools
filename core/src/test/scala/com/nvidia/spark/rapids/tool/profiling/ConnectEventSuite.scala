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

import com.nvidia.spark.rapids.BaseNoSparkSuite
import com.nvidia.spark.rapids.tool.EventLogPathProcessor

import org.apache.spark.sql.TrampolineUtil
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil

/**
 * Tests for Spark Connect event parsing.
 * Requires spark-connect on the test classpath (Spark 3.5+ profiles).
 */
class ConnectEventSuite extends BaseNoSparkSuite {

  private val hadoopConf = RapidsToolsConfUtil.newHadoopConf()

  /** Checks whether Connect event classes are available on the classpath. */
  private def checkConnectClassesAvailable(): (Boolean, String) = {
    val available = try {
      Class.forName(
        "org.apache.spark.sql.connect.service.SparkListenerConnectSessionStarted")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
    (available, "spark-connect jar not on classpath (requires Spark 3.5+ test profile)")
  }

  // --- Standard boilerplate events for a valid event log ---

  private val logStartEvent =
    """{"Event":"SparkListenerLogStart","Spark Version":"3.5.0"}"""
  private val appStartEvent =
    """{"Event":"SparkListenerApplicationStart","App Name":"ConnectTest",""" +
      """"App ID":"local-connecttest","Timestamp":100000,"User":"testUser"}"""
  private val envUpdateEvent =
    """{"Event":"SparkListenerEnvironmentUpdate","JVM Information":{},""" +
      """"Spark Properties":{"spark.master":"local[*]"},""" +
      """"Hadoop Properties":{},"System Properties":{"file.encoding":"UTF-8"},""" +
      """"Classpath Entries":{}}"""
  private val appEndEvent =
    """{"Event":"SparkListenerApplicationEnd","Timestamp":200000}"""

  // --- Connect event JSON lines ---

  private val sessionStartedEvent =
    """{"Event":"org.apache.spark.sql.connect.service.SparkListenerConnectSessionStarted",""" +
      """"sessionId":"sess-aaa-111","userId":"userA","eventTime":110000,""" +
      """"extraTags":{}}"""

  private val sessionClosedEvent =
    """{"Event":"org.apache.spark.sql.connect.service.SparkListenerConnectSessionClosed",""" +
      """"sessionId":"sess-aaa-111","userId":"userA","eventTime":190000,""" +
      """"extraTags":{}}"""

  // scalastyle:off line.size.limit
  private val opStartedEvent =
    """{"Event":"org.apache.spark.sql.connect.service.SparkListenerConnectOperationStarted",""" +
      """"jobTag":"SparkConnect_OperationTag_User_userA_Session_sess-aaa-111_Operation_op-bbb-222",""" +
      """"operationId":"op-bbb-222","eventTime":120000,""" +
      """"sessionId":"sess-aaa-111","userId":"userA","userName":"",""" +
      """"statementText":"common { plan_id: 0 } range { start: 0 end: 100 step: 1 }",""" +
      """"sparkSessionTags":[],"extraTags":{}}"""
  // scalastyle:on line.size.limit

  // scalastyle:off line.size.limit
  private val opAnalyzedEvent =
    """{"Event":"org.apache.spark.sql.connect.service.SparkListenerConnectOperationAnalyzed",""" +
      """"jobTag":"SparkConnect_OperationTag_User_userA_Session_sess-aaa-111_Operation_op-bbb-222",""" +
      """"operationId":"op-bbb-222","eventTime":121000,"extraTags":{}}"""

  private val opReadyEvent =
    """{"Event":"org.apache.spark.sql.connect.service.SparkListenerConnectOperationReadyForExecution",""" +
      """"jobTag":"SparkConnect_OperationTag_User_userA_Session_sess-aaa-111_Operation_op-bbb-222",""" +
      """"operationId":"op-bbb-222","eventTime":121500,"extraTags":{}}"""

  private val opFinishedEvent =
    """{"Event":"org.apache.spark.sql.connect.service.SparkListenerConnectOperationFinished",""" +
      """"jobTag":"SparkConnect_OperationTag_User_userA_Session_sess-aaa-111_Operation_op-bbb-222",""" +
      """"operationId":"op-bbb-222","eventTime":125000,"producedRowCount":10,"extraTags":{}}"""

  private val opClosedEvent =
    """{"Event":"org.apache.spark.sql.connect.service.SparkListenerConnectOperationClosed",""" +
      """"jobTag":"SparkConnect_OperationTag_User_userA_Session_sess-aaa-111_Operation_op-bbb-222",""" +
      """"operationId":"op-bbb-222","eventTime":125500,"extraTags":{}}"""

  private val opFailedEvent =
    """{"Event":"org.apache.spark.sql.connect.service.SparkListenerConnectOperationFailed",""" +
      """"jobTag":"SparkConnect_OperationTag_User_userA_Session_sess-aaa-111_Operation_op-bbb-222",""" +
      """"operationId":"op-bbb-222","eventTime":125000,""" +
      """"errorMessage":"ModuleNotFoundError: No module named 'helper'","extraTags":{}}"""

  private val opCanceledEvent =
    """{"Event":"org.apache.spark.sql.connect.service.SparkListenerConnectOperationCanceled",""" +
      """"jobTag":"SparkConnect_OperationTag_User_userA_Session_sess-aaa-111_Operation_op-bbb-222",""" +
      """"operationId":"op-bbb-222","eventTime":125000,"extraTags":{}}"""
  // scalastyle:on line.size.limit

  /**
   * Helper to write synthetic event log lines to a temp file and process through
   * ApplicationInfo, then run assertions.
   */
  private def withConnectEventLog(events: String*)(verify: ApplicationInfo => Unit): Unit = {
    val content = events.mkString("\n")
    TrampolineUtil.withTempDir { tempDir =>
      val path = Paths.get(tempDir.getAbsolutePath, "test_eventlog")
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
      val app = new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path.toString, hadoopConf).head._1)
      verify(app)
    }
  }

  // --- Connect-specific tests (skipped when spark-connect not on classpath) ---

  runConditionalTest(
    "Connect session and operation lifecycle parsed correctly",
    checkConnectClassesAvailable) {
    withConnectEventLog(
      logStartEvent, appStartEvent, envUpdateEvent,
      sessionStartedEvent,
      opStartedEvent, opAnalyzedEvent, opReadyEvent,
      opFinishedEvent, opClosedEvent,
      sessionClosedEvent,
      appEndEvent) { app =>
      assert(app.isConnectMode, "Should detect Connect mode")

      // Verify session
      assert(app.connectSessions.size == 1)
      val session = app.connectSessions("sess-aaa-111")
      assert(session.userId == "userA")
      assert(session.startTime == 110000L)
      assert(session.endTime.contains(190000L))

      // Verify operation lifecycle
      assert(app.connectOperations.size == 1)
      val op = app.connectOperations("op-bbb-222")
      assert(op.sessionId == "sess-aaa-111")
      assert(op.userId == "userA")
      assert(op.startTime == 120000L)
      assert(op.analyzeTime.contains(121000L))
      assert(op.readyForExecTime.contains(121500L))
      assert(op.finishTime.contains(125000L))
      assert(op.closeTime.contains(125500L))
      assert(op.producedRowCount.contains(10L))
      assert(op.errorMessage.isEmpty)
      assert(!op.isCanceled)

      // Verify jobTag correlation index
      val expectedTag =
        "SparkConnect_OperationTag_User_userA_Session_sess-aaa-111_Operation_op-bbb-222"
      assert(app.jobTagToConnectOpId.get(expectedTag).contains("op-bbb-222"))
    }
  }

  runConditionalTest(
    "Connect failed operation captures error message",
    checkConnectClassesAvailable) {
    withConnectEventLog(
      logStartEvent, appStartEvent, envUpdateEvent,
      sessionStartedEvent,
      opStartedEvent, opAnalyzedEvent, opFailedEvent,
      sessionClosedEvent,
      appEndEvent) { app =>
      assert(app.isConnectMode)
      val op = app.connectOperations("op-bbb-222")
      assert(op.analyzeTime.contains(121000L))
      assert(op.errorMessage.contains("ModuleNotFoundError: No module named 'helper'"))
      assert(op.finishTime.isEmpty)
      assert(op.closeTime.isEmpty)
    }
  }

  runConditionalTest(
    "Connect canceled operation sets isCanceled flag",
    checkConnectClassesAvailable) {
    withConnectEventLog(
      logStartEvent, appStartEvent, envUpdateEvent,
      sessionStartedEvent,
      opStartedEvent, opCanceledEvent,
      sessionClosedEvent,
      appEndEvent) { app =>
      assert(app.isConnectMode)
      val op = app.connectOperations("op-bbb-222")
      assert(op.isCanceled)
    }
  }

  runConditionalTest(
    "Connect session without close event leaves endTime as None",
    checkConnectClassesAvailable) {
    withConnectEventLog(
      logStartEvent, appStartEvent, envUpdateEvent,
      sessionStartedEvent,
      opStartedEvent, opFinishedEvent, opClosedEvent,
      // No sessionClosedEvent — server was killed
      appEndEvent) { app =>
      assert(app.isConnectMode)
      val session = app.connectSessions("sess-aaa-111")
      assert(session.endTime.isEmpty, "Session endTime should be None without close event")
    }
  }

  runConditionalTest(
    "Connect statementText captured from OperationStarted",
    checkConnectClassesAvailable) {
    withConnectEventLog(
      logStartEvent, appStartEvent, envUpdateEvent,
      sessionStartedEvent,
      opStartedEvent, opFinishedEvent, opClosedEvent,
      sessionClosedEvent,
      appEndEvent) { app =>
      val op = app.connectOperations("op-bbb-222")
      assert(op.statementText.contains("range"))
      assert(op.statementText.contains("plan_id: 0"))
    }
  }

  // --- Non-conditional tests (always run) ---

  test("Non-Connect event log has isConnectMode == false") {
    withConnectEventLog(
      logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      assert(!app.isConnectMode)
      assert(app.connectSessions.isEmpty)
      assert(app.connectOperations.isEmpty)
      assert(app.jobTagToConnectOpId.isEmpty)
    }
  }
}
