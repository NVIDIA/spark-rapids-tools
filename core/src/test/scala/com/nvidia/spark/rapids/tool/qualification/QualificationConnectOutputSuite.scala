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

package com.nvidia.spark.rapids.tool.qualification

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator

import scala.collection.mutable

import com.nvidia.spark.rapids.BaseNoSparkSuite
import com.nvidia.spark.rapids.tool.profiling.{ConnectOperationInfo, ConnectSessionInfo}
import com.nvidia.spark.rapids.tool.views.QualRawReportGenerator

import org.apache.spark.sql.TrampolineUtil
import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo
import org.apache.spark.sql.rapids.tool.util.UTF8Source

/**
 * Verifies that the qualification raw-metrics writer emits the same Spark
 * Connect tables and statement sidecars as profiling, but under
 * `raw_metrics/<appId>/`.
 */
class QualificationConnectOutputSuite extends BaseNoSparkSuite {

  private val logStartEvent =
    """{"Event":"SparkListenerLogStart","Spark Version":"3.5.0"}"""
  private val appStartEvent =
    """{"Event":"SparkListenerApplicationStart","App Name":"QualConnectOutputTest",""" +
      """"App ID":"local-qual-connect-output","Timestamp":100000,"User":"testUser"}"""
  private val envUpdateEvent =
    """{"Event":"SparkListenerEnvironmentUpdate","JVM Information":{},""" +
      """"Spark Properties":{"spark.master":"local[*]"},""" +
      """"Hadoop Properties":{},"System Properties":{"file.encoding":"UTF-8"},""" +
      """"Classpath Entries":{}}"""
  private val appEndEvent =
    """{"Event":"SparkListenerApplicationEnd","Timestamp":200000}"""

  private def withQualificationApp(events: String*)(verify: QualificationAppInfo => Unit): Unit = {
    val content = events.mkString("\n")
    TrampolineUtil.withTempDir { tempDir =>
      val path = Paths.get(tempDir.getAbsolutePath, "test_eventlog")
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
      val app = createAppFromEventlog(path.toString)
      verify(app)
    }
  }

  private def deleteRecursively(root: Path): Unit = {
    if (Files.exists(root)) {
      val stream = Files.walk(root)
      try {
        stream.sorted(Comparator.reverseOrder[Path]())
          .forEach(p => Files.deleteIfExists(p))
      } finally {
        stream.close()
      }
    }
  }

  private def readAllLines(path: Path): Seq[String] = {
    val src = UTF8Source.fromFile(path.toFile)
    try {
      src.getLines().toList
    } finally {
      src.close()
    }
  }

  test("qualification raw metrics emit connect CSVs and statement sidecars") {
    withQualificationApp(logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      app.connectSessions.put("sess-1", new ConnectSessionInfo(
        sessionId = "sess-1",
        userId = "alice",
        startTime = 100L,
        endTime = Some(500L)))

      val op1StatementText = "SELECT 1 plan body"
      app.connectOperations.put("op-1", new ConnectOperationInfo(
        operationId = "op-1",
        sessionId = "sess-1",
        userId = "alice",
        jobTag = "SparkConnect_OperationTag_User_alice_Session_sess-1_Operation_op-1",
        statementText = op1StatementText,
        startTime = 110L,
        analyzeTime = Some(120L),
        readyForExecTime = Some(130L),
        finishTime = Some(150L),
        closeTime = Some(160L),
        producedRowCount = Some(10L)))
      app.operationIdToSqlIds.put("op-1", mutable.HashSet(42L))
      app.operationIdToJobIds.put("op-1", mutable.HashSet(7))

      app.connectOperations.put("op-2", new ConnectOperationInfo(
        operationId = "op-2",
        sessionId = "sess-1",
        userId = "alice",
        jobTag = "SparkConnect_OperationTag_User_alice_Session_sess-1_Operation_op-2",
        statementText = "",
        startTime = 200L,
        failTime = Some(260L),
        errorMessage = Some("boom")))

      val tmpDir = Files.createTempDirectory("qual-connect-out-")
      try {
        QualRawReportGenerator.generateRawMetricQualViewAndGetDataSourceInfo(tmpDir.toString, app)

        val appDir = tmpDir.resolve("raw_metrics").resolve(app.appId)
        val sessionsCsv = appDir.resolve("connect_sessions.csv")
        val operationsCsv = appDir.resolve("connect_operations.csv")
        assert(Files.exists(sessionsCsv), s"expected $sessionsCsv to exist")
        assert(Files.exists(operationsCsv), s"expected $operationsCsv to exist")

        val sessionLines = readAllLines(sessionsCsv)
        assert(sessionLines.size == 2, s"unexpected session rows: $sessionLines")
        assert(sessionLines.head ==
          "appID,sessionId,userId,startTime,endTime,durationMs,operationCount",
          s"unexpected session header: ${sessionLines.head}")

        val opLines = readAllLines(operationsCsv)
        assert(opLines.size == 3, s"unexpected operation rows: $opLines")
        assert(opLines.head.contains("statementFile"),
          s"connect_operations header should include statementFile: ${opLines.head}")

        val statementsDir = appDir.resolve("connect_statements")
        assert(Files.isDirectory(statementsDir),
          s"expected $statementsDir directory for op-1 sidecar")
        val op1Sidecar = statementsDir.resolve("op-1.txt")
        val op2Sidecar = statementsDir.resolve("op-2.txt")
        assert(Files.exists(op1Sidecar), s"expected $op1Sidecar to exist")
        assert(!Files.exists(op2Sidecar),
          s"expected $op2Sidecar not to exist for empty statementText")
        val op1Contents = new String(Files.readAllBytes(op1Sidecar), StandardCharsets.UTF_8)
        assert(op1Contents == op1StatementText,
          s"sidecar contents mismatch: $op1Contents vs $op1StatementText")
      } finally {
        deleteRecursively(tmpDir)
      }
    }
  }

  test("qualification raw metrics emit connect_sessions.csv for session-only Connect logs") {
    withQualificationApp(logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      app.connectSessions.put("sess-1", new ConnectSessionInfo(
        sessionId = "sess-1",
        userId = "alice",
        startTime = 100L,
        endTime = Some(500L)))

      assert(app.isConnectMode, "Session-only qualification app should report Connect mode")

      val tmpDir = Files.createTempDirectory("qual-connect-out-")
      try {
        QualRawReportGenerator.generateRawMetricQualViewAndGetDataSourceInfo(tmpDir.toString, app)

        val appDir = tmpDir.resolve("raw_metrics").resolve(app.appId)
        val sessionsCsv = appDir.resolve("connect_sessions.csv")
        val operationsCsv = appDir.resolve("connect_operations.csv")
        assert(Files.exists(sessionsCsv), s"expected $sessionsCsv to exist")
        assert(!Files.exists(operationsCsv),
          s"expected no $operationsCsv for session-only Connect app")

        val sessionLines = readAllLines(sessionsCsv)
        assert(sessionLines.size == 2, s"unexpected session rows: $sessionLines")
        assert(sessionLines(1).contains("sess-1"),
          s"expected sess-1 row in session output: ${sessionLines(1)}")
      } finally {
        deleteRecursively(tmpDir)
      }
    }
  }
}
