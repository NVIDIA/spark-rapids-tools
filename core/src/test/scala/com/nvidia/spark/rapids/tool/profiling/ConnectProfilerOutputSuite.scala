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
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator

import scala.collection.mutable

import com.nvidia.spark.rapids.BaseNoSparkSuite
import com.nvidia.spark.rapids.tool.EventLogPathProcessor
import com.nvidia.spark.rapids.tool.views.OutHeaderRegistry

import org.apache.spark.sql.TrampolineUtil
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.util.{RapidsToolsConfUtil, UTF8Source}

/**
 * Tests Task 5 of Spark Connect Phase 3 (#2065): wiring
 * [[ConnectSessionProfileResult]] / [[ConnectOperationProfileResult]] into the
 * Profiler's per-app CSV output. Verifies that `connect_sessions.csv` and
 * `connect_operations.csv` are produced in Connect mode and absent otherwise.
 */
class ConnectProfilerOutputSuite extends BaseNoSparkSuite {

  private val hadoopConf = RapidsToolsConfUtil.newHadoopConf()

  private val logStartEvent =
    """{"Event":"SparkListenerLogStart","Spark Version":"3.5.0"}"""
  private val appStartEvent =
    """{"Event":"SparkListenerApplicationStart","App Name":"ConnectOutputTest",""" +
      """"App ID":"local-connect-output","Timestamp":100000,"User":"testUser"}"""
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

  test("writeConnectTables emits connect_sessions.csv and connect_operations.csv " +
    "when isConnectMode") {
    withEventLog(logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      // Seed two sessions and two operations (one SUCCEEDED, one FAILED) plus
      // sqlID / jobID correlations, so both result types exercise their
      // convertToCSVSeq paths.
      app.connectSessions.put("sess-1", new ConnectSessionInfo(
        sessionId = "sess-1",
        userId = "alice",
        startTime = 100L,
        endTime = Some(500L)))

      val op1StatementText = "SELECT 1 plan body"
      val op1 = new ConnectOperationInfo(
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
        producedRowCount = Some(10L))
      app.connectOperations.put("op-1", op1)
      app.operationIdToSqlIds.put("op-1", mutable.HashSet(42L))
      app.operationIdToJobIds.put("op-1", mutable.HashSet(7))

      val op2 = new ConnectOperationInfo(
        operationId = "op-2",
        sessionId = "sess-1",
        userId = "alice",
        jobTag = "SparkConnect_OperationTag_User_alice_Session_sess-1_Operation_op-2",
        statementText = "",
        startTime = 200L,
        failTime = Some(260L),
        errorMessage = Some("boom"))
      app.connectOperations.put("op-2", op2)

      assert(app.isConnectMode, "Seeded app should report Connect mode")

      val tmpDir = Files.createTempDirectory("prof-connect-out-").toFile
      try {
        val writer = new ProfileOutputWriter(tmpDir.getAbsolutePath, "profile",
          numOutputRows = 1000, outputCSV = true)
        try {
          Profiler.writeConnectTables(writer, app)
        } finally {
          writer.close()
        }

        val sessionsCsv = Paths.get(tmpDir.getAbsolutePath, "connect_sessions.csv")
        val operationsCsv = Paths.get(tmpDir.getAbsolutePath, "connect_operations.csv")
        assert(Files.exists(sessionsCsv), s"expected $sessionsCsv to exist")
        assert(Files.exists(operationsCsv), s"expected $operationsCsv to exist")

        val sessionLines = readAllLines(sessionsCsv)
        // Header + 1 session row
        assert(sessionLines.size == 2, s"unexpected session rows: $sessionLines")
        assert(sessionLines.head ==
          "appID,sessionId,userId,startTime,endTime,durationMs,operationCount",
          s"unexpected session header: ${sessionLines.head}")

        val opLines = readAllLines(operationsCsv)
        // Header + 2 op rows
        assert(opLines.size == 3, s"unexpected operation rows: $opLines")
        assert(opLines.head ==
          "appID,operationId,sessionId,userId,jobTag,startTime,finishTime,closeTime," +
            "failTime,cancelTime,durationMs,status,errorMessage,sqlIds,jobIds," +
            "statementFile,statementTruncated",
          s"unexpected operation header: ${opLines.head}")
        // Per-column parse: find the `status` column index from the registry and
        // assert exactly one SUCCEEDED and one FAILED row. Rows in this test case
        // contain no embedded commas, so simple string split is sufficient.
        // CSV string columns are wrapped in double quotes by reformatCSVString;
        // strip surrounding quotes before comparison.
        val opHeaders = OutHeaderRegistry.outputHeaders("ConnectOperationProfileResult")
        val statusIdx = opHeaders.indexOf("status")
        assert(statusIdx >= 0, s"status column missing from registry headers: ${
          opHeaders.mkString(",")}")
        val statusValues = opLines.tail.map(_.split(",", -1)(statusIdx).stripPrefix("\"")
          .stripSuffix("\""))
        assert(statusValues.count(_ == "SUCCEEDED") == 1,
          s"expected exactly one SUCCEEDED row: $statusValues")
        assert(statusValues.count(_ == "FAILED") == 1,
          s"expected exactly one FAILED row: $statusValues")

        // Sidecars are disabled by default. The statementFile column remains
        // empty until the caller opts into writing sidecars.
        val statementsDir = Paths.get(tmpDir.getAbsolutePath,
          ConnectStatementWriter.SUB_DIR)
        assert(!Files.exists(statementsDir),
          s"expected no $statementsDir when sidecars are disabled")
        val opIdIdx = opHeaders.indexOf("operationId")
        val statementFileIdx = opHeaders.indexOf("statementFile")
        assert(statementFileIdx >= 0,
          s"statementFile column missing from registry headers: ${
            opHeaders.mkString(",")}")
        val stmtFileByOp = opLines.tail.map { line =>
          val cols = line.split(",", -1)
          val opId = cols(opIdIdx).stripPrefix("\"").stripSuffix("\"")
          val stmtFile = cols(statementFileIdx).stripPrefix("\"").stripSuffix("\"")
          opId -> stmtFile
        }.toMap
        assert(stmtFileByOp("op-1") == "",
          s"expected op-1 statementFile empty by default, got ${stmtFileByOp("op-1")}")
        assert(stmtFileByOp("op-2") == "",
          s"expected op-2 statementFile empty, got ${stmtFileByOp("op-2")}")
      } finally {
        deleteRecursively(tmpDir.toPath)
      }
    }
  }

  test("writeConnectTables writes no files when app is not in Connect mode") {
    withEventLog(logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      assert(!app.isConnectMode, "Fresh app should not be in Connect mode")

      val tmpDir = Files.createTempDirectory("prof-connect-out-").toFile
      try {
        val writer = new ProfileOutputWriter(tmpDir.getAbsolutePath, "profile",
          numOutputRows = 1000, outputCSV = true)
        try {
          Profiler.writeConnectTables(writer, app)
        } finally {
          writer.close()
        }

        val sessionsCsv = Paths.get(tmpDir.getAbsolutePath, "connect_sessions.csv")
        val operationsCsv = Paths.get(tmpDir.getAbsolutePath, "connect_operations.csv")
        assert(!Files.exists(sessionsCsv),
          s"expected no $sessionsCsv for non-Connect app")
        assert(!Files.exists(operationsCsv),
          s"expected no $operationsCsv for non-Connect app")
      } finally {
        deleteRecursively(tmpDir.toPath)
      }
    }
  }

  test("writeConnectTables emits connect_sessions.csv for session-only Connect logs") {
    withEventLog(logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      app.connectSessions.put("sess-1", new ConnectSessionInfo(
        sessionId = "sess-1",
        userId = "alice",
        startTime = 100L,
        endTime = Some(500L)))

      assert(app.isConnectMode, "Session-only app should report Connect mode")

      val tmpDir = Files.createTempDirectory("prof-connect-out-").toFile
      try {
        val writer = new ProfileOutputWriter(tmpDir.getAbsolutePath, "profile",
          numOutputRows = 1000, outputCSV = true)
        try {
          Profiler.writeConnectTables(writer, app)
        } finally {
          writer.close()
        }

        val sessionsCsv = Paths.get(tmpDir.getAbsolutePath, "connect_sessions.csv")
        val operationsCsv = Paths.get(tmpDir.getAbsolutePath, "connect_operations.csv")
        assert(Files.exists(sessionsCsv), s"expected $sessionsCsv to exist")
        assert(!Files.exists(operationsCsv),
          s"expected no $operationsCsv for session-only Connect app")

        val sessionLines = readAllLines(sessionsCsv)
        assert(sessionLines.size == 2, s"unexpected session rows: $sessionLines")
        assert(sessionLines(1).contains("sess-1"),
          s"expected sess-1 row in session output: ${sessionLines(1)}")
      } finally {
        deleteRecursively(tmpDir.toPath)
      }
    }
  }

  test("writeConnectTables writes statement sidecars only when enabled") {
    withEventLog(logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      val op1StatementText = "SELECT 1 plan body"
      app.connectSessions.put("sess-1", new ConnectSessionInfo(
        sessionId = "sess-1",
        userId = "alice",
        startTime = 100L,
        endTime = Some(500L)))
      app.connectOperations.put("op-1", new ConnectOperationInfo(
        operationId = "op-1",
        sessionId = "sess-1",
        userId = "alice",
        jobTag = "SparkConnect_OperationTag_User_alice_Session_sess-1_Operation_op-1",
        statementText = op1StatementText,
        startTime = 110L))
      app.connectOperations.put("op-2", new ConnectOperationInfo(
        operationId = "op-2",
        sessionId = "sess-1",
        userId = "alice",
        jobTag = "SparkConnect_OperationTag_User_alice_Session_sess-1_Operation_op-2",
        statementText = "",
        startTime = 120L))

      val tmpDir = Files.createTempDirectory("prof-connect-out-").toFile
      try {
        val writer = new ProfileOutputWriter(tmpDir.getAbsolutePath, "profile",
          numOutputRows = 1000, outputCSV = true)
        try {
          Profiler.writeConnectTables(writer, app, writeStatementSidecars = true)
        } finally {
          writer.close()
        }

        val opLines = readAllLines(Paths.get(tmpDir.getAbsolutePath, "connect_operations.csv"))
        val opHeaders = OutHeaderRegistry.outputHeaders("ConnectOperationProfileResult")
        val opIdIdx = opHeaders.indexOf("operationId")
        val statementFileIdx = opHeaders.indexOf("statementFile")
        val stmtFileByOp = opLines.tail.map { line =>
          val cols = line.split(",", -1)
          val opId = cols(opIdIdx).stripPrefix("\"").stripSuffix("\"")
          val stmtFile = cols(statementFileIdx).stripPrefix("\"").stripSuffix("\"")
          opId -> stmtFile
        }.toMap

        val statementsDir = Paths.get(tmpDir.getAbsolutePath, ConnectStatementWriter.SUB_DIR)
        val op1Sidecar = statementsDir.resolve("op-1.txt")
        val op2Sidecar = statementsDir.resolve("op-2.txt")
        assert(Files.isDirectory(statementsDir),
          s"expected $statementsDir directory when sidecars are enabled")
        assert(Files.exists(op1Sidecar), s"expected $op1Sidecar to exist")
        assert(!Files.exists(op2Sidecar),
          s"expected $op2Sidecar not to exist for empty statementText")
        val op1Contents = new String(Files.readAllBytes(op1Sidecar), StandardCharsets.UTF_8)
        assert(op1Contents == op1StatementText,
          s"sidecar contents mismatch: $op1Contents vs $op1StatementText")
        assert(stmtFileByOp("op-1") == "op-1.txt",
          s"expected op-1 statementFile=op-1.txt, got ${stmtFileByOp("op-1")}")
        assert(stmtFileByOp("op-2") == "",
          s"expected op-2 statementFile empty, got ${stmtFileByOp("op-2")}")
      } finally {
        deleteRecursively(tmpDir.toPath)
      }
    }
  }
}
