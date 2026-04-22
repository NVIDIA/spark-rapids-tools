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

import com.nvidia.spark.rapids.tool.views.OutHeaderRegistry
import org.scalatest.funsuite.AnyFunSuite

class ConnectProfileResultsSuite extends AnyFunSuite {

  private def operationHeaders: Array[String] =
    OutHeaderRegistry.outputHeaders("ConnectOperationProfileResult")

  private def operationCol(row: ConnectOperationProfileResult, name: String): String =
    row.convertToSeq()(operationHeaders.indexOf(name))

  test("ConnectSessionProfileResult emits correct raw columns") {
    val row = ConnectSessionProfileResult(
      appId = "app-1", sessionId = "s1", userId = "u1",
      startTime = 1000L, endTime = Some(5000L), operationCount = 3)
    assert(row.convertToSeq().toSeq ===
      Seq("app-1", "s1", "u1", "1000", "5000", "4000", "3"))

    val open = ConnectSessionProfileResult(
      appId = "app-2", sessionId = "s2", userId = "u2",
      startTime = 1000L, endTime = None, operationCount = 0)
    val openSeq = open.convertToSeq()
    // endTime is rendered as null (orNull) when absent, matching the convention
    // used by neighboring ProfileResult classes in ProfileClassWarehouse.scala.
    assert(openSeq(0) == "app-2")
    assert(openSeq(1) == "s2")
    assert(openSeq(2) == "u2")
    assert(openSeq(3) == "1000")
    assert(openSeq(4) == null)
    assert(openSeq(5) == "-1")
    assert(openSeq(6) == "0")
  }

  test("ConnectSessionProfileResult.convertToCSVSeq wraps string fields in quotes") {
    val row = ConnectSessionProfileResult(
      appId = "app,comma", sessionId = "s\"quote", userId = "u1",
      startTime = 1000L, endTime = Some(5000L), operationCount = 3)
    val csv = row.convertToCSVSeq()
    // reformatCSVString escapes inner " by doubling and wraps the result in "..."
    assert(csv(0) == "\"app,comma\"")
    assert(csv(1) == "\"s\"\"quote\"")
    assert(csv(2) == "\"u1\"")
    // numeric fields stay raw
    assert(csv(3) == "1000")
    assert(csv(4) == "5000")
    assert(csv(5) == "4000")
    assert(csv(6) == "3")
  }

  test("OutHeaderRegistry includes ConnectSession and ConnectOperation headers") {
    assert(OutHeaderRegistry.outputHeaders.contains("ConnectSessionProfileResult"))
    assert(OutHeaderRegistry.outputHeaders.contains("ConnectOperationProfileResult"))
    assert(OutHeaderRegistry.outputHeaders("ConnectSessionProfileResult").toSeq ===
      Seq("appID", "sessionId", "userId", "startTime", "endTime", "durationMs",
        "operationCount"))
  }

  test("row array length matches header count") {
    val sess = ConnectSessionProfileResult("a", "s", "u", 0L, None, 0L)
    assert(sess.convertToCSVSeq().length == sess.outputHeaders.length)
    assert(sess.convertToSeq().length == sess.outputHeaders.length)

    val op = new ConnectOperationInfo("o", "s", "u", "t", "", 0L)
    val opRow = ConnectOperationProfileResult.from("a", op, Seq.empty, Seq.empty, None)
    assert(opRow.convertToCSVSeq().length == opRow.outputHeaders.length)
    assert(opRow.convertToSeq().length == opRow.outputHeaders.length)
  }

  test("ConnectOperationProfileResult derives status and core columns correctly") {
    val op = new ConnectOperationInfo(
      operationId = "op", sessionId = "s", userId = "u",
      jobTag = "tag", statementText = "SELECT 1", startTime = 100L)
    op.analyzeTime = Some(200L)
    op.readyForExecTime = Some(300L)
    op.finishTime = Some(500L)
    op.closeTime = Some(600L)
    op.producedRowCount = Some(1L)
    val row = ConnectOperationProfileResult.from(
      appId = "app-1", op = op, sqlIds = Seq(42L), jobIds = Seq(7),
      statementFile = Some("op.txt"))
    assert(operationCol(row, "operationId") == "op")
    assert(operationCol(row, "status") == "SUCCEEDED")
    assert(operationCol(row, "durationMs") == "500")
    assert(operationCol(row, "finishTime") == "500")
    assert(operationCol(row, "closeTime") == "600")
    assert(operationCol(row, "sqlIds") == "42")
    assert(operationCol(row, "jobIds") == "7")
    assert(operationCol(row, "statementFile") == "op.txt")
    assert(operationCol(row, "statementTruncated") == "false")
  }

  test("ConnectOperationProfileResult derives FAILED status with errorMessage") {
    val op = new ConnectOperationInfo(
      operationId = "op-f", sessionId = "s", userId = "u",
      jobTag = "tag", statementText = "bad", startTime = 100L)
    op.failTime = Some(150L)
    op.errorMessage = Some("boom")
    val row = ConnectOperationProfileResult.from(
      appId = "app", op = op, sqlIds = Seq.empty, jobIds = Seq.empty,
      statementFile = None)
    assert(operationCol(row, "status") == "FAILED")
    assert(operationCol(row, "errorMessage") == "boom")
    assert(operationCol(row, "statementFile") == "")
    assert(operationCol(row, "durationMs") == "50")
    assert(operationCol(row, "failTime") == "150")
    assert(operationCol(row, "sqlIds") == "")
  }

  test("ConnectOperationProfileResult derives CANCELED status takes priority over FAILED") {
    val op = new ConnectOperationInfo(
      operationId = "op-c", sessionId = "s", userId = "u",
      jobTag = "tag", statementText = "", startTime = 100L)
    op.cancelTime = Some(150L)
    op.failTime = Some(155L)
    val row = ConnectOperationProfileResult.from("app", op, Seq.empty, Seq.empty, None)
    assert(operationCol(row, "status") == "CANCELED")
  }

  test("ConnectOperationProfileResult derives RUNNING status when no terminal timestamp") {
    val op = new ConnectOperationInfo(
      operationId = "op-r", sessionId = "s", userId = "u",
      jobTag = "tag", statementText = "", startTime = 100L)
    val row = ConnectOperationProfileResult.from("app", op, Seq.empty, Seq.empty, None)
    assert(operationCol(row, "status") == "RUNNING")
    assert(operationCol(row, "durationMs") == "-1")
  }

  test("ConnectOperationProfileResult detects truncated statementText") {
    val op = new ConnectOperationInfo(
      operationId = "op-t", sessionId = "s", userId = "u",
      jobTag = "tag",
      statementText = "plan body ... " + ConnectOperationProfileResult.TruncationMarker +
        "1234)] more",
      startTime = 100L)
    val row = ConnectOperationProfileResult.from("app", op, Seq.empty, Seq.empty, None)
    assert(operationCol(row, "statementTruncated") == "true")
  }

  test("ConnectOperationProfileResult does not infer truncation from structural elision") {
    val op = new ConnectOperationInfo(
      operationId = "op-struct", sessionId = "s", userId = "u",
      jobTag = "tag",
      statementText =
        """filter {
          |  input {
          |    common {}
          |    join {}
          |  }
          |}""".stripMargin,
      startTime = 100L)
    val row = ConnectOperationProfileResult.from("app", op, Seq.empty, Seq.empty, None)
    assert(operationCol(row, "statementTruncated") == "false")
  }

  test("ConnectOperationProfileResult.convertToCSVSeq wraps string fields in quotes") {
    val op = new ConnectOperationInfo(
      operationId = "op,x", sessionId = "s\"q", userId = "u1",
      jobTag = "tag", statementText = "SELECT 1", startTime = 100L)
    op.errorMessage = Some("bad, msg")
    val row = ConnectOperationProfileResult.from(
      appId = "app-1", op = op, sqlIds = Seq(1L, 2L), jobIds = Seq(3, 4),
      statementFile = Some("f,x.txt"))
    val csv = row.convertToCSVSeq()
    def csvCol(name: String): String = csv(operationHeaders.indexOf(name))
    assert(csvCol("appID") == "\"app-1\"")
    assert(csvCol("operationId") == "\"op,x\"")
    assert(csvCol("sessionId") == "\"s\"\"q\"")
    assert(csvCol("userId") == "\"u1\"")
    assert(csvCol("errorMessage") == "\"bad, msg\"")
    assert(csvCol("sqlIds") == "\"1;2\"")
    assert(csvCol("jobIds") == "\"3;4\"")
    assert(csvCol("statementFile") == "\"f,x.txt\"")
    // numeric/boolean fields remain raw
    assert(csvCol("startTime") == "100")
    assert(csvCol("statementTruncated") == "false")
  }
}
