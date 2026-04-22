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

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests Task 6 of Spark Connect Phase 3 (#2065): writing each operation's
 * `statementText` to a sidecar file under
 * `<per-app-output-dir>/connect_statements/<operationId>.txt` and returning
 * the basename map used to populate the `statementFile` column in
 * `connect_operations.csv`.
 */
class ConnectStatementWriterSuite extends AnyFunSuite {

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

  private def makeOp(opId: String, stmt: String): ConnectOperationInfo = {
    new ConnectOperationInfo(
      operationId = opId,
      sessionId = "sess-1",
      userId = "alice",
      jobTag = s"SparkConnect_OperationTag_User_alice_Session_sess-1_Operation_$opId",
      statementText = stmt,
      startTime = 100L)
  }

  test("writes non-empty statementText to sidecar file and returns basename") {
    val tmpDir = Files.createTempDirectory("connect-stmt-writer-")
    try {
      val op = makeOp("op-1", "range(0, 10)")
      val result = ConnectStatementWriter.writeStatementFiles(
        tmpDir.toString, Seq(op))
      assert(result == Map("op-1" -> "op-1.txt"),
        s"expected map op-1 -> op-1.txt, got $result")
      val expectedPath = tmpDir.resolve(ConnectStatementWriter.SUB_DIR).resolve("op-1.txt")
      assert(Files.exists(expectedPath), s"expected sidecar at $expectedPath")
      val written = new String(Files.readAllBytes(expectedPath), StandardCharsets.UTF_8)
      assert(written == "range(0, 10)", s"unexpected content: $written")
    } finally {
      deleteRecursively(tmpDir)
    }
  }

  test("empty statementText is skipped and not in returned map") {
    val tmpDir = Files.createTempDirectory("connect-stmt-writer-")
    try {
      val op1 = makeOp("op-1", "plan body")
      val op2 = makeOp("op-2", "")
      val result = ConnectStatementWriter.writeStatementFiles(
        tmpDir.toString, Seq(op1, op2))
      assert(result.keySet == Set("op-1"),
        s"expected only op-1 in map, got ${result.keySet}")
      assert(result("op-1") == "op-1.txt")
      val op2Path = tmpDir.resolve(ConnectStatementWriter.SUB_DIR).resolve("op-2.txt")
      assert(!Files.exists(op2Path), s"op-2 sidecar should not exist: $op2Path")
    } finally {
      deleteRecursively(tmpDir)
    }
  }

  test("does not create connect_statements dir when all statementText empty") {
    val tmpDir = Files.createTempDirectory("connect-stmt-writer-")
    try {
      val op1 = makeOp("op-1", "")
      val op2 = makeOp("op-2", "")
      val result = ConnectStatementWriter.writeStatementFiles(
        tmpDir.toString, Seq(op1, op2))
      assert(result.isEmpty, s"expected empty map, got $result")
      val subDir = tmpDir.resolve(ConnectStatementWriter.SUB_DIR)
      assert(!Files.exists(subDir),
        s"sidecar directory should not have been created: $subDir")
    } finally {
      deleteRecursively(tmpDir)
    }
  }

  test("Unicode / multi-byte statementText roundtrips through UTF-8") {
    val tmpDir = Files.createTempDirectory("connect-stmt-writer-")
    try {
      val unicode = "SELECT 'λ', '漢字', '🚀' FROM t"
      val op = makeOp("op-1", unicode)
      val result = ConnectStatementWriter.writeStatementFiles(
        tmpDir.toString, Seq(op))
      assert(result == Map("op-1" -> "op-1.txt"))
      val path = Paths.get(tmpDir.toString, ConnectStatementWriter.SUB_DIR, "op-1.txt")
      val bytes = Files.readAllBytes(path)
      assert(bytes.sameElements(unicode.getBytes(StandardCharsets.UTF_8)),
        "bytes on disk should match UTF-8 encoded original")
      val roundtrip = new String(bytes, StandardCharsets.UTF_8)
      assert(roundtrip == unicode, s"roundtrip mismatch: $roundtrip vs $unicode")
    } finally {
      deleteRecursively(tmpDir)
    }
  }

  test("sanitizes operationId before resolving sidecar path") {
    val tmpDir = Files.createTempDirectory("connect-stmt-writer-")
    try {
      val op = makeOp("../../etc/foo", "range(0, 10)")
      val result = ConnectStatementWriter.writeStatementFiles(tmpDir.toString, Seq(op))
      val expectedBasename = ".._.._etc_foo.txt"
      assert(result == Map("../../etc/foo" -> expectedBasename),
        s"expected sanitized basename map, got $result")
      val expectedPath = tmpDir.resolve(ConnectStatementWriter.SUB_DIR).resolve(expectedBasename)
      assert(Files.exists(expectedPath), s"expected sidecar at $expectedPath")
      assert(expectedPath.normalize().startsWith(tmpDir.resolve(ConnectStatementWriter.SUB_DIR)),
        s"sidecar should remain under ${ConnectStatementWriter.SUB_DIR}: $expectedPath")
    } finally {
      deleteRecursively(tmpDir)
    }
  }
}
