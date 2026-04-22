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

import org.apache.spark.internal.Logging

/**
 * Writes per-operation Spark Connect `statementText` payloads to sidecar files
 * so large protobuf-text plans do not inflate the `connect_operations.csv`
 * table. Files are written under `<rootDir>/connect_statements/<opId>.txt` and
 * the returned map records the basenames for inclusion in the operation CSV
 * `statementFile` column.
 *
 * Operations with empty `statementText` are skipped entirely (no file, no map
 * entry). The `connect_statements` subdirectory is created lazily on the first
 * non-empty statement, so apps with no statements at all do not produce an
 * empty directory. Per-file IO errors are logged and skipped; they do not
 * abort the batch.
 */
object ConnectStatementWriter extends Logging {

  val SUB_DIR: String = "connect_statements"
  val FILE_EXTENSION: String = ".txt"

  /**
   * Writes each operation's `statementText` to
   * `<rootDir>/connect_statements/<operationId>.txt` when non-empty.
   *
   * @param rootDir per-app output directory (already exists)
   * @param ops     operations to persist
   * @return map of `operationId -> "<operationId>.txt"` basenames for the
   *         operations whose sidecar file was written successfully.
   */
  def writeStatementFiles(
      rootDir: String,
      ops: Iterable[ConnectOperationInfo]): Map[String, String] = {
    val subDirPath = Paths.get(rootDir, SUB_DIR)
    var subDirCreated = false
    val builder = Map.newBuilder[String, String]
    ops.foreach { op =>
      val text = op.statementText
      if (text.nonEmpty) {
        try {
          if (!subDirCreated) {
            Files.createDirectories(subDirPath)
            subDirCreated = true
          }
          val basename = s"${op.operationId}$FILE_EXTENSION"
          val target = subDirPath.resolve(basename)
          Files.write(target, text.getBytes(StandardCharsets.UTF_8))
          builder += (op.operationId -> basename)
        } catch {
          case e: Exception =>
            logWarning(s"Failed to write Connect statement sidecar for operation " +
              s"${op.operationId} under $subDirPath", e)
        }
      }
    }
    builder.result()
  }
}
