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

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

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
 *
 * Writes go through [[ToolTextFileWriter]] so the same UTF-8, permissions, and
 * local/raw-filesystem behavior used by the rest of the tools output applies
 * here as well.
 */
object ConnectStatementWriter extends Logging {

  val SUB_DIR: String = "connect_statements"
  val FILE_EXTENSION: String = ".txt"
  private val UnsafePathChars = "[^A-Za-z0-9._-]".r

  private def sanitizeOperationId(operationId: String): String = {
    UnsafePathChars.replaceAllIn(operationId, "_")
  }

  /**
   * Writes each operation's `statementText` to
   * `<rootDir>/connect_statements/<operationId>.txt` when non-empty.
   *
   * @param rootDir    per-app output directory (already exists)
   * @param ops        operations to persist
   * @param hadoopConf Hadoop configuration used to resolve the target
   *                   filesystem. When `None`, a fresh `Configuration` is
   *                   used (which resolves the default filesystem, typically
   *                   local).
   * @return map of `operationId -> "<sanitizedOperationId>.txt"` basenames for
   *         the operations whose sidecar file was written successfully.
   */
  def writeStatementFiles(
      rootDir: String,
      ops: Iterable[ConnectOperationInfo],
      hadoopConf: Option[Configuration] = None): Map[String, String] = {
    val subDirPath = new Path(rootDir, SUB_DIR)
    val builder = Map.newBuilder[String, String]
    ops.foreach { op =>
      val text = op.statementText
      if (text.nonEmpty) {
        try {
          val safeId = sanitizeOperationId(op.operationId)
          val basename = s"$safeId$FILE_EXTENSION"
          val target = new Path(subDirPath, basename)
          // Defense-in-depth containment check. Sanitization already removes
          // `/` and `..`, but verify the resolved parent matches.
          require(target.getParent == subDirPath,
            s"Refusing to write Connect statement sidecar outside $subDirPath: $target")
          val writer = new ToolTextFileWriter(
            subDirPath.toString,
            basename,
            s"Connect statement sidecar for operation ${op.operationId}",
            hadoopConf)
          try {
            writer.write(text)
          } finally {
            writer.close()
          }
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
