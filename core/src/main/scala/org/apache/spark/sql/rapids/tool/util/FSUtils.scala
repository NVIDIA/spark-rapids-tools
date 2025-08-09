/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.tool.util

import java.io.{BufferedWriter, File, FileOutputStream, InputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import scala.io.{BufferedSource, Source}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, LocalFileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission

/**
 * Utility functions to interact with the file system (HDFS, local, etc).
 */
object FSUtils {
  // use same as Spark event log writer
  private val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("660", 8).toShort)
  private val LOG_FOLDER_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  // Copied from org.apache.spark.streaming.util.HdfsUtils
  def getFSForPath(path: Path, hadoopConf: Configuration): FileSystem = {
    // For local file systems, return the raw local file system, such calls to flush()
    // actually flushes the stream.
    val fs = path.getFileSystem(hadoopConf)
    fs match {
      case localFs: LocalFileSystem => localFs.getRawFileSystem
      case _ => fs
    }
  }

  private def getOutputStream(outputLoc: String, hadoopConf: Configuration): FSDataOutputStream = {
    val dfsPath = new Path(outputLoc)
    getOutputStream(dfsPath, hadoopConf)
  }

  private def getOutputStream(dfsPath: Path, hadoopConf: Configuration): FSDataOutputStream = {
    val dfs = getFSForPath(dfsPath, hadoopConf)
    val uri = dfsPath.toUri
    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"
    val outputStream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        FileSystem.mkdirs(dfs, dfsPath.getParent, LOG_FOLDER_PERMISSIONS)
        new FSDataOutputStream(new FileOutputStream(uri.getPath), null)
      } else {
        dfs.create(dfsPath)
      }
    dfs.setPermission(dfsPath, LOG_FILE_PERMISSIONS)
    outputStream
  }

  private def getInputStream(dfsPath: Path, hadoopConf: Configuration): FSDataInputStream = {
    val dfs = getFSForPath(dfsPath, hadoopConf)
    dfs.open(dfsPath)
  }

  def getUTF8BufferedWriter(outputLoc: String,
      hadoopConf: Option[Configuration]): BufferedWriter = {
    val outStream = getOutputStream(outputLoc, hadoopConf.getOrElse(new Configuration()))
    new BufferedWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8))
  }

  /**
   * Reads the content of a file as UTF-8 and closes the resources.
   * Works with any Hadoop-compatible filesystem (local, HDFS, S3, GCS, etc.).
   */
  def readFileContentAsUTF8(
      filePath: String,
      hadoopConf: Configuration = new Configuration(),
      delim: String = "\n"): String = {
    val path = new Path(filePath)
    var fsIs: FSDataInputStream = null
    try {
      fsIs = getInputStream(path, hadoopConf)
      val source = UTF8Source.fromInputStream(fsIs)
      try {
        source.getLines().mkString(delim)
      } finally {
        source.close()
      }
    } finally {
      if (fsIs != null) {
        try {
          fsIs.close()
        } catch {
          case _: Throwable => // ignore close errors
        }
      }
    }
  }
}

// scalastyle:off regex.source.from
/**
 * Wrapper object for creating a BufferedSource object for reading a file as UTF-8.
 * Any additional methods for reading files using the `Source` class should be implemented
 * in this object to avoid encoding issues. This is also enforced by the scalastyle rule.
 */
object UTF8Source {
  /**
   * Creates a BufferedSource object for reading a file as UTF-8.
   */
  def fromFile(filePath: String): BufferedSource = {
    Source.fromFile(filePath)(StandardCharsets.UTF_8)
  }

  /**
   * Creates a BufferedSource object for reading a file as UTF-8 from a java.io.File.
   */
  def fromFile(file: File): BufferedSource = {
    Source.fromFile(file)(StandardCharsets.UTF_8)
  }

  /**
   * Creates a BufferedSource object for reading a file as UTF-8 from the resources.
   */
  def fromResource(filePath: String): BufferedSource = {
    Source.fromResource(filePath)(StandardCharsets.UTF_8)
  }

  /**
   * Creates a BufferedSource object for reading a file as UTF-8 from an InputStream.
   */
  def fromInputStream(inputStream: InputStream): BufferedSource = {
    Source.fromInputStream(inputStream)(StandardCharsets.UTF_8)
  }
}
// scalastyle:on regex.source.from
