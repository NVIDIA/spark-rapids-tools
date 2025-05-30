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
package com.nvidia.spark.rapids.tool

import java.io.BufferedWriter
import java.util.Properties

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.util.FSUtils

/**
 * Class for writing local files, allows writing to distributed file systems.
 */
class ToolTextFileWriter(
    finalOutputDir: String,
    logFileName: String,
    finalLocationText: String,
    hadoopConf: Option[Configuration] = None) extends Logging {

  private val textOutputLoc = s"$finalOutputDir/$logFileName"

  def getFileOutputPath: Path = new Path(textOutputLoc)

  // The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
  // Therefore, for local files, use FileOutputStream instead.
  // this overwrites existing path
  private var utf8Writer: Option[BufferedWriter] = {
    try {
      Some(FSUtils.getUTF8BufferedWriter(textOutputLoc, hadoopConf))
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to open output path [$textOutputLoc] for writing", e)
        None
    }
  }

  def write(stringToWrite: String): Unit = {
    utf8Writer.foreach(_.write(stringToWrite))
  }

  def writeLn(stringToWrite: String): Unit = {
    utf8Writer.foreach { w =>
      w.write(stringToWrite)
      w.write("\n")
    }
  }

  def writeProperties(props: Properties, comment: String): Unit = {
    utf8Writer.foreach(props.store(_, comment))
  }

  def flush(): Unit = {
    utf8Writer.foreach { writer =>
      writer.flush()
    }
  }

  def close(): Unit = {
    // No need to close the outputStream.
    // Java should handle nested streams automatically.
    utf8Writer.foreach { writer =>
      logDebug(s"$finalLocationText output location: $textOutputLoc")
      writer.flush()
      writer.close()
    }
    utf8Writer = None
  }
}
