/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

import java.io.{BufferedReader, InputStreamReader, IOException}

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil

trait DriverLogInfoProvider {
  def getUnsupportedOperators: Seq[DriverLogUnsupportedOperators] = Seq.empty
}

/**
 * A base class definition that provides an empty implementation of the driver log information
 * [[ApplicationSummaryInfo]].
 */
class BaseDriverLogInfoProvider
  extends DriverLogInfoProvider {
  def isLogPathAvailable = false
}

class DriverLogProcessor(hadoopConf: Configuration, logPath: String)
  extends BaseDriverLogInfoProvider()
  with Logging {

  private def processDriverLogFile(): Seq[DriverLogUnsupportedOperators] = {
    val path = new Path(logPath)
    var fsIs: FSDataInputStream = null
    val countsMap = mutable.Map[(String, String), Int]().withDefaultValue(0)
    try {
      val fs = FileSystem.get(path.toUri, hadoopConf)
      fsIs = fs.open(path)
      val reader = new BufferedReader(new InputStreamReader(fsIs))
      // Process each line in the file
      Iterator.continually(reader.readLine()).takeWhile(_ != null)
        .filter { line =>
          line.contains("cannot run on GPU") &&
            !line.contains("not all expressions can be replaced")
        }.foreach { line =>
          val operatorName = line.split("<")(1).split(">")(0)
          val reason = line.split("because")(1).trim()
          val key = (operatorName, reason)
          countsMap += key -> (countsMap(key) + 1)
        }
    } catch {
      // In case of missing file/malformed for driver log catch and log as a warning
      case e: IOException =>
        logError(s"Could not load/open logDriver: $logPath", e)
      case NonFatal(e) =>
        logError(s"Unexpected error while processing driver log: $logPath", e)
    } finally {
      if (fsIs != null) {
        try {
          fsIs.close()
        } catch {
          case e: IOException =>
            logError(s"Failed to close the input stream for driver log: $logPath", e)
        }
      }
    }
    countsMap.iterator.map { case ((name, reason), count) =>
      DriverLogUnsupportedOperators(name, count, reason)
    }.toSeq
  }

  lazy val unsupportedOps: Seq[DriverLogUnsupportedOperators] = processDriverLogFile()
  override def getUnsupportedOperators: Seq[DriverLogUnsupportedOperators] = unsupportedOps
}

object BaseDriverLogInfoProvider {
  def noneDriverLog: BaseDriverLogInfoProvider = new BaseDriverLogInfoProvider()
  def apply(logPath: Option[String], hadoopConf: Option[Configuration]): DriverLogInfoProvider = {
    logPath match {
      case Some(path) =>
        new DriverLogProcessor(hadoopConf.getOrElse(RapidsToolsConfUtil.newHadoopConf()), path)
      case None =>
        noneDriverLog
    }
  }
}
