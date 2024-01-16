/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import scala.io.Source

import org.apache.spark.internal.Logging

trait DriverLogInfoProvider {
  def getUnsupportedOperators: Seq[DriverLogUnsupportedOperators] = Seq.empty
}

/**
 * A base class definition that provides an empty implementation of the driver log information
 * [[ApplicationSummaryInfo]].
 */
class BaseDriverLogInfoProvider(driverlogPath: Option[String] = None)
  extends DriverLogInfoProvider {
  def isLogPathAvailable = driverlogPath.isDefined
}

class DriverLogProcessor(logPath: String)
  extends BaseDriverLogInfoProvider(Some(logPath))
  with Logging {

  lazy val unsupportedOps = processDriverLog()
  override def getUnsupportedOperators = unsupportedOps
  private def processDriverLog(): Seq[DriverLogUnsupportedOperators] = {
    val source = Source.fromFile(logPath)
    // Create a map to store the counts for each operator and reason
    var countsMap = Map[(String, String), Int]().withDefaultValue(0)
    try {
      // Process each line in the file
      for (line <- source.getLines()) {
        // condition to check if the line contains unsupported operators
        if (line.contains("cannot run on GPU") &&
          !line.contains("not all expressions can be replaced")) {
          val operatorName = line.split("<")(1).split(">")(0)
          val reason = line.split("because")(1).trim()
          val key = (operatorName, reason)
          countsMap += key -> (countsMap(key) + 1)
        }
      }
    } catch {
      case e: Exception =>
        logError(s"Unexpected exception processing driver log: $logPath", e)
    } finally {
      source.close()
    }
    countsMap.map(x => DriverLogUnsupportedOperators(x._1._1, x._2, x._1._2)).toSeq
  }
}


object BaseDriverLogInfoProvider {
  def noneDriverLog: BaseDriverLogInfoProvider = new BaseDriverLogInfoProvider()
}