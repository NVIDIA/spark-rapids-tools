/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, PlatformFactory, PlatformNames}
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil

/**
 * Base class containing all non-Spark common methods used for testing.
 */
class BaseNoSparkSuite extends FunSuite with BeforeAndAfterEach with Logging {

  protected def checkDeltaLakeSparkRelease(): (Boolean, String) = {
    // deltaLake tests supported for spark35+
    (ToolUtils.isSpark350OrLater(), "DeltaLake release is not supported in Spark Version")
  }

  protected def checkUDFDetectionSupportForSpark(): (Boolean, String) = {
    // The UDF in query can be detected only for Spark-3.1.1
    // Follow https://issues.apache.org/jira/browse/SPARK-43131 for updates on detecting
    // UDFs in spark3.2+
    (!ToolUtils.isSpark320OrLater(), "The UDF in query can be detected only for Spark-3.1.1")
  }

  protected def shouldSkipFailedLogsForSpark(): (Boolean, String) = {
    (!ToolUtils.runtimeIsSparkVersion("3.4.0"),
      "Spark340 does not parse the eventlog correctly")
  }

  protected def ignoreExprForSparkGTE340(): (Boolean, String) = {
    (!ToolUtils.isSpark340OrLater(),
      "Spark340+ does not support the expression")
  }

  protected def execsSupportedSparkGTE331(): (Boolean, String) = {
    (ToolUtils.isSpark331OrLater(),
      "Spark331+ supports the Exec/Expression")
  }

  protected def execsSupportedSparkGTE340(): (Boolean, String) = {
    (ToolUtils.isSpark340OrLater(),
      "Spark340+ supports the Exec/Expression")
  }

  protected def execsSupportedSparkGTE350(): (Boolean, String) = {
    (ToolUtils.isSpark350OrLater(),
      "Spark350+ supports the Exec/Expression")
  }

  protected def subExecutionSupportedSparkGTE340(): (Boolean, String) = {
    (ToolUtils.isSpark340OrLater(),
      "Spark340+ supports the sub-execution grouping")
  }

  protected def createAppFromEventlog(eventLog: String,
    platformName: String = PlatformNames.DEFAULT): QualificationAppInfo = {
    val hadoopConf = RapidsToolsConfUtil.newHadoopConf()
    val (_, allEventLogs) = EventLogPathProcessor.processAllPaths(
      None, None, List(eventLog), hadoopConf)
    val pluginTypeChecker = new PluginTypeChecker()
    assert(allEventLogs.size == 1)
    val appResult = QualificationAppInfo.createApp(allEventLogs.head, hadoopConf,
      pluginTypeChecker, reportSqlLevel = false, mlOpsEnabled = false, penalizeTransitions = true,
      PlatformFactory.createInstance(platformName))
    appResult match {
      case Right(app) => app
      case Left(_) => throw new AssertionError("Cannot create application")
    }
  }

  def runConditionalTest(testName: String, assumeCondition: () => (Boolean, String))
    (fun: => Unit): Unit = {
    val (isAllowed, ignoreMessage) = assumeCondition()
    Try(assume(isAllowed)) match {
      case Success(_) =>
        test(testName) {
          fun
        }
      case Failure(_) =>
        // it does not matter the type of the failure
        ignore(s"$testName. Ignore Reason: $ignoreMessage") {}
    }
  }
}
