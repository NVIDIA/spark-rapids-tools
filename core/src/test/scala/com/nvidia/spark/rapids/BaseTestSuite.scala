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

package com.nvidia.spark.rapids

import scala.util.{Failure, Success, Try}

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.ToolUtils

class BaseTestSuite extends FunSuite with BeforeAndAfterEach with Logging {
  private val testAppName = "Rapids Spark Profiling Tool Unit Tests"
  protected var sparkSession: SparkSession = _

  override protected def beforeEach(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    createSparkSession()
  }

  override protected def afterEach(): Unit = {
    // close the spark session at the end of the test to avoid leaks
    TrampolineUtil.cleanupAnyExistingSession()
  }

  protected def createSparkSession(): Unit = {
    sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(testAppName)
      .getOrCreate()
  }

  protected def checkDeltaLakeSparkRelease(): (Boolean, String) = {
    // deltaLake is supported until Spark-340
    val supportsDeltaLake = if (ToolUtils.isSpark340OrLater()) {
      ToolUtils.runtimeIsSparkVersion("3.4.0")
    } else {
      true
    }
    (supportsDeltaLake, "DeltaLake release is not supported in Spark Version")
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

  protected def shouldSkipUnsupportedExprForSparkLT340(): (Boolean, String) = {
    (!ToolUtils.isSpark340OrLater(),
      "Spark340 does not support the expression")
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
