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

package com.nvidia.spark.rapids

import org.apache.spark.sql.{DataFrame, SparkSession, TrampolineUtil}

class BaseTestSuite extends BaseNoSparkSuite {
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

  def withTable(spark: SparkSession, tableNames: String*)(f: => DataFrame): DataFrame = {
    try {
      f  // Execute the passed block of code.
    } finally {
      tableNames.foreach { name =>
        // Attempt to drop each table, ignoring any errors if the table doesn't exist.
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }
}
