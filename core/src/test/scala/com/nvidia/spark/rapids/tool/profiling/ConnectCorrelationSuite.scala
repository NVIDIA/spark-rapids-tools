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

import com.nvidia.spark.rapids.BaseNoSparkSuite
import com.nvidia.spark.rapids.tool.EventLogPathProcessor

import org.apache.spark.sql.TrampolineUtil
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil

/**
 * Tests for Spark Connect sqlID/jobID correlation indexes on AppBase.
 * Task 1 only verifies the reverse-index HashMaps exist and are initialized
 * empty on a fresh app. Later tasks populate them from
 * SparkListenerSQLExecutionStart.jobTags and
 * SparkListenerJobStart.properties["spark.job.tags"].
 */
class ConnectCorrelationSuite extends BaseNoSparkSuite {

  private val hadoopConf = RapidsToolsConfUtil.newHadoopConf()

  private val logStartEvent =
    """{"Event":"SparkListenerLogStart","Spark Version":"3.5.0"}"""
  private val appStartEvent =
    """{"Event":"SparkListenerApplicationStart","App Name":"CorrelationTest",""" +
      """"App ID":"local-correlation","Timestamp":100000,"User":"testUser"}"""
  private val envUpdateEvent =
    """{"Event":"SparkListenerEnvironmentUpdate","JVM Information":{},""" +
      """"Spark Properties":{"spark.master":"local[*]"},""" +
      """"Hadoop Properties":{},"System Properties":{"file.encoding":"UTF-8"},""" +
      """"Classpath Entries":{}}"""
  private val appEndEvent =
    """{"Event":"SparkListenerApplicationEnd","Timestamp":200000}"""

  private def withEventLog(events: String*)(verify: ApplicationInfo => Unit): Unit = {
    val content = events.mkString("\n")
    TrampolineUtil.withTempDir { tempDir =>
      val path = Paths.get(tempDir.getAbsolutePath, "test_eventlog")
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
      val app = new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path.toString, hadoopConf).head._1)
      verify(app)
    }
  }

  test("operationIdToSqlIds / operationIdToJobIds are initialized empty on AppBase") {
    withEventLog(logStartEvent, appStartEvent, envUpdateEvent, appEndEvent) { app =>
      assert(app.operationIdToSqlIds.isEmpty)
      assert(app.operationIdToJobIds.isEmpty)
    }
  }
}