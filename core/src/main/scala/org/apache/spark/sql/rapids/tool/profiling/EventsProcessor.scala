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

package org.apache.spark.sql.rapids.tool.profiling

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.rapids.tool.EventProcessorBase
import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * This class is to process all events and do validation in the end.
 */
class EventsProcessor(app: ApplicationInfo) extends EventProcessorBase[ApplicationInfo](app)
  with Logging {

  override def doSparkListenerJobStart(
      app: ApplicationInfo,
      event: SparkListenerJobStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerJobStart(app, event)
    val sqlIDString = event.properties.getProperty("spark.sql.execution.id")
    val sqlID = StringUtils.stringToLong(sqlIDString)
    // add jobInfoClass
    val thisJob = new JobInfoClass(
      event.jobId,
      event.stageIds,
      sqlID,
      event.properties.asScala,
      event.time,
      None,
      None,
      None,
      None,
      app.isGPUModeEnabledForJob(event)
    )
    app.jobIdToInfo.put(event.jobId, thisJob)
  }
}
