/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLExecutionStart}
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

  override def doSparkListenerResourceProfileAddedReflect(
      app: ApplicationInfo,
      event: SparkListenerEvent): Boolean = {
    val rpAddedClass = "org.apache.spark.scheduler.SparkListenerResourceProfileAdded"
    if (event.getClass.getName.equals(rpAddedClass)) {
      try {
        event match {
          case _: SparkListenerResourceProfileAdded =>
            doSparkListenerResourceProfileAdded(app,
              event.asInstanceOf[SparkListenerResourceProfileAdded])
            true
          case _ => false
        }
      } catch {
        case _: ClassNotFoundException =>
          logWarning("Error trying to parse SparkListenerResourceProfileAdded, Spark" +
            " version likely older than 3.1.X, unable to parse it properly.")
          false
      }
    } else {
      false
    }
  }

  override def doSparkListenerEnvironmentUpdate(
      app: ApplicationInfo,
      event: SparkListenerEnvironmentUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerEnvironmentUpdate(app, event)

    logDebug(s"App's GPU Mode = ${app.gpuMode}")
  }

  override def doSparkListenerSQLExecutionStart(
      app: ApplicationInfo,
      event: SparkListenerSQLExecutionStart): Unit = {
    super.doSparkListenerSQLExecutionStart(app, event)
    app.physicalPlanDescription += (event.executionId -> event.physicalPlanDescription)
  }

  override def doSparkListenerTaskGettingResult(
      app: ApplicationInfo,
      event: SparkListenerTaskGettingResult): Unit = {
    logDebug("Processing event: " + event.getClass)
  }

  override def doSparkListenerSQLAdaptiveExecutionUpdate(
      app: ApplicationInfo,
      event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    // AQE plan can override the ones got from SparkListenerSQLExecutionStart
    app.physicalPlanDescription += (event.executionId -> event.physicalPlanDescription)
    super.doSparkListenerSQLAdaptiveExecutionUpdate(app, event)
  }

  // To process all other unknown events
  override def doOtherEvent(app: ApplicationInfo, event: SparkListenerEvent): Unit = {
    logDebug("Skipping unhandled event: " + event.getClass)
    // not used
  }
}
