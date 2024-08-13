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

package org.apache.spark.sql.rapids.tool

import com.nvidia.spark.rapids.tool.EventLogInfo
import org.apache.hadoop.conf.Configuration

import org.apache.spark.scheduler.{SparkListenerApplicationStart, SparkListenerEnvironmentUpdate, SparkListenerEvent}



class FilterAppInfo(
    eventLogInfo: EventLogInfo,
    hadoopConf: Configuration) extends AppBase(Some(eventLogInfo), Some(hadoopConf)) {

  def doSparkListenerApplicationStart(
      event: SparkListenerApplicationStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val appMeta = AppMetaData(getEventLogPath, event)
    appMetaData = Some(appMeta)
  }

  def doSparkListenerEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    handleEnvUpdateForCachedProps(event)
  }

  // We are currently processing 2 events. This is used as counter to send true when both the
  // event are processed so that we can stop processing further events.
  var eventsToProcess: Int = 2

  override def processEvent(event: SparkListenerEvent): Boolean = {
    if (event.isInstanceOf[SparkListenerApplicationStart]) {
      doSparkListenerApplicationStart(event.asInstanceOf[SparkListenerApplicationStart])
      eventsToProcess -= 1
      eventsToProcess == 0
    } else if (event.isInstanceOf[SparkListenerEnvironmentUpdate]) {
      doSparkListenerEnvironmentUpdate(event.asInstanceOf[SparkListenerEnvironmentUpdate])
      eventsToProcess -= 1
      eventsToProcess == 0
    } else {
      false
    }
  }

  override def processSparkRapidsBuildEvent(event: SparkRapidsBuildInfo): Boolean = {
    this.sparkRapidsBuildInfo = event
    false
  }

  processEvents()
}
