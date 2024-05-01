/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.tool.profiling.ProfileUtils

import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.ui.UIUtils

/**
 * A class used to hold the information of a Spark Application
 * @param eventLogPath captures the path of the eventlog being processed by teh tools.
 * @param appName name of the application
 * @param appId application id
 * @param sparkUser user who ran the Spark application
 * @param startTime startTime of a Spark application
 * @param endTime endTime of the spark Application
 */
class AppMetaData(
    val eventLogPath: Option[String],
    val appName: String,
    val appId: Option[String],
    val sparkUser: String,
    val startTime: Long,
    var endTime: Option[Long] = None) {

  // Calculated as time in ms
  var duration: Option[Long] = _
  // Boolean to indicate whether the endTime was estimated.
  private var durationEstimated: Boolean = _

  /**
   * Returns the duration of the application in human readable format
   * @return human readable format or empty String when the endTime is undefined.
   */
  def getDurationString: String = {
    duration match {
      case Some(i) => UIUtils.formatDuration(i)
      case None => ""
    }
  }

  /**
   * Set the endTime of the application and calculate the duration.
   * @param newEndTime new endTime in ms
   * @param estimated flag to indicate whether the endTime was estimated or not.
   */
  def setEndTime(newEndTime: Long, estimated: Boolean = false): Unit = {
    endTime = Some(newEndTime)
    calculateDurationInternal()
    setEstimatedFlag(estimated)
  }

  /**
   * Recalculate the duration of the application when the endTime is updated
   */
  private def calculateDurationInternal(): Unit = {
    duration = ProfileUtils.OptionLongMinusLong(endTime, startTime)
  }

  private def setEstimatedFlag(newValue: Boolean): Unit = {
    durationEstimated = newValue
  }

  def isDurationEstimated: Boolean = durationEstimated

  // Initialization code
  // 1- Calculate the duration based on the constructor argument endTime
  // 2- Set the estimated flag to true if endTime is not set
  calculateDurationInternal()
  // initial estimated flag is set to True if endTime is not set
  setEstimatedFlag(endTime.isEmpty)
}

/**
 * A class to handle appMetadata for a running SparkApplication. This is created when a Spark
 * Listener is used to analyze an existing App.
 * The AppMetaData for a running application does not have eventlog.
 * @param rName name of the application
 * @param rId application id
 * @param rUser user who ran the Spark application
 * @param rStartTime startTime of a Spark application
 */
class RunningAppMetadata(
    rName: String,
    rId: Option[String],
    val rUser: String,
    val rStartTime: Long) extends AppMetaData(None, rName, rId, rUser, rStartTime) {

}

object AppMetaData {

  def apply(
      evtLogPath: String,
      appStartEv: SparkListenerApplicationStart): AppMetaData = {
    new AppMetaData(Some(evtLogPath),
      appStartEv.appName,
      appStartEv.appId, appStartEv.sparkUser,
      appStartEv.time)
  }

  def createRunningAppMetadata(
      rName: String,
      rId: Option[String],
      rStartTime: Long): RunningAppMetadata = {
    new RunningAppMetadata(rName, rId, "", rStartTime)
  }
}
