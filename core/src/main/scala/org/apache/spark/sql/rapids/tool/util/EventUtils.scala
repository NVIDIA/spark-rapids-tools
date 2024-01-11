/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.tool.util

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.profiling.TaskStageAccumCase

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo

/**
 * Utility containing the implementation of helpers used for parsing data from event.
 */
object EventUtils extends Logging {

  /**
   * Used to parse (value/update) fields of the AccumulableInfo object. If the data is not
   * a valid long, it tries to parse it as a duration in the format of "hour:mm:ss.SSS".
   *
   * @param data value stored in the (value/update) of the AccumulableInfo
   * @return valid parsed long of the content or the duration
   */
  @throws[NullPointerException]
  def parseAccumFieldToLong(data: Any): Option[Long] = {
    val strData = data.toString
    try {
      Some(strData.toLong)
    } catch {
      case _ : NumberFormatException =>
        StringUtils.parseFromDurationToLongOption(strData)
      case NonFatal(_) =>
        None
    }
  }

  /**
   * Given AccumulableInfo object, this method tries to parse value/update fields into long.
   * It is common to have one of those two fields set to None. That's why, we are not skipping the
   * entire accumulable if one of those fields fail to parse.
   *
   * @param accuInfo object of AccumulableInfo to be processed
   * @param stageId the tageId to which the metric belongs
   * @param attemptId the ID of the current execution
   * @param taskId the task-id to which the metric belongs , if any.
   * @return option(TaskStageAccumCase) if art least one of the two fields can be parsed to Long.
   */
  def buildTaskStageAccumFromAccumInfo(accuInfo: AccumulableInfo,
      stageId: Int, attemptId: Int, taskId: Option[Long] = None): Option[TaskStageAccumCase] = {
    val value = accuInfo.value.flatMap(parseAccumFieldToLong)
    val update = accuInfo.update.flatMap(parseAccumFieldToLong)
    if (!(value.isDefined || update.isDefined)) {
      // we could not get any valid number from both value/update
      if (log.isDebugEnabled()) {
        if ((accuInfo.value.isDefined && value.isEmpty) ||
          (accuInfo.update.isDefined && update.isEmpty)) {
          // in this case we failed to parse
          logDebug(s"Failed to parse accumulable for stageId=$stageId, taskId=$taskId." +
            s"The problematic accumulable is: $accuInfo")
        }
      }
      // No need to return a new object to save memory consumption
      None
    } else {
      Some(TaskStageAccumCase(
        stageId, attemptId,
        taskId, accuInfo.id, accuInfo.name, value, update, accuInfo.internal))
    }
  }

  // A utility function used to read Spark properties and compare it to a given target.
  // Note that it takes a default argument as well in case the property is not available.
  def isPropertyMatch(properties: collection.Map[String, String], propKey: String,
      defValue: String, targetValue: String): Boolean = {
    properties.getOrElse(propKey, defValue).equals(targetValue)
  }
}
