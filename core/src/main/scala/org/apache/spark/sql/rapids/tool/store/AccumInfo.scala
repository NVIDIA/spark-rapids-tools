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

package org.apache.spark.sql.rapids.tool.store

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.analysis.StatisticsMetrics

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.sql.rapids.tool.util.EventUtils.parseAccumFieldToLong

/**
 * Maintains the accumulator information for a single accumulator
 * This maintains following information:
 * 1. Task updates for the accumulator - a map of all taskIds and their update values
 * 2. Stage values for the accumulator - a map of all stageIds and their total values
 * 3. AccumMetaRef for the accumulator - a reference to the Meta information
 * @param infoRef - AccumMetaRef for the accumulator
 */
class AccumInfo(val infoRef: AccMetaRef) {
  // TODO: Should we use sorted maps for stageIDs and taskIds?
  val taskUpdatesMap: mutable.HashMap[Long, Long] =
    new mutable.HashMap[Long, Long]()
  val stageValuesMap: mutable.HashMap[Int, Long] =
    new mutable.HashMap[Int, Long]()

  def addAccToStage(stageId: Int,
      accumulableInfo: AccumulableInfo,
      update: Option[Long] = None): Unit = {
    val parsedValue = accumulableInfo.value.flatMap(parseAccumFieldToLong)
    val existingValue = stageValuesMap.getOrElse(stageId, 0L)
    val incomingValue = parsedValue match {
      case Some(v) => v
      case _ => update.getOrElse(0L)
    }
    stageValuesMap.put(stageId, Math.max(existingValue, incomingValue))
  }

  def addAccToTask(stageId: Int, taskId: Long, accumulableInfo: AccumulableInfo): Unit = {
    val parsedUpdateValue = accumulableInfo.update.flatMap(parseAccumFieldToLong)
    // we have to update the stageMap if the stageId does not exist in the map
    var updateStageFlag = !stageValuesMap.contains(stageId)
    // This is for cases where same task updates the same accum multiple times
    val existingUpdateValue = taskUpdatesMap.getOrElse(taskId, 0L)
    parsedUpdateValue match {
      case Some(v) =>
        taskUpdatesMap.put(taskId, v + existingUpdateValue)
      case None =>
        taskUpdatesMap.put(taskId, existingUpdateValue)
    }
    // update the stage value map if necessary
    if (updateStageFlag) {
      addAccToStage(stageId, accumulableInfo, parsedUpdateValue)
    }
  }

  def getStageIds: Set[Int] = {
    stageValuesMap.keySet.toSet
  }

  def getMinStageId: Int = {
      stageValuesMap.keys.min
  }

  def calculateAccStats(): StatisticsMetrics = {
    val sortedTaskUpdates = taskUpdatesMap.values.toSeq.sorted
    if (sortedTaskUpdates.isEmpty) {
      // do not check stage values because the stats is only meant for task updates
      StatisticsMetrics.ZERO_RECORD
    } else {
      val min = sortedTaskUpdates.head
      val max = sortedTaskUpdates.last
      val sum = sortedTaskUpdates.sum
      val median = if (sortedTaskUpdates.size % 2 == 0) {
        val mid = sortedTaskUpdates.size / 2
        (sortedTaskUpdates(mid) + sortedTaskUpdates(mid - 1)) / 2
      } else {
        sortedTaskUpdates(sortedTaskUpdates.size / 2)
      }
      StatisticsMetrics(min, median, max, sum)
    }
  }

  def getMaxStageValue: Option[Long] = {
    if (stageValuesMap.values.isEmpty) {
      None
    } else {
      Some(stageValuesMap.values.max)
    }
  }
}
