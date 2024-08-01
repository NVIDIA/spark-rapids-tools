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

class AccumInfo(val infoRef: AccMetaRef) {
  // TODO: Should we use sorted maps for stageIDs and taskIds?
  val taskUpdatesMap: mutable.HashMap[Long, Long] =
    new mutable.HashMap[Long, Long]()
  val stageValuesMap: mutable.HashMap[Int, Long] =
    new mutable.HashMap[Int, Long]()

  def addAccToStage(stageId: Int,
      accumulableInfo: AccumulableInfo,
      update: Option[Long] = None): Unit = {
    val value = accumulableInfo.value.flatMap(parseAccumFieldToLong)
    val existingValue = stageValuesMap.getOrElse(stageId, 0L)
    value match {
      case Some(v) =>
        // This assert prevents out of order events to be processed
        assert( v >= existingValue,
          s"Stage $stageId: Out of order events detected.")
        stageValuesMap.put(stageId, v)
      case _ =>
        val incomingUpdate = update.getOrElse(0L)
        assert( incomingUpdate >= existingValue,
          s"Stage $stageId: Out of order events detected.")
        // this case is for metrics that are not parsed as long
        // We track the accumId to stageId and taskId mapping
        stageValuesMap.put(stageId, incomingUpdate)
    }
  }

  def addAccToTask(stageId: Int, taskId: Long, accumulableInfo: AccumulableInfo): Unit = {
    val update = accumulableInfo.update.flatMap(parseAccumFieldToLong)
    // we have to update the stageMap if the stageId does not exist in the map
    var updateStageFlag = !stageValuesMap.contains(stageId)
    // TODO: Task can update an accum multiple times. Should account for that case.
    // This is for cases where same task updates the same accum multiple times
    val existingUpdate = taskUpdatesMap.getOrElse(taskId, 0L)
    update match {
      case Some(v) =>
        taskUpdatesMap.put(taskId, v + existingUpdate)
        // update teh stage if the task's update is non-zero
        updateStageFlag ||= v != 0
      case None =>
        taskUpdatesMap.put(taskId, existingUpdate)
    }
    // update the stage value map if necessary
    if (updateStageFlag) {
      addAccToStage(stageId, accumulableInfo, update.map(_ + existingUpdate))
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