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
class AccumInfo(val infoRef: AccumMetaRef) {
  // TODO: use sorted maps for stageIDs and taskIds

//  var sumTaskUpdate = 0L

//  val taskUpdatesMap: mutable.HashMap[Long, Long] =
//    new mutable.HashMap[Long, Long]()
//  val stageValuesMap: mutable.HashMap[Int, Long] =
//    new mutable.HashMap[Int, Long]()
  val stageValuesMap: mutable.HashMap[Int, (Long, StatisticsMetrics)] =
    new mutable.HashMap[Int, (Long, StatisticsMetrics)]()

  /**
   * Add accumulable to a stage while:
   * 1- handling StageCompleted event.
   * 2- handling taskEnd event, we want to make sure that there is an entry that maps the stage to
   *    the accumulable. The reason is that we want to keep track of the stageIDs in case their
   *    stageCompleted has never been triggered. It is common case for incomplete eventlogs.
   * @param stageId the stageId pulled from the StageCompleted/TaskEnd event
   * @param accumulableInfo the accumulableInfo from the StageCompleted/TaskEnd event
   * @param update optional long that represents the task update value in case the call was
   *               triggered by a taskEnd event and the map between stage-Acc has not been
   *               established yet.
   */
  def addAccumToStage(stageId: Int,
      accumulableInfo: AccumulableInfo,
      update: Option[Long] = None): Unit = {
    val parsedValue = accumulableInfo.value.flatMap(parseAccumFieldToLong)
    // in case there is an out of order event, the value showing up later could be
    // lower-than the previous value. In that case we should take the maximum.
//    val existingValue = stageValuesMap.getOrElse(stageId, 0L)
    val existingEntry = stageValuesMap.getOrElse(stageId,
      (0L, StatisticsMetrics(0L, 0L, 0L, 0L)))
    val incomingValue = parsedValue match {
      case Some(v) => v
      case _ => update.getOrElse(0L)
    }
    val newValue = Math.max(existingEntry._1, incomingValue)
//    stageValuesMap.put(stageId, Math.max(existingValue, incomingValue))
    stageValuesMap.put(stageId, (newValue, existingEntry._2))
  }

  /**
   * Add accumulable to a task while handling TaskEnd event.
   * This can propagate a call to addAccToStage if the stageId does not exist in the stageValuesMap
   * @param stageId the stageId pulled from the TaskEnd event
   * @param taskId the taskId pulled from the TaskEnd event
   * @param accumulableInfo the accumulableInfo from the TaskEnd event
   */
  def addAccumToTask(stageId: Int, taskId: Long, accumulableInfo: AccumulableInfo): Unit = {
    val parsedUpdateValue = accumulableInfo.update.flatMap(parseAccumFieldToLong)
    // we need to update the stageMap if the stageId does not exist in the map
    parsedUpdateValue.foreach{ value =>
      val (total, stats) = stageValuesMap.getOrElse(stageId,
        (0L, StatisticsMetrics(value, 0L, value, 0L)))
      val newStats = StatisticsMetrics(
        Math.min(stats.min, value),
        (stats.med * stats.total + value) / ( stats.total + 1),
        Math.max(stats.max, value),
        stats.total + 1
      )
      stageValuesMap.put(stageId, (total + value, newStats))
    }
//    val updateStageFlag = !stageValuesMap.contains(stageId)
//    parsedUpdateValue match {
//      case Some(x) =>
//        minTaskUpdate = Math.min(minTaskUpdate, x)
//        maxTaskUpdate = Math.max(maxTaskUpdate, x)
//      case None =>
//    }
    // This is for cases where same task updates the same accum multiple times
//    val existingUpdateValue = taskUpdatesMap.getOrElse(taskId, 0L)
//    parsedUpdateValue match {
//      case Some(_) =>
//        taskUpdatesMap.put(taskId, v + existingUpdateValue)
//      case None =>
//        taskUpdatesMap.put(taskId, existingUpdateValue)
//    }
    // update the stage value map if necessary
//    if (updateStageFlag) {
//      addAccumToStage(stageId, accumulableInfo, parsedUpdateValue)
//    }
  }

  def getStageIds: Set[Int] = {
    stageValuesMap.keySet.toSet
  }

  def getMinStageId: Int = {
    stageValuesMap.keys.min
  }

//  def calculateAccStats(): StatisticsMetrics = {
//    // do not check stage values because the stats is only meant for task updates
//    StatisticsMetrics(minTaskUpdate, 0L, maxTaskUpdate, stageValuesMap.values.sum)
//    StatisticsMetrics.createFromArr(taskUpdatesMap.map(_._2)(breakOut))
//  }

  def calculateAccStats(): StatisticsMetrics = {
    val reduced_val = stageValuesMap.values.reduce { (a, b) =>
      (a._1 + b._1,
        StatisticsMetrics(
          Math.min(a._2.min, b._2.min),
          (a._2.med * a._2.total + b._2.med * b._2.total) / (a._2.total + b._2.total),
          Math.max(a._2.max, b._2.max),
          a._2.total + b._2.total
      ))
    }
    StatisticsMetrics(
      reduced_val._2.min,
      reduced_val._2.med,
      reduced_val._2.max,
      reduced_val._1)
  }

  def calculateAccStatsForStage(stageId: Int): Option[StatisticsMetrics] = {
    val output = stageValuesMap.get(stageId)
    output match {
      case Some(x) => Some(StatisticsMetrics(x._2.min, x._2.med, x._2.max, x._1))
      case None => None
    }
  }

  def getMaxStageValue: Option[Long] = {
    if (stageValuesMap.values.isEmpty) {
      None
    } else {
      Some(stageValuesMap.values.map(_._1).max)
    }
  }
}
