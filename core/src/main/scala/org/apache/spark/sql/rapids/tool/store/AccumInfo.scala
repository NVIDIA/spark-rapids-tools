/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
  /**
   * Maps stageId to a tuple of:
   * - Total accumulator value for the stage (Long)
   * - Statistical metrics for task-level updates (StatisticsMetrics)
   */
  val stageValuesMap: mutable.HashMap[Int, (Long, StatisticsMetrics)] =
    new mutable.HashMap[Int, (Long, StatisticsMetrics)]()

  /**
   * Adds or updates accumulator information for a stage.
   * Called during StageCompleted or TaskEnd events processing.
   *
   * @param stageId The ID of the stage
   * @param accumulableInfo Accumulator information from the event
   * @param update Optional task update value for TaskEnd events
   */
  def addAccumToStage(stageId: Int,
      accumulableInfo: AccumulableInfo,
      update: Option[Long] = None): Unit = {
    val parsedValue = accumulableInfo.value.flatMap(parseAccumFieldToLong)
    // in case there is an out of order event, the value showing up later could be
    // lower-than the previous value. In that case we should take the maximum.
    val existingEntry = stageValuesMap.getOrElse(stageId,
      (0L, StatisticsMetrics(0L, 0L, 0L, 0L)))
    val incomingValue = parsedValue match {
      case Some(v) => v
      case _ => update.getOrElse(0L)
    }
    val newValue = Math.max(existingEntry._1, incomingValue)
    stageValuesMap.put(stageId, (newValue, existingEntry._2))
  }

  /**
   * Processes task-level accumulator updates and updates stage-level statistics.
   * Called during TaskEnd event processing.
   *
   * @param stageId The ID of the stage containing the task
   * @param taskId The ID of the completed task
   * @param accumulableInfo Accumulator information from the TaskEnd event
   */
  def addAccumToTask(stageId: Int, taskId: Long, accumulableInfo: AccumulableInfo): Unit = {
    // For each incoming Task, we update the statistics with min, rolling average
    // max and count of tasks

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
  }

  /**
   * Returns all stage IDs that have accumulator updates.
   *
   * @return Set of stage IDs
   */
  def getStageIds: Set[Int] = {
    stageValuesMap.keySet.toSet
  }

  /**
   * Returns the smallest stage ID in the accumulator data.
   *
   * @return Minimum stage ID
   */
  def getMinStageId: Int = {
    stageValuesMap.keys.min
  }

  /**
   * Calculates aggregate statistics across all stages for this accumulator.
   *
   * @return Combined StatisticsMetrics including min, median, max, and total
   */
  def calculateAccStats(): StatisticsMetrics = {
    // While reducing we leverage the count stored during TaskEnd processing
    // We use it to update the rolling average
    // However, the sum ( calculated using sum of all values for stages )
    // is returned at the end
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

  /**
   * Retrieves statistical metrics for a specific stage.
   *
   * @param stageId The ID of the stage
   * @return Option containing StatisticsMetrics if stage exists, None otherwise
   */
  def calculateAccStatsForStage(stageId: Int): Option[StatisticsMetrics] = {
    val output = stageValuesMap.get(stageId)
    output match {
      case Some(x) => Some(StatisticsMetrics(x._2.min, x._2.med, x._2.max, x._1))
      case None => None
    }
  }

  /**
   * Returns the highest accumulator value across all stages.
   *
   * @return Option containing maximum value if stages exist, None otherwise
   */
  def getMaxStageValue: Option[Long] = {
    if (stageValuesMap.values.isEmpty) {
      None
    } else {
      Some(stageValuesMap.values.map(_._1).max)
    }
  }
}
