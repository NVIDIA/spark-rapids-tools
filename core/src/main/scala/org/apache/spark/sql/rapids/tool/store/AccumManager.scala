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

import scala.collection.{mutable, Map}

import com.nvidia.spark.rapids.tool.analysis.StatisticsMetrics

import org.apache.spark.scheduler.AccumulableInfo

/**
 * A class that manages task/stage accumulables -
 * maintains a map of accumulable id to AccumInfo
 */
class AccumManager {
  val accumInfoMap: mutable.HashMap[Long, AccumInfo] = {
    new mutable.HashMap[Long, AccumInfo]()
  }

  private def getOrCreateAccumInfo(id: Long, name: Option[String]): AccumInfo = {
    accumInfoMap.getOrElseUpdate(id, new AccumInfo(AccumMetaRef(id, name)))
  }

  def addAccToStage(stageId: Int, accumulableInfo: AccumulableInfo): Unit = {
    val accumInfoRef = getOrCreateAccumInfo(accumulableInfo.id, accumulableInfo.name)
    accumInfoRef.addAccumToStage(stageId, accumulableInfo)
  }

  def addAccToTask(stageId: Int, accumulableInfo: AccumulableInfo): Unit = {
    val accumInfoRef = getOrCreateAccumInfo(accumulableInfo.id, accumulableInfo.name)
    accumInfoRef.addAccumToTask(stageId, accumulableInfo)
  }

  def getAccStageIds(id: Long): Set[Int] = {
    accumInfoMap.get(id).map(_.getStageIds).getOrElse(Set.empty)
  }

  def getAccumSingleStage: Map[Long, Int] = {
    accumInfoMap.map { case (id, accInfo) =>
      (id, accInfo.getMinStageId)
    }.toMap
  }

  def removeAccumInfo(id: Long): Option[AccumInfo] = {
    accumInfoMap.remove(id)
  }

  def calculateAccStats(id: Long): Option[StatisticsMetrics] = {
    accumInfoMap.get(id).map(_.calculateAccStats())
  }

  def getMaxStageValue(id: Long): Option[Long] = {
    accumInfoMap.get(id).map(_.getMaxTotalAcrossStages.get)
  }

  /**
   * Applies the function `f` to each AccumInfo in the accumInfoMap.
   */
  def applyToAccumInfoMap(f: AccumInfo => Unit): Unit = {
    accumInfoMap.values.foreach(f)
  }
}
