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

class AccumManager {
  val accumInfoMap: mutable.HashMap[Long, AccumInfo] = {
    new mutable.HashMap[Long, AccumInfo]()
  }

  def getOrCreateAccumInfo(id: Long, name: Option[String]): AccumInfo = {
    accumInfoMap.getOrElseUpdate(id, new AccumInfo(AccMetaRef(id, name)))
  }

  def addAccToStage(stageId: Int, accumulableInfo: AccumulableInfo): Unit = {
    val accumInfoRef = getOrCreateAccumInfo(accumulableInfo.id, accumulableInfo.name)
    accumInfoRef.addAccToStage(stageId, accumulableInfo)
  }

  def addAccToTask(stageId: Int, taskId: Long, accumulableInfo: AccumulableInfo): Unit = {
    val accumInfoRef = getOrCreateAccumInfo(accumulableInfo.id, accumulableInfo.name)
    accumInfoRef.addAccToTask(stageId, taskId, accumulableInfo)
  }

  def getAccStageIds(id: Long): Set[Int] = {
    // TODO: Does Set.empty allocates memory? or it uses a constant in scala?
    accumInfoMap.get(id).map(_.getStageIds).getOrElse(Set.empty)
  }

  def calculateAccStats(id: Long): Option[StatisticsMetrics] = {
    accumInfoMap.get(id).map(_.calculateAccStats)
  }

  def getMaxStageValue(id: Long): Option[Long] = {
    accumInfoMap.get(id).map(_.getMaxStageValue.get)
  }
}
