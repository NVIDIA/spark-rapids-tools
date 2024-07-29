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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.sql.rapids.tool.util.EventUtils.{normalizeMetricName, parseAccumFieldToLong}

case class AccNameRef(value: String) {

}

object AccNameRef {
  val EMPTY_ACC_NAME_REF: AccNameRef = new AccNameRef("N/A")
  def fromString(value: String): AccNameRef =
    new AccNameRef(normalizeMetricName(value))
}

object AccNameTable {
  val namesTable: ConcurrentHashMap[String, AccNameRef] =
    new ConcurrentHashMap[String, AccNameRef]()
  def internAccName(name: Option[String]): AccNameRef = {
    name match {
      case Some(n) =>
        namesTable.computeIfAbsent(n, AccNameRef.fromString)
      case None =>
        AccNameRef.EMPTY_ACC_NAME_REF
    }
  }
}

case class AccMetaRef(id: Long, name: AccNameRef) {

}

object AccMetaRef {
  def apply(id: Long, name: Option[String]): AccMetaRef =
    new AccMetaRef(id, AccNameTable.internAccName(name))
}

class AccumInfo(var infoRef: AccMetaRef) {
  val taskUpdatesMap: mutable.HashMap[Long, Long] =
    new mutable.HashMap[Long, Long]()
  var stageValuesMap: mutable.HashMap[Int, Long] =
    new mutable.HashMap[Int, Long]()
}

// not thread safe
class AccumInfoStore {
  val accumInfoMap: mutable.HashMap[Long, AccumInfo] = {
    new mutable.HashMap[Long, AccumInfo]()
  }

  def getOrCreateAccumInfo(id: Long, name: Option[String]): AccumInfo = {
    accumInfoMap.getOrElseUpdate(id, new AccumInfo(AccMetaRef(id, name)))
  }

  def addAccToStage(accumInfoRef: AccumInfo,
      stageId: Int,
      accumulableInfo: AccumulableInfo,
      update: Option[Long] = None): Unit = {
    val value = accumulableInfo.value.flatMap(parseAccumFieldToLong)
    value match {
      case Some(v) =>
        accumInfoRef.stageValuesMap.put(stageId, v)
      case None =>
        accumInfoRef.stageValuesMap.put(stageId, update.getOrElse(0L))
    }
  }

  def addAccToStage(stageId: Int, accumulableInfo: AccumulableInfo): Unit = {
    val accumInfo = getOrCreateAccumInfo(accumulableInfo.id, accumulableInfo.name)
    addAccToStage(accumInfo, stageId, accumulableInfo)
  }

  def addAccToTask(stageId: Int, taskId: Long, accumulableInfo: AccumulableInfo): Unit = {
    val accumInfoRef = getOrCreateAccumInfo(accumulableInfo.id, accumulableInfo.name)
    val update = accumulableInfo.update.flatMap(parseAccumFieldToLong)
    update match {
      case Some(v) =>
        accumInfoRef.taskUpdatesMap.put(taskId, v)
      case None =>
        accumInfoRef.taskUpdatesMap.put(taskId, 0L)
    }
    addAccToStage(accumInfoRef, stageId, accumulableInfo, update)
  }
}
