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

import org.apache.spark.sql.rapids.tool.util.EventUtils.normalizeMetricName

case class AccNameRef(value: String)

object AccNameRef {
  private val EMPTY_ACC_NAME_REF: AccNameRef = new AccNameRef("N/A")
  val NAMES_TABLE: ConcurrentHashMap[String, AccNameRef] = {
    val initMap = new ConcurrentHashMap[String, AccNameRef]()
    initMap.put("gpuSemaphoreWait", fromString("gpuSemaphoreWait"))
    initMap
  }

  def internAccName(name: Option[String]): AccNameRef = {
    name match {
      case Some(n) =>
        NAMES_TABLE.computeIfAbsent(n, AccNameRef.fromString)
      case _ =>
        AccNameRef.EMPTY_ACC_NAME_REF
    }
  }

  def fromString(value: String): AccNameRef =
    new AccNameRef(normalizeMetricName(value))
}
