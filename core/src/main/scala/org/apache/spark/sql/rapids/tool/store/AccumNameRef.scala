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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.rapids.tool.util.CachedRefBase
import org.apache.spark.sql.rapids.tool.util.EventUtils.normalizeMetricName


/**
 * Accumulator Name Reference
 * This maintains references to all accumulator names
 * @param value the accumulator name to be stored
 */
case class AccumNameRef(value: String) extends CachedRefBase[AccumNameRef] {
  // generate and store the CSV formatted name as it is used by multiple rows, and it can be shared
  // by multiple threads.
  // There is a tradeoff between caching this value Vs generating it every time.
  // We opt to use this optimization because while writing the CSV files, each row is going to
  // create a new CSV string even though they represent the same AccumulatorName.
}

object AccumNameRef {
  // A global table to store reference to all accumulator names. The map is accessible by all
  // threads (different applications) running in parallel. This avoids duplicate work across
  // different threads.
  val NAMES_TABLE: ConcurrentHashMap[String, AccumNameRef] =
    new ConcurrentHashMap[String, AccumNameRef]()

  // Dummy AccNameRef to represent None accumulator names. This is an optimization to avoid
  // storing an option[string] for all accumulable names which leads to "get-or-else" everywhere.
  val EMPTY_ACC_NAME_REF: AccumNameRef =
    CachedRefBase.createDefault("N/A", NAMES_TABLE, fromString)

  val SEMAPHORE_WAIT_ACC_NAME_REF: AccumNameRef =
    CachedRefBase.createDefault("gpuSemaphoreWait", NAMES_TABLE, fromString)

  def getOrCreateAccumNameRef(nameKey: String): AccumNameRef = {
    CachedRefBase.getOrCreate(nameKey, NAMES_TABLE, fromString)
  }

  // Intern the accumulator name if it is not already present in the table.
  def getOrCreateAccumNameRef(name: Option[String]): AccumNameRef = {
    name.map(getOrCreateAccumNameRef).getOrElse(EMPTY_ACC_NAME_REF)
  }

  // Allocate a new AccNameRef for the given accumulator name.
  private def fromString(value: String): AccumNameRef =
    AccumNameRef(normalizeMetricName(value))
}
