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

package org.apache.spark.sql.rapids.tool

import java.util.concurrent.ConcurrentHashMap

object AppAttemptTracker {
  private val attemptIdMap = new ConcurrentHashMap[String, Int]()

  /**
   * Update the map with the attemptId if it is greater (newer) than the existing attemptId
   * for the given appId.
   */
  def registerNewerAttemptId(appId: String, attemptId: Int): Unit = {
    attemptIdMap.compute(appId, (_, v) => {
      Option(v) match {
        case Some(existingAttemptId) => Math.max(existingAttemptId, attemptId)
        case None => attemptId
      }
    })
  }

  /**
   * Check if the attemptId is older than the existing attemptId for the given appId.
   */
  def isOlderAttemptId(appId: String, attemptId: Int): Boolean = {
    val existingAttemptId = attemptIdMap.getOrDefault(appId, 1)
    if (existingAttemptId == 1) {
      // If the attemptId is 1, then it is the first attempt and not older.
      false
    } else {
      attemptId < existingAttemptId
    }
  }
}
