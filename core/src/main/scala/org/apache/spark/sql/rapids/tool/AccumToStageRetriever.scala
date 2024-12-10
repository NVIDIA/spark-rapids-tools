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

/**
 * Trait that defines the interface for retrieving stage IDs from accumulables.
 * This is used to map accumulables to stages. We use it as interface in order to allow to separate
 * the logic and use dummy different implementations and mocks for testing when needed.
 */
trait AccumToStageRetriever {
  /**
   * Given a sequence of accumIds, return a set of stage IDs that are associated with the
   * accumIds. Note that this method can only be called after the accumulables have been fully
   * processed.
   */
  def getStageIDsFromAccumIds(accumIds: Seq[Long]): Set[Int]
}
