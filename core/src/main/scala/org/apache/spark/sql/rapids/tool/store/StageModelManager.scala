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

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.sql.rapids.tool.annotation.{Calculated, Since, WallClock}

/**
 * A class to maintain the stage models.
 */
@Since("24.02.3")
class StageModelManager extends Logging {
  // TODO: 1- Support multithreading and concurrency.
  //       2- Convert the storage into generic that can be replaced by file-backed storage later
  //          when needed.

  // A nested HashMap to map between ((Int: stageId, Int: attemptId) -> StageModel).
  // We keep track of the attemptId to allow improvement down the road if we decide to handle
  // different Attempts.
  // - 1st level maps between [Int: stageId -> 2nd Level]
  // - 2nd level maps between [Int: attemptId -> StageModel]
  // Use Nested Maps to store stageModels which should be faster to retrieve than a map of
  // of composite key (i.e., Tuple).
  // Composite keys would cost more because it implicitly allocates a new object every time there
  // is a read operation from the map.
  // Finally use SortedMaps to keep the map sorted. That way iterating on the map will be orders
  // by IDs/AttemptIDs.
  private val stageIdToInfo: mutable.SortedMap[Int, mutable.SortedMap[Int, StageModel]] =
    mutable.SortedMap[Int, mutable.SortedMap[Int, StageModel]]()

  /**
   * Returns all StageModels that have been created as a result of handling
   * StageSubmitted/StageCompleted-events. This includes stages with multiple attempts.
   *
   * @return Iterable of all StageModels
   */
  def getAllStages: Iterable[StageModel] = stageIdToInfo.values.flatMap(_.values)

  def getAllCompletedStages: Iterable[StageModel] = {
    stageIdToInfo.values.flatMap(_.values).filter(stage => {
      stage.hasCompleted && !stage.hasFailed
    })
  }

  /**
   * Returns all Ids of stage objects created as a result of handling StageSubmitted/StageCompleted.
   *
   * @return Iterable of stage Ids
   */
  def getAllStageIds: Iterable[Int] = stageIdToInfo.keys

  // Internal method used to create new instance of StageModel if it does not exist.
  private def getOrCreateStage(stageInfo: StageInfo): StageModel = {
    val currentAttempts =
      stageIdToInfo.getOrElseUpdate(stageInfo.stageId, mutable.SortedMap[Int, StageModel]())
    val sModel = StageModel(stageInfo, currentAttempts.get(stageInfo.attemptNumber()))
    currentAttempts.put(stageInfo.attemptNumber(), sModel)
    sModel
  }

  // Used to retrieve a stage model and does not create a new instance if it does not exist.
  def getStage(stageId: Int, attemptId: Int): Option[StageModel] = {
    stageIdToInfo.get(stageId).flatMap(_.get(attemptId))
  }

  // Used to retrieve list of stages by stageId (in case multiple attempts exist)
  def getStagesByIds(stageIds: Iterable[Int]): Iterable[StageModel] = {
    stageIds.flatMap { stageId =>
      stageIdToInfo.get(stageId).map(_.values)
    }.flatten
  }

  // Returns all stages that have failed
  def getFailedStages: Iterable[StageModel] = {
    getAllStages.filter(_.hasFailed)
  }

  // Shortcut to get the duration of a stage by stageId
  @WallClock
  @Calculated("Sum all the WallClockDuration for the given stageId")
  def getDurationById(stageId: Int): Long = {
    stageIdToInfo.get(stageId).map { attempts =>
      attempts.values.map(_.getDuration).sum
    }.getOrElse(0L)
  }

  // Remove all Stages by stageId
  def removeStages(stageIds: Iterable[Int]): Unit = {
    stageIdToInfo --= stageIds
  }

  /**
   * Given a Spark.StageInfo, this method will return an existing StageModel that has
   * (stageId, attemptId) if it exists. Otherwise, it will create a new instance of StageModel.
   * Once the stageModel instance is obtained, it will update the accumulator mapping based on the
   * elements of sInfo.accumulables.
   *
   * @param sInfo a snapshot from StageInfo captured from StageSubmitted/StageCompleted-events
   * @return existing or new instance of StageModel with (sInfo.stageId, sInfo.attemptID)
   */
  def addStageInfo(sInfo: StageInfo): StageModel = {
    val stage = getOrCreateStage(sInfo)
    stage
  }
}
