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

import scala.collection.{mutable, Iterable, Map}
import scala.collection.immutable.{SortedSet, TreeSet}

import com.nvidia.spark.rapids.tool.profiling.ProfileUtils

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.sql.rapids.tool.annotation.{Calculated, Since, WallClock}

/**
 * StageModel is a class to store the information of a stage.
 * Typically, a new instance is created while handling StageSubmitted/StageCompleted events.
 *
 * @param sInfo Snapshot from the stage info loaded from the eventlog
 */
@Since("24.02.3")
class StageModel private(var sInfo: StageInfo) {

  val sId: Int = sInfo.stageId
  val attemptId: Int = sInfo.attemptNumber()

  @WallClock
  @Calculated("Calculated as (submissionTime - completionTime)")
  var duration: Option[Long] = None

  /**
   * Updates the snapshot of Spark's stageInfo to point to the new value and recalculate the
   * duration. Typically, a new StageInfo object is created with both StageSubmitted/StageCompleted
   * events
   * @param newSInfo Spark's StageInfo loaded from StageSubmitted/StageCompleted events.
   */
  private def updateInfo(newSInfo: StageInfo): Unit = {
    this.sInfo = newSInfo
    // recalculate duration
    calculateDuration()
  }

  /**
   * Calculate the duration of the stage.
   * This is called automatically whenever the stage info is updated.
   */
  private def calculateDuration(): Unit = {
    duration =
      ProfileUtils.optionLongMinusOptionLong(sInfo.completionTime, sInfo.submissionTime)
  }

  def hasFailed: Boolean = {
    sInfo.failureReason.isDefined
  }

  def getFailureReason: String = {
    sInfo.failureReason.getOrElse("")
  }

  def getSInfoAccumIds: Iterable[Long] = {
    sInfo.accumulables.keySet
  }


  @Calculated
  @WallClock
  /**
   * Duration won't be defined when neither submitted/completion-Time is defined.
   * @return the WallClock duration of the stage in milliseconds if defined, or 0L otherwise.
   */
  def getDuration: Long = {
    duration.getOrElse(0L)
  }
}

object StageModel {
  /**
   * Factory method to create a new instance of StageModel.
   * The purpose of this method is to encapsulate the logic of updating the stageModel based on
   * the argument.
   * Note that this encapsulation is added to avoid a bug when the Spark's stageInfo was not
   * updated correctly when an event was triggered. This resulted in the stageInfo pointing to an
   * outdated Spark's StageInfo.
   * 1- For a new StageModel: this could be triggered by either stageSubmitted event; or
   *    stageCompleted event.
   * 2- For an existing StageModel: the stageInfo argument is not the same object captured when the
   *    stageModel was created. In that case, we need to call updateInfo to point to the new Spark's
   *    StageInfo and re-calculate the duration.
   * @param stageInfo Spark's StageInfo captured from StageSubmitted/StageCompleted events
   * @param stageModel Option of StageModel represents the existing instance of StageModel that was
   *                   created when the stage was submitted.
   * @return a new instance of StageModel if it exists, or returns the existing StageModel after
   *         updating its sInfo and duration fields.
   */
  def apply(stageInfo: StageInfo, stageModel: Option[StageModel]): StageModel = {
    val sModel = stageModel.getOrElse(new StageModel(stageInfo))
    // Initialization code used to update the duration based on the StageInfo captured from Spark's
    // eventlog
    sModel.updateInfo(stageInfo)
    sModel
  }
}

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
  // - 1st level maps between [Int: StageId -> 2nd Level]
  // - 2nd level maps between [Int:, StageModel]
  // Use Nested Maps to store stageModels which should be faster to retrieve than a map of
  // of composite key (i.e., Tuple).
  // Composite keys would cost more because it implicitly allocates a new object every time there
  // is a read operation from the map.
  private val stageIdToInfo: mutable.HashMap[Int, mutable.HashMap[Int, StageModel]] =
    new mutable.HashMap[Int, mutable.HashMap[Int, StageModel]]()

  // Holds the mapping between AccumulatorIDs to Stages (1-to-N)
  // [Long: AccumId -> SortedSet[Int: StageId]]
  // Note that we keep it as primitive type in case we receive a stageID that does not exist
  // in the stageIdToInfo map.
  private val accumIdToStageId: mutable.HashMap[Long, SortedSet[Int]] =
    new mutable.HashMap[Long, SortedSet[Int]]()

  /**
   * Returns all StageModels that have been created as a result of handling
   * StageSubmitted/StageCompleted-events. This includes stages with multiple attempts.
   * @return Iterable of all StageModels
   */
  def getAllStages: Iterable[StageModel] = stageIdToInfo.values.flatMap(_.values)

  /**
   * Returns all Ids of stage objects created as a result of handling StageSubmitted/StageCompleted.
   * @return Iterable of stage Ids
   */
  def getAllStageIds: Iterable[Int] = stageIdToInfo.keys

  // Internal method used to create new instance of StageModel if it does not exist.
  private def getOrCreateStage(stageInfo: StageInfo): StageModel = {
    val currentAttempts =
      stageIdToInfo.getOrElseUpdate(stageInfo.stageId, new mutable.HashMap[Int, StageModel]())
    val sModel = StageModel(stageInfo, currentAttempts.get(stageInfo.attemptNumber()))
    currentAttempts.put(stageInfo.attemptNumber(), sModel)
    sModel
  }

  // Internal method to update the accumulatorMap using the current stage model.
  private def addAccumIdsToStage(stageModel: StageModel): Unit = {
    val sInfoAccumIds = stageModel.getSInfoAccumIds
    if (sInfoAccumIds.nonEmpty) {
      sInfoAccumIds.foreach { accumId =>
        val stageIds = accumIdToStageId.getOrElseUpdate(accumId, TreeSet[Int]())
        accumIdToStageId.put(accumId, stageIds + stageModel.sId)
      }
    }
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
  @Calculated("Sum all the WallaClockDuration for the given stageId")
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
   * @param sInfo a snapshot from StageInfo captured from StageSubmitted/StageCompleted-events
   * @return existing or new instance of StageModel with (sInfo.stageId, sInfo.attemptID)
   */
  def addStageInfo(sInfo: StageInfo): StageModel = {
    val stage = getOrCreateStage(sInfo)
    addAccumIdsToStage(stage)
    stage
  }

  /**
   * Returns a mapping between AccumulatorID and a single stageId (1-to-1) by taking the head of
   * the list.
   * That getter is used as a temporary hack to avoid callers that expect a 1-to-1 mapping between
   * accumulators and stages. i.e., GenerateDot.writeDotGraph expects a 1-to-1 mapping but it is
   * rarely used for now.
   * @return a Map of AccumulatorID to StageId
   */
  def getAccumToSingleStage(): Map[Long, Int] = {
    accumIdToStageId.map { case (accumId, stageIds) =>
      accumId -> stageIds.head
    }.toMap
  }

  def addAccumIdToStage(stageId: Int, accumIds: Iterable[Long]): Unit = {
    accumIds.foreach { accumId =>
      val stageIds = accumIdToStageId.getOrElseUpdate(accumId, TreeSet[Int]())
      accumIdToStageId.put(accumId, stageIds + stageId)
    }
  }

  def getStagesIdsByAccumId(accumId: Long): Iterable[Int] = {
    accumIdToStageId.getOrElse(accumId, TreeSet[Int]())
  }

  def removeAccumulatorId(accId: Long): Unit = {
    accumIdToStageId.remove(accId)
  }
}
