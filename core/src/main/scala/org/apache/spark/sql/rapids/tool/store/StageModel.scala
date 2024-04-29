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

  // Whenever an event is triggered, the object should update the Stage info.
  private def updatedInfo(newSInfo: StageInfo): Unit = {
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
  def getDuration: Long = {
    duration.getOrElse(0L)
  }
}

object StageModel {
  def apply(stageInfo: StageInfo, stageModel: Option[StageModel]): StageModel = {
    val sModel = stageModel.getOrElse(new StageModel(stageInfo))
    // Initialization code used to update the duration based on the StageInfo captured from Spark's
    // eventlog
    sModel.updatedInfo(stageInfo)
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

  // Use Nested Maps to store stageModels which should be faster to retrieve than a map of
  // of composite key (i.e., Tuple).
  // Composite keys would cost more because it implicitly allocates a new object every time there
  // is a read operation from the map.
  private val stageIdToInfo: mutable.HashMap[Int, mutable.HashMap[Int, StageModel]] =
    new mutable.HashMap[Int, mutable.HashMap[Int, StageModel]]()

  // Holds the mapping between AccumulatorIDs to Stages (1-to-N)
  // Note that we keep it as primitive type in case a stage was not created in the original map.
  private val accumIdToStageId: mutable.HashMap[Long, SortedSet[Int]] =
    new mutable.HashMap[Long, SortedSet[Int]]()

  /**
   * Returns all StageModels that have been created. This includes stages with multiple attempts.
   * @return
   */
  def getAllStages: Iterable[StageModel] = stageIdToInfo.values.flatMap(_.values)

  /**
   * Returns all Ids of stage objects created as a result of handling StageSubmitted/StageCompleted.
   * In order to get all Ids that appeared within the accumulables' Info, then
   * accumIdToStageId.keySet would be more appropriate for that.
   *
   * @return Iterable of all stage Ids
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
    stageIds.foreach { stageId =>
      stageIdToInfo.remove(stageId)
    }
  }

  // Used as the public entry to get or instantiate new Stage.
  // Note that this method has a couple of side effects:
  // 1- recalculates the WallClockDuration of the stage; and
  // 2- adds its accumulator IDs to the accumulator mapping.
  def getOrUpdateStageByInfo(sInfo: StageInfo): StageModel = {
    val stage = getOrCreateStage(sInfo)
    addAccumIdsToStage(stage)
    stage
  }

  // This is used as a temporary workaround to use the same map with consumers that expect a
  // 1-to-1 mapping.
  def reduceAccumMapping(): Map[Long, Int] = {
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
