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

import com.nvidia.spark.rapids.tool.profiling.ProfileUtils

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


