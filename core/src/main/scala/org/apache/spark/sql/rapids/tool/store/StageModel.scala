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
class StageModel private(sInfo: StageInfo) {

  private val sId: Int = sInfo.stageId
  private val sAttemptId: Int = sInfo.attemptNumber()
  private val sName: String = sInfo.name
  private var sNumTasks: Int = sInfo.numTasks
  private var sDetails: String = sInfo.details
  private var sSubmissionTime: Option[Long] = sInfo.submissionTime
  private var sCompletionTime: Option[Long] = sInfo.completionTime
  private var sFailureReason: Option[String] = sInfo.failureReason

  @WallClock
  @Calculated("Calculated as (submissionTime - completionTime)")
  private var duration: Option[Long] = None

  /**
   * Updates the snapshot of Spark's stageInfo to point to the new value and recalculate the
   * duration. Typically, a new StageInfo object is created with both StageSubmitted/StageCompleted
   * events
   * @param newStageInfo Spark's StageInfo loaded from StageSubmitted/StageCompleted events.
   */
  private def updateInfo(newStageInfo: StageInfo): Unit = {
    sNumTasks = newStageInfo.numTasks
    sDetails = newStageInfo.details
    sSubmissionTime = newStageInfo.submissionTime
    sCompletionTime = newStageInfo.completionTime
    sFailureReason = newStageInfo.failureReason
    calculateDuration()
  }

  /**
   * Calculate the duration of the stage.
   * This is called automatically whenever the stage info is updated.
   */
  private def calculateDuration(): Unit = {
    duration =
      ProfileUtils.optionLongMinusOptionLong(sCompletionTime,sSubmissionTime)
  }

  def hasFailed: Boolean = {
    sFailureReason.isDefined
  }

  def getFailureReason: String = {
    sFailureReason.getOrElse("")
  }

  def getStageId: Int = {
    sId
  }

  def getStageAttemptId: Int = {
    sAttemptId
  }

  def getStageName: String = {
    sName
  }

  def getStageNumTasks: Int = {
    sNumTasks
  }

  def getStageDetails: String = {
    sDetails
  }

  def getStageSubmissionTime: Option[Long] = {
    sSubmissionTime
  }

  def getStageCompletionTime: Option[Long] = {
    sCompletionTime
  }

  /**
   * Duration won't be defined when neither submitted/completion-Time is defined.
   *
   * @return the WallClock duration of the stage in milliseconds if defined, or 0L otherwise.
   */
  @Calculated
  @WallClock
  def getStageDuration: Option[Long] = {
    duration
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


