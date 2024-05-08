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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.sql.rapids.tool.annotation.Since

/**
 * A class to maintain the tasks.
 * There is an alternative design that consists of using StageModel as a parent holding a list of
 * tasks.
 * However, the decision to use a standalone TaskModelManager at the moment is to achieve the
 * following targets:
 * 1- separation between managing the Stages and their Tasks. It is known that Tasks represent the
 * highest percentage of allocated objects in the memory. This allows us to drop the entire tasks
 * management if we dice to aggregate the metrics through the Acccumulables list.
 * 2- flexibility in refactoring the TaskManager to use a permanent storage toward future
 * improvements.
 */
@Since("24.04.1")
class TaskModelManager {
  // A nested HashMap to map between ((Int: stageId, Int: attemptId) -> ArrayBuffer[TaskModel]).
  // We keep track of the attemptId to allow improvement down the road if we decide to handle
  // different Attempts.
  // A new Task is added by TaskEnd event handler.
  // - 1st level maps between [Int: stageId -> 2nd Level]
  // - 2nd level maps between [Int: attemptId -> ArrayBuffer[TaskModel]]
  // Use Nested Maps to store taskModels which should be faster to retrieve than a map of
  // of composite key (i.e., Tuple).
  // Composite keys would cost more because it implicitly allocates a new object every time there
  // is a read operation from the map.
  val stageAttemptToTasks: mutable.HashMap[Int, mutable.HashMap[Int, ArrayBuffer[TaskModel]]] =
    new mutable.HashMap[Int, mutable.HashMap[Int, ArrayBuffer[TaskModel]]]()

  // Given a Spark taskEnd event, create a new Task and add it to the Map.
  def addTaskFromEvent(event: SparkListenerTaskEnd): Unit = {
    val taskModel = TaskModel(event)
    val stageAttempts =
      stageAttemptToTasks.getOrElseUpdate(event.stageId,
        new mutable.HashMap[Int, ArrayBuffer[TaskModel]]())
    val attemptToTasks =
      stageAttempts.getOrElseUpdate(event.stageAttemptId, ArrayBuffer[TaskModel]())
    attemptToTasks += taskModel
  }

  // Given a stageID and stageAttemptID, return all tasks or Empty iterable.
  // The returned tasks will be filtered by the the predicate func if the latter exists
  def getTasks(stageID: Int, stageAttemptID: Int,
      predicateFunc: Option[TaskModel => Boolean] = None): Iterable[TaskModel] = {
    stageAttemptToTasks.get(stageID).flatMap { stageAttempts =>
      stageAttempts.get(stageAttemptID).map { tasks =>
        if (predicateFunc.isDefined) {
          tasks.filter(predicateFunc.get)
        } else {
          tasks
        }
      }
    }.getOrElse(Iterable.empty)
  }

  // Returns the combined list of tasks that belong to a specific stageID.
  // This includes tasks belonging to different stageAttempts.
  // This is mainly supporting callers that use stageID (without attemptID).
  def getAllTasksStageAttempt(stageID: Int): Iterable[TaskModel] = {
    stageAttemptToTasks.get(stageID).map { stageAttempts =>
      stageAttempts.values.flatten
    }.getOrElse(Iterable.empty)
  }

  // Returns the combined list of all tasks that satisfy the predicate function if it exists.
  def getAllTasks(predicateFunc: Option[TaskModel => Boolean] = None): Iterable[TaskModel] = {
    stageAttemptToTasks.collect {
      case (_, attemptsToTasks) if attemptsToTasks.nonEmpty =>
        if (predicateFunc.isDefined) {
          attemptsToTasks.values.flatten.filter(predicateFunc.get)
        } else {
          attemptsToTasks.values.flatten
        }
    }.flatten
  }

  // Return a list of tasks that failed within all the stageAttempts
  def getAllFailedTasks: Iterable[TaskModel] = {
    getAllTasks(Some(!_.successful))
  }

  // Given an iterable of StageIDs, return all the tasks that belong to these stages. Note that
  // this include tasks within multiple stageAttempts.
  // This is implemented to support callers that do not use stageAttemptID in their logic.
  def getTasksByStageIds(stageIds: Iterable[Int]): Iterable[TaskModel] = {
    stageIds.flatMap(getAllTasksStageAttempt)
  }
}
