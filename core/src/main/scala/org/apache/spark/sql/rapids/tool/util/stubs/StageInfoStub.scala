/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.tool.util.stubs

import org.apache.spark.scheduler.StageInfo
import org.apache.spark.sql.rapids.tool.annotation.ToolsReflection

@ToolsReflection("Common",
  "StageInfo is a common class used in all versions of Spark but the constructor signature is" +
    " different across versions.")
case class StageInfoStub(
    stageId: Int,
    attemptId: Int,
    name: String,
    numTasks: Int,
    details: String,
    /** When this stage was submitted from the DAGScheduler to a TaskScheduler. */
    submissionTime: Option[Long] = None,
    /** Time when the stage completed or when the stage was cancelled. */
    completionTime: Option[Long] = None,
    /** If the stage failed, the reason why. */
    failureReason: Option[String] = None) {

  def attemptNumber(): Int = attemptId
}

object StageInfoStub {
  def fromStageInfo(stageInfo: StageInfo): StageInfoStub = {
    StageInfoStub(
      stageInfo.stageId,
      stageInfo.attemptNumber(),
      stageInfo.name,
      stageInfo.numTasks,
      stageInfo.details,
      stageInfo.submissionTime,
      stageInfo.completionTime,
      stageInfo.failureReason)
  }
}
