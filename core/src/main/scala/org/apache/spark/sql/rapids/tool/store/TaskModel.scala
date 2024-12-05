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

import java.util.concurrent.TimeUnit

import org.apache.spark.TaskFailedReason
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.sql.rapids.tool.annotation.Since

@Since("24.04.1")
case class TaskModel(
    stageId: Int,
    stageAttemptId: Int,
    taskType: String,
    endReason: String,
    taskId: Long,
    attempt: Int,
    launchTime: Long,
    finishTime: Long,
    duration: Long,
    successful: Boolean,
    executorId: String,
    host: String,
    taskLocality: String,
    speculative: Boolean,
    gettingResultTime: Long,
    executorDeserializeTime: Long,
    executorDeserializeCPUTime: Long,
    executorRunTime: Long,
    executorCPUTime: Long,
    peakExecutionMemory: Long,
    resultSize: Long,
    jvmGCTime: Long,
    resultSerializationTime: Long,
    memoryBytesSpilled: Long,
    diskBytesSpilled: Long,
    // Note: sr stands for ShuffleRead
    sr_remoteBlocksFetched: Long,
    sr_localBlocksFetched: Long,
    sr_fetchWaitTime: Long,
    sr_remoteBytesRead: Long,
    sr_remoteBytesReadToDisk: Long,
    sr_localBytesRead: Long,
    sr_totalBytesRead: Long,
    // Note: sw stands for ShuffleWrite
    sw_bytesWritten: Long,
    sw_writeTime: Long,
    sw_recordsWritten: Long,
    input_bytesRead: Long,
    input_recordsRead: Long,
    output_bytesWritten: Long,
    output_recordsWritten: Long)

object TaskModel {
  def apply(event: SparkListenerTaskEnd): TaskModel = {
    val reason = event.reason match {
      case failed: TaskFailedReason =>
        failed.toErrorString
      case _ =>
        event.reason.toString
    }

    TaskModel(
      event.stageId,
      event.stageAttemptId,
      event.taskType,
      reason,
      event.taskInfo.taskId,
      event.taskInfo.attemptNumber,
      event.taskInfo.launchTime,
      event.taskInfo.finishTime,
      event.taskInfo.duration,
      event.taskInfo.successful,
      event.taskInfo.executorId,
      event.taskInfo.host,
      event.taskInfo.taskLocality.toString,
      event.taskInfo.speculative,
      event.taskInfo.gettingResultTime,
      event.taskMetrics.executorDeserializeTime,
      TimeUnit.NANOSECONDS.toMillis(event.taskMetrics.executorDeserializeCpuTime),
      event.taskMetrics.executorRunTime,
      TimeUnit.NANOSECONDS.toMillis(event.taskMetrics.executorCpuTime),
      event.taskMetrics.peakExecutionMemory,
      event.taskMetrics.resultSize,
      event.taskMetrics.jvmGCTime,
      event.taskMetrics.resultSerializationTime,
      event.taskMetrics.memoryBytesSpilled,
      event.taskMetrics.diskBytesSpilled,
      event.taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      event.taskMetrics.shuffleReadMetrics.localBlocksFetched,
      event.taskMetrics.shuffleReadMetrics.fetchWaitTime,
      event.taskMetrics.shuffleReadMetrics.remoteBytesRead,
      event.taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
      event.taskMetrics.shuffleReadMetrics.localBytesRead,
      event.taskMetrics.shuffleReadMetrics.totalBytesRead,
      event.taskMetrics.shuffleWriteMetrics.bytesWritten,
      event.taskMetrics.shuffleWriteMetrics.writeTime,
      event.taskMetrics.shuffleWriteMetrics.recordsWritten,
      event.taskMetrics.inputMetrics.bytesRead,
      event.taskMetrics.inputMetrics.recordsRead,
      event.taskMetrics.outputMetrics.bytesWritten,
      event.taskMetrics.outputMetrics.recordsWritten)
  }
}
