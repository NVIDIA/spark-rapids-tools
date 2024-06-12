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
import org.apache.spark.status.KVUtils.KVIndexParam

@Since("24.04.1")
case class TaskModel(
                      @KVIndexParam stageId: Int = 1,
                      stageAttemptId: Int = 1,
                      taskType: String = "a",
                      endReason: String = "a",
                      taskId: Long = 1L,
                      attempt: Int = 1,
                      launchTime: Long = 1L,
                      finishTime: Long = 1L,
                      duration: Long = 1L,
                      successful: Boolean = true,
                      executorId: String = "a",
                      host: String = "a",
                      taskLocality: String = "a",
                      speculative: Boolean = false,
                      gettingResultTime: Long = 1L,
                      executorDeserializeTime: Long = 1L,
                      executorDeserializeCPUTime: Long = 1L,
                      executorRunTime: Long = 1L,
                      executorCPUTime: Long = 1L,
                      peakExecutionMemory: Long = 1L,
                      resultSize: Long = 1L,
                      jvmGCTime: Long = 1L,
                      resultSerializationTime: Long = 1L,
                      memoryBytesSpilled: Long = 1L,
                      diskBytesSpilled: Long = 1L,
                      // Note: sr stands for ShuffleRead
                      sr_remoteBlocksFetched: Long = 1L,
                      sr_localBlocksFetched: Long = 1L,
                      sr_fetchWaitTime: Long = 1L,
                      sr_remoteBytesRead: Long= 1L,
                      sr_remoteBytesReadToDisk: Long = 1L,
                      sr_localBytesRead: Long = 1L,
                      sr_totalBytesRead: Long = 1L,
                      // Note: sw stands for ShuffleWrite
                      sw_bytesWritten: Long = 1L,
                      sw_writeTime: Long = 1L,
                      sw_recordsWritten: Long = 1L,
                      input_bytesRead: Long = 1L,
                      input_recordsRead: Long = 1L,
                      output_bytesWritten: Long = 1L,
                      output_recordsWritten: Long = 1L)

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
      TimeUnit.NANOSECONDS.toMillis(event.taskMetrics.shuffleWriteMetrics.writeTime),
      event.taskMetrics.shuffleWriteMetrics.recordsWritten,
      event.taskMetrics.inputMetrics.bytesRead,
      event.taskMetrics.inputMetrics.recordsRead,
      event.taskMetrics.outputMetrics.bytesWritten,
      event.taskMetrics.outputMetrics.recordsWritten)
  }
}
