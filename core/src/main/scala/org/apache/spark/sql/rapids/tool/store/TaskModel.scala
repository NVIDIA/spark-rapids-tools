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

package org.apache.spark.sql.rapids.tool.store

import scala.collection.immutable

import org.apache.spark.TaskFailedReason
import org.apache.spark.scheduler.SparkListenerTaskEnd



object LongMetrics {
  val LAUNCH_TIME = 1
  val FINISH_TIME = 2
  val DURATION = 3
  val GETTING_RESULT_TIME = 4
  val EXEC_DESERIALIZE_TIME = 5
  val EXEC_DESERIALIZE_CPU_TIME = 6
  val EXEC_RUN_TIME = 7
  val EXEC_CPU_TIME = 8
  val PEAK_MEMORY = 9
  val RESULT_SIZE = 10
  val JVM_GC_TIME = 11
  val RESULT_SER_TIME = 12
  val MEM_SPILLED = 13
  val DISK_SPILLED = 14
  val SR_REMOTE_BLOCKS = 15
  val SR_LOCAL_BLOCKS = 16
  val SR_FETCH_WAIT = 17
  val SR_REMOTE_BYTES = 18
  val SR_REMOTE_DISK_BYTES = 19
  val SR_LOCAL_BYTES = 20
  val SR_TOTAL_BYTES = 21
  val SW_BYTES = 22
  val SW_TIME = 23
  val SW_RECORDS = 24
  val INPUT_BYTES = 25
  val INPUT_RECORDS = 26
  val OUTPUT_BYTES = 27
  val OUTPUT_RECORDS = 28
}

class TaskModel(
  val stageId: Int,
  val stageAttemptId: Int,
  val taskType: String,
  val endReason: String,
  val taskId: Long,
  val attempt: Int,
  val successful: Boolean,
  val executorId: String,
  val host: String,
  val taskLocality: String,
  val speculative: Boolean,
  private val taskMetrics: immutable.IntMap[Long]) {
  import LongMetrics._

  def launchTime: Long = taskMetrics.getOrElse(LAUNCH_TIME, 0L)
  def finishTime: Long = taskMetrics.getOrElse(FINISH_TIME, 0L)
  def duration: Long = taskMetrics.getOrElse(DURATION, 0L)
  def executorDeserializeTime: Long = taskMetrics.getOrElse(EXEC_DESERIALIZE_TIME, 0L)
  def executorDeserializeCPUTime: Long = taskMetrics.getOrElse(EXEC_DESERIALIZE_CPU_TIME, 0L)
  def executorRunTime: Long = taskMetrics.getOrElse(EXEC_RUN_TIME, 0L)
  def executorCPUTime: Long = taskMetrics.getOrElse(EXEC_CPU_TIME, 0L)
  def peakExecutionMemory: Long = taskMetrics.getOrElse(PEAK_MEMORY, 0L)
  def resultSize: Long = taskMetrics.getOrElse(RESULT_SIZE, 0L)
  def jvmGCTime: Long = taskMetrics.getOrElse(JVM_GC_TIME, 0L)
  def resultSerializationTime: Long = taskMetrics.getOrElse(RESULT_SER_TIME, 0L)
  def memoryBytesSpilled: Long = taskMetrics.getOrElse(MEM_SPILLED, 0L)
  def diskBytesSpilled: Long = taskMetrics.getOrElse(DISK_SPILLED, 0L)
  def sr_remoteBlocksFetched: Long = taskMetrics.getOrElse(SR_REMOTE_BLOCKS, 0L)
  def sr_localBlocksFetched: Long = taskMetrics.getOrElse(SR_LOCAL_BLOCKS, 0L)
  def sr_fetchWaitTime: Long = taskMetrics.getOrElse(SR_FETCH_WAIT, 0L)
  def sr_remoteBytesRead: Long = taskMetrics.getOrElse(SR_REMOTE_BYTES, 0L)
  def sr_remoteBytesReadToDisk: Long = taskMetrics.getOrElse(SR_REMOTE_DISK_BYTES, 0L)
  def sr_localBytesRead: Long = taskMetrics.getOrElse(SR_LOCAL_BYTES, 0L)
  def sr_totalBytesRead: Long = taskMetrics.getOrElse(SR_TOTAL_BYTES, 0L)
  def sw_bytesWritten: Long = taskMetrics.getOrElse(SW_BYTES, 0L)
  def sw_writeTime: Long = taskMetrics.getOrElse(SW_TIME, 0L)
  def sw_recordsWritten: Long = taskMetrics.getOrElse(SW_RECORDS, 0L)
  def input_bytesRead: Long = taskMetrics.getOrElse(INPUT_BYTES, 0L)
  def input_recordsRead: Long = taskMetrics.getOrElse(INPUT_RECORDS, 0L)
  def output_bytesWritten: Long = taskMetrics.getOrElse(OUTPUT_BYTES, 0L)
  def output_recordsWritten: Long = taskMetrics.getOrElse(OUTPUT_RECORDS, 0L)
}

object TaskModel {
  import LongMetrics._

  def apply(event: SparkListenerTaskEnd): TaskModel = {
    val taskInfo = event.taskInfo
    val metrics = event.taskMetrics
    val shuffleRead = metrics.shuffleReadMetrics
    val shuffleWrite = metrics.shuffleWriteMetrics
    val input = metrics.inputMetrics
    val output = metrics.outputMetrics
    val reason = event.reason match {
      case failed: TaskFailedReason =>
        failed.toErrorString
      case _ =>
        event.reason.toString
    }


    val taskMetrics = immutable.IntMap.empty[Long]

    def storeIfNonZero(field: Int, value: Long): Unit =
      if (value != 0) taskMetrics.updated(field, value)

    storeIfNonZero(LAUNCH_TIME, taskInfo.launchTime)
    storeIfNonZero(FINISH_TIME, taskInfo.finishTime)
    storeIfNonZero(DURATION, taskInfo.duration)
    storeIfNonZero(GETTING_RESULT_TIME, taskInfo.gettingResultTime)
    storeIfNonZero(EXEC_DESERIALIZE_TIME, metrics.executorDeserializeTime)
    storeIfNonZero(EXEC_DESERIALIZE_CPU_TIME, metrics.executorDeserializeCpuTime)
    storeIfNonZero(EXEC_RUN_TIME, metrics.executorRunTime)
    storeIfNonZero(EXEC_CPU_TIME, metrics.executorCpuTime)
    storeIfNonZero(PEAK_MEMORY, metrics.peakExecutionMemory)
    storeIfNonZero(RESULT_SIZE, metrics.resultSize)
    storeIfNonZero(JVM_GC_TIME, metrics.jvmGCTime)
    storeIfNonZero(RESULT_SER_TIME, metrics.resultSerializationTime)
    storeIfNonZero(MEM_SPILLED, metrics.memoryBytesSpilled)
    storeIfNonZero(DISK_SPILLED, metrics.diskBytesSpilled)

    storeIfNonZero(SR_REMOTE_BLOCKS, shuffleRead.remoteBlocksFetched)
    storeIfNonZero(SR_LOCAL_BLOCKS, shuffleRead.localBlocksFetched)
    storeIfNonZero(SR_FETCH_WAIT, shuffleRead.fetchWaitTime)
    storeIfNonZero(SR_REMOTE_BYTES, shuffleRead.remoteBytesRead)
    storeIfNonZero(SR_REMOTE_DISK_BYTES, shuffleRead.remoteBytesReadToDisk)
    storeIfNonZero(SR_LOCAL_BYTES, shuffleRead.localBytesRead)
    storeIfNonZero(SR_TOTAL_BYTES, shuffleRead.totalBytesRead)

    storeIfNonZero(SW_BYTES, shuffleWrite.bytesWritten)
    storeIfNonZero(SW_TIME, shuffleWrite.writeTime)
    storeIfNonZero(SW_RECORDS, shuffleWrite.recordsWritten)

    storeIfNonZero(INPUT_BYTES, input.bytesRead)
    storeIfNonZero(INPUT_RECORDS, input.recordsRead)
    storeIfNonZero(OUTPUT_BYTES, output.bytesWritten)
    storeIfNonZero(OUTPUT_RECORDS, output.recordsWritten)

    new TaskModel(
      stageId = event.stageId,
      stageAttemptId = event.stageAttemptId,
      taskType = event.taskType,
      endReason = reason,
      taskId = taskInfo.taskId,
      attempt = taskInfo.attemptNumber,
      successful = taskInfo.successful,
      executorId = taskInfo.executorId,
      host = taskInfo.host,
      taskLocality = taskInfo.taskLocality.toString,
      speculative = taskInfo.speculative,
      taskMetrics = taskMetrics
    )
  }
}
