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

package com.nvidia.spark.rapids.tool

import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadFactory, ThreadPoolExecutor}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.sql.rapids.tool.ui.ConsoleProgressBar
import org.apache.spark.sql.rapids.tool.util.{AppResult, FailureAppResult, RuntimeReporter}

/**
 * Base class for Profiling and Qualification tools.
 */
abstract class ToolBase(timeout: Option[Long]) extends RuntimeReporter {

  protected val simpleName: String
  protected val waitTimeInSec: Long = timeout.getOrElse(60 * 60 * 24L)
  protected val threadFactory: ThreadFactory = new ThreadFactoryBuilder()
    .setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build()
  protected val threadPool: ThreadPoolExecutor =
    Executors.newFixedThreadPool(getNumThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  protected var progressBar: Option[ConsoleProgressBar] = None
  // Store application status reports indexed by event log path.
  protected val appStatusReporter: ConcurrentHashMap[String, AppResult] =
    new ConcurrentHashMap[String, AppResult]

  def getNumThreads: Int

  logInfo(s"Threadpool size is $getNumThreads")

  /**
   * Handles a failed event log by logging the failure, adding it to appStatusReporter,
   * and updating the progress bar.
   *
   * @param failedEventLog The event log that failed to process.
   */
  final def handleFailedEventLogs(failedEventLog: FailedEventLog): Unit = {
    val pathStr = failedEventLog.eventLog.toString
    val failureAppResult = FailureAppResult(pathStr, failedEventLog.getReason)
    failureAppResult.logMessage()
    appStatusReporter.put(pathStr, failureAppResult)
    progressBar.foreach(_.reportFailedProcess())
  }
}
