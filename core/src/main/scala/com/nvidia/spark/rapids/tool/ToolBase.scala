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
