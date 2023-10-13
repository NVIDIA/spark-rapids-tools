/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.qualification

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.ThreadFactoryBuilder
import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter.DEFAULT_JOB_FREQUENCY
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification._
import org.apache.spark.sql.rapids.tool.ui.{ConsoleProgressBar, QualificationReportGenerator}
import org.apache.spark.sql.rapids.tool.util._

class Qualification(outputDir: String, numRows: Int, hadoopConf: Configuration,
    timeout: Option[Long], nThreads: Int, order: String,
    pluginTypeChecker: PluginTypeChecker, reportReadSchema: Boolean,
    printStdout: Boolean, uiEnabled: Boolean, enablePB: Boolean,
    reportSqlLevel: Boolean, maxSQLDescLength: Int, mlOpsEnabled:Boolean,
    penalizeTransitions: Boolean) extends Logging {

  private val allApps = new ConcurrentLinkedQueue[QualificationSummaryInfo]()

  // default is 24 hours
  private val waitTimeInSec = timeout.getOrElse(60 * 60 * 24L)

  private val threadFactory = new ThreadFactoryBuilder()
    .setDaemon(true).setNameFormat("qualTool" + "-%d").build()
  logInfo(s"Threadpool size is $nThreads")
  private val threadPool = Executors.newFixedThreadPool(nThreads, threadFactory)
    .asInstanceOf[ThreadPoolExecutor]

  private var progressBar: Option[ConsoleProgressBar] = None
  // Store application status reports indexed by event log path.
  private val appStatusReporter = new ConcurrentHashMap[String, QualAppResult]

  private class QualifyThread(path: EventLogInfo) extends Runnable {
    def run: Unit = qualifyApp(path, hadoopConf)
  }

  def qualifyApps(allPaths: Seq[EventLogInfo]): Seq[QualificationSummaryInfo] = {
    if (enablePB && allPaths.nonEmpty) { // total count to start the PB cannot be 0
      progressBar = Some(new ConsoleProgressBar("Qual Tool", allPaths.length))
    }
    allPaths.foreach { path =>
      try {
        threadPool.submit(new QualifyThread(path))
      } catch {
        case e: Exception =>
          logError(s"Unexpected exception submitting log ${path.eventLog.toString}, skipping!", e)
      }
    }
    // wait for the threads to finish processing the files
    threadPool.shutdown()
    if (!threadPool.awaitTermination(waitTimeInSec, TimeUnit.SECONDS)) {
      logError(s"Processing log files took longer then $waitTimeInSec seconds," +
        " stopping processing any more event logs")
      threadPool.shutdownNow()
    }
    progressBar.foreach(_.finishAll())
    val allAppsSum = estimateAppFrequency(allApps.asScala.toSeq)

    val qWriter = new QualOutputWriter(getReportOutputPath, reportReadSchema, printStdout,
      order)
    // sort order and limit only applies to the report summary text file,
    // the csv file we write the entire data in descending order
    val sortedDescDetailed = sortDescForDetailedReport(allAppsSum)
    qWriter.writeTextReport(allAppsSum,
      sortForExecutiveSummary(sortedDescDetailed, order), numRows)
    qWriter.writeDetailedCSVReport(sortedDescDetailed)
    if (reportSqlLevel) {
      qWriter.writePerSqlTextReport(allAppsSum, numRows, maxSQLDescLength)
      qWriter.writePerSqlCSVReport(allAppsSum, maxSQLDescLength)
    }
    qWriter.writeExecReport(allAppsSum, order)
    qWriter.writeStageReport(allAppsSum, order)
    qWriter.writeUnsupportedOperatorsCSVReport(allAppsSum, order)
    val appStatusResult = generateStatusSummary(appStatusReporter.asScala.values.toSeq)
    qWriter.writeStatusReport(appStatusResult, order)
    if (mlOpsEnabled) {
      if (allAppsSum.exists(x => x.mlFunctions.nonEmpty)) {
        qWriter.writeMlFuncsReports(allAppsSum, order)
        qWriter.writeMlFuncsTotalDurationReports(allAppsSum)
      } else {
        logWarning(s"Eventlogs doesn't contain any ML functions")
      }
    }
    if (uiEnabled) {
      QualificationReportGenerator.generateDashBoard(getReportOutputPath, allAppsSum)
    }
    sortedDescDetailed
  }

  private def sortDescForDetailedReport(
      allAppsSum: Seq[QualificationSummaryInfo]): Seq[QualificationSummaryInfo] = {
    // Default sorting for of the csv files. Use the endTime to break the tie.
    allAppsSum.sortBy(sum => {
      (sum.estimatedInfo.recommendation, sum.estimatedInfo.estimatedGpuSpeedup,
        sum.estimatedInfo.estimatedGpuTimeSaved, sum.startTime + sum.estimatedInfo.appDur)
    }).reverse
  }

  // Sorting for the pretty printed executive summary.
  // The sums elements is ordered in descending order. so, only we need to reverse it if the order
  // is ascending
  private def sortForExecutiveSummary(appsSumDesc: Seq[QualificationSummaryInfo],
      order: String): Seq[EstimatedSummaryInfo] = {
    if (QualificationArgs.isOrderAsc(order)) {
      appsSumDesc.reverse.map(sum =>
        EstimatedSummaryInfo(
          sum.estimatedInfo, sum.estimatedFrequency.getOrElse(DEFAULT_JOB_FREQUENCY)))
    } else {
      appsSumDesc.map(sum => EstimatedSummaryInfo(
        sum.estimatedInfo, sum.estimatedFrequency.getOrElse(DEFAULT_JOB_FREQUENCY)))
    }
  }

  // Estimate app frequency based off of all applications in this run, unit jobs per month
  private def estimateAppFrequency(
    appsSum: Seq[QualificationSummaryInfo]): Seq[QualificationSummaryInfo] = {
    val appFrequency = scala.collection.mutable.Map[String, Double]()
    var windowStart: Long = Long.MaxValue
    var windowEnd: Long = Long.MinValue

    appsSum.foreach { sum =>
      appFrequency += (sum.appName -> (1.0 + appFrequency.getOrElse(sum.appName, 0.0)))
      windowStart = Math.min(sum.startTime, windowStart)
      windowEnd = Math.max(windowEnd, sum.startTime + sum.estimatedInfo.appDur)
    }
    val windowInMonths =
      if (windowEnd > windowStart) ((windowEnd - windowStart) / (1000.0*60*60*24*30)) else 1.0
    // Scale frequency to per month assuming uniform distribution over the logging window rather
    // than the individual applications window. Single run jobs are given a default frequency
    val monthlyFrequency = appFrequency.map { case (appName, numApps) => (appName ->
      (if (numApps <= 1) DEFAULT_JOB_FREQUENCY else (numApps / windowInMonths).round))
    }
    appsSum.map { app =>
      val frequency = monthlyFrequency.getOrElse(app.appName, DEFAULT_JOB_FREQUENCY)
      // Ensure jobs have a valid frequency, rounding up to 1 (monthly)
      app.copy(estimatedFrequency =
        Option(if (frequency <= 0) 1 else frequency))
    }
  }

  private def qualifyApp(
      path: EventLogInfo,
      hadoopConf: Configuration): Unit = {
    val pathStr = path.eventLog.toString
    try {
      val startTime = System.currentTimeMillis()
      val appResult = QualificationAppInfo.createApp(path, hadoopConf, pluginTypeChecker,
        reportSqlLevel, mlOpsEnabled, penalizeTransitions)
      val qualAppResult = appResult match {
        case Left(errorMessage: String) =>
          // Case when an error occurred during QualificationAppInfo creation
          progressBar.foreach(_.reportUnkownStatusProcess())
          UnknownQualAppResult(pathStr, "", errorMessage)
        case Right(app: QualificationAppInfo) =>
          // Case with successful creation of QualificationAppInfo
          val qualSumInfo = app.aggregateStats()
          if (qualSumInfo.isDefined) {
            allApps.add(qualSumInfo.get)
            progressBar.foreach(_.reportSuccessfulProcess())
            val endTime = System.currentTimeMillis()
            SuccessQualAppResult(pathStr, app.appId,
              s"Took ${endTime - startTime}ms to process")
          } else {
            progressBar.foreach(_.reportUnkownStatusProcess())
            UnknownQualAppResult(pathStr, app.appId,
              "No aggregated stats for event log")
          }
      }
      // Log the information to the console
      qualAppResult.logMessage()
      // Update the appStatusReporter with the result of QualificationAppInfo processing
      appStatusReporter.put(pathStr, qualAppResult)
    } catch {
      case oom: OutOfMemoryError =>
        logError(s"OOM error while processing large file: $pathStr." +
            s"Increase heap size.", oom)
        System.exit(1)
      case o: Error =>
        logError(s"Error occured while processing file: $pathStr", o)
        System.exit(1)
      case e: Exception =>
        progressBar.foreach(_.reportFailedProcess())
        val failureAppResult = FailureQualAppResult(pathStr,
          s"Unexpected exception processing log, skipping!")
        failureAppResult.logMessage(Some(e))
        appStatusReporter.put(pathStr, failureAppResult)
    }
  }

  /**
   * The outputPath of the current instance of the provider
   */
  def getReportOutputPath: String = {
    s"$outputDir/rapids_4_spark_qualification_output"
  }

  /**
   * For each app status report, generate a summary containing appId and message (if any).
   * @return Seq[Summary] - Seq[(path, status, [appId], [message])]
   */
  private def generateStatusSummary(appStatuses: Seq[QualAppResult]): Seq[StatusSummaryInfo] = {
    appStatuses.map {
      case SuccessQualAppResult(path, appId, message) =>
        StatusSummaryInfo(path, "SUCCESS", appId, message)
      case FailureQualAppResult(path, message) =>
        StatusSummaryInfo(path, "FAILURE", "", message)
      case UnknownQualAppResult(path, appId, message) =>
        StatusSummaryInfo(path, "UNKNOWN", appId, message)
      case qualAppResult: QualAppResult =>
        throw new UnsupportedOperationException(s"Invalid status for $qualAppResult")
    }
  }
}
