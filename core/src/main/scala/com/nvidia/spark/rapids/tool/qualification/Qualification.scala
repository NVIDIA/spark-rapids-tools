/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.tool.{EventLogInfo, FailedEventLog, PlatformFactory, ToolBase}
import com.nvidia.spark.rapids.tool.profiling.AutoTuner
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter.DEFAULT_JOB_FREQUENCY
import com.nvidia.spark.rapids.tool.tuning.TunerContext
import com.nvidia.spark.rapids.tool.views.QualRawReportGenerator
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.rapids.tool.FailureApp
import org.apache.spark.sql.rapids.tool.qualification._
import org.apache.spark.sql.rapids.tool.ui.ConsoleProgressBar
import org.apache.spark.sql.rapids.tool.util._

class Qualification(outputPath: String, numRows: Int, hadoopConf: Configuration,
    timeout: Option[Long], nThreads: Int, order: String,
    pluginTypeChecker: PluginTypeChecker, reportReadSchema: Boolean,
    printStdout: Boolean, enablePB: Boolean,
    reportSqlLevel: Boolean, maxSQLDescLength: Int, mlOpsEnabled:Boolean,
    penalizeTransitions: Boolean, tunerContext: Option[TunerContext],
    clusterReport: Boolean, platformArg: String, workerInfoPath: String) extends ToolBase(timeout) {

  override val simpleName: String = "qualTool"
  override val outputDir = s"$outputPath/rapids_4_spark_qualification_output"
  private val allApps = new ConcurrentHashMap[String, QualificationSummaryInfo]()

  override def getNumThreads: Int = nThreads

  private class QualifyThread(path: EventLogInfo) extends Runnable {
    def run: Unit = qualifyApp(path, hadoopConf)
  }

  def qualifyApps(allPaths: Seq[EventLogInfo]): Seq[QualificationSummaryInfo] = {
    if (enablePB && allPaths.nonEmpty) { // total count to start the PB cannot be 0
      progressBar = Some(new ConsoleProgressBar("Qual Tool", allPaths.length))
    }
    // generate metadata
    generateRuntimeReport()

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
    val allAppsSum = estimateAppFrequency(allApps.asScala.values.toSeq)
    // sort order and limit only applies to the report summary text file,
    // the csv file we write the entire data in descending order
    val sortedDescDetailed = sortDescForDetailedReport(allAppsSum)
    generateQualificationReport(allAppsSum, sortedDescDetailed)
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
      // Early handling of failed event logs
      path match {
        case failedEventLog: FailedEventLog =>
          handleFailedEventLogs(failedEventLog)
          return
        case _ => // No action needed for other cases
      }
      val startTime = System.currentTimeMillis()
      // we need a platform per application because it's storing cluster information which could
      // vary between applications, especially when using dynamic allocation
      val platform = {
        val clusterPropsOpt = AutoTuner.loadClusterProps(workerInfoPath)
        PlatformFactory.createInstance(platformArg, clusterPropsOpt)
      }
      val appResult = QualificationAppInfo.createApp(path, hadoopConf, pluginTypeChecker,
        reportSqlLevel, mlOpsEnabled, penalizeTransitions, platform)
      val qualAppResult = appResult match {
        case Left(FailureApp("skipped", errorMessage)) =>
          // Case to be skipped, e.g. encountered Databricks Photon event log
          progressBar.foreach(_.reportSkippedProcess())
          SkippedAppResult(pathStr, errorMessage)
        case Left(FailureApp(_, errorMessage)) =>
          // Case when other error occurred during QualificationAppInfo creation
          progressBar.foreach(_.reportUnkownStatusProcess())
          UnknownAppResult(pathStr, "", errorMessage)
        case Right(app: QualificationAppInfo) =>
          // Case with successful creation of QualificationAppInfo
          // First, generate the Raw metrics view
          val appIndex = 1
          // this is a bit ugly right now to overload writing out the report and returning the
          // DataSource information but this encapsulates the analyzer to keep the memory usage
          // smaller.
          val dsInfo =
            AppSubscriber.withSafeValidAttempt(app.appId, app.attemptId) { () =>
              QualRawReportGenerator.generateRawMetricQualViewAndGetDataSourceInfo(
                outputDir, app, appIndex)
            }.getOrElse(Seq.empty)
          val qualSumInfo = app.aggregateStats()
          AppSubscriber.withSafeValidAttempt(app.appId, app.attemptId) { () =>
            tunerContext.foreach { tuner =>
              // Run the autotuner if it is enabled.
              // Note that we call the autotuner anyway without checking the aggregate results
              // because the Autotuner can still make some recommendations based on the information
              // enclosed by the QualificationInfo object
              tuner.tuneApplication(app, qualSumInfo, appIndex, dsInfo, platform)
            }
          }
          if (qualSumInfo.isDefined) {
            // add the recommend cluster info into the summary
            val tempSummary = qualSumInfo.get
            val newClusterSummary = tempSummary.clusterSummary.copy(
              recommendedClusterInfo = platform.recommendedClusterInfo)
            AppSubscriber.withSafeValidAttempt(app.appId, app.attemptId) { () =>
              val newQualSummary = tempSummary.copy(clusterSummary = newClusterSummary)
              // check if the app is already in the map
              if (allApps.containsKey(app.appId)) {
                // fix the progress bar counts
                progressBar.foreach(_.adjustCounterForMultipleAttempts())
                logInfo(s"Removing older app summary for app: ${app.appId} " +
                  s"before adding the new one with attempt: ${app.attemptId}")
              }
              progressBar.foreach(_.reportSuccessfulProcess())
              allApps.put(app.appId, newQualSummary)
              val endTime = System.currentTimeMillis()
              SuccessAppResult(pathStr, app.appId, app.attemptId,
                s"Took ${endTime - startTime}ms to process")
            } match {
              case Some(successfulResult) => successfulResult
              case _ =>
                // If the attemptId is an older attemptId, skip this attempt.
                // This can happen when the user has provided event logs for multiple attempts
                progressBar.foreach(_.reportSkippedProcess())
                SkippedAppResult.fromAppAttempt(pathStr, app.appId, app.attemptId)
            }
          } else {
            progressBar.foreach(_.reportUnkownStatusProcess())
            UnknownAppResult(pathStr, app.appId,
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
        logError(s"Error occurred while processing file: $pathStr", o)
        System.exit(1)
      case e: Exception =>
        progressBar.foreach(_.reportFailedProcess())
        val failureAppResult = FailureAppResult(pathStr,
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
   * Generates a qualification report based on the provided summary information.
   */
  private def generateQualificationReport(allAppsSum: Seq[QualificationSummaryInfo],
      sortedDescDetailed: Seq[QualificationSummaryInfo]): Unit = {
    val qWriter = new QualOutputWriter(outputDir, reportReadSchema, printStdout,
      order)

    qWriter.writeTextReport(allAppsSum,
      sortForExecutiveSummary(sortedDescDetailed, order), numRows)
    qWriter.writeDetailedCSVReport(sortedDescDetailed)
    if (reportSqlLevel) {
      qWriter.writePerSqlTextReport(allAppsSum, numRows, maxSQLDescLength)
      qWriter.writePerSqlCSVReport(allAppsSum, maxSQLDescLength)
    }
    qWriter.writeExecReport(allAppsSum, order)
    qWriter.writeStageReport(allAppsSum, order)
    qWriter.writeUnsupportedOpsSummaryCSVReport(allAppsSum)
    val appStatusResult = generateStatusResults(appStatusReporter.asScala.values.toSeq)
    qWriter.writeStatusReport(appStatusResult, order)
    if (mlOpsEnabled) {
      if (allAppsSum.exists(x => x.mlFunctions.nonEmpty)) {
        qWriter.writeMlFuncsReports(allAppsSum, order)
        qWriter.writeMlFuncsTotalDurationReports(allAppsSum)
      } else {
        logWarning(s"Eventlogs doesn't contain any ML functions")
      }
    }
    if (clusterReport) {
      qWriter.writeClusterReport(allAppsSum)
      qWriter.writeClusterReportCsv(allAppsSum)
    }
  }
}
