/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.tool.{EventLogInfo, FailedEventLog, PlatformFactory, ToolBase}
import com.nvidia.spark.rapids.tool.tuning.{ClusterProperties, TargetClusterProps, TunerContext, TuningConfigsProvider}
import com.nvidia.spark.rapids.tool.views.QualRawReportGenerator
import com.nvidia.spark.rapids.tool.views.qualification.{QualPerAppReportGenerator, QualReportGenConfProvider, QualToolReportGenerator}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.rapids.tool.{AppBase, FailureApp}
import org.apache.spark.sql.rapids.tool.qualification._
import org.apache.spark.sql.rapids.tool.ui.ConsoleProgressBar
import org.apache.spark.sql.rapids.tool.util._

class Qualification(outputPath: String, hadoopConf: Configuration,
    timeout: Option[Long], nThreads: Int, pluginTypeChecker: PluginTypeChecker,
    enablePB: Boolean,
    reportSqlLevel: Boolean, maxSQLDescLength: Int, mlOpsEnabled: Boolean,
    penalizeTransitions: Boolean, tunerContext: Option[TunerContext],
    clusterReport: Boolean, platformArg: String, workerInfoPath: Option[String],
    targetClusterInfoPath: Option[String], tuningConfigsPath: Option[String])
  extends ToolBase(timeout) {

  override val simpleName: String = "qualTool"
  override val outputDir: String = QualReportGenConfProvider.getGlobalReportPath(outputPath)
  private val qualResultBuilder = QualToolResultBuilder()

  // Set the report configurations from the qualArgs
  private val qualAppReportOptions: Map[String, String] = Map(
    "clusterInfoJSONReport.report.enabled" -> clusterReport.toString,
    "mlFunctionsCSVReport.report.enabled" -> mlOpsEnabled.toString,
    "mlFunctionsDurationsCSVReport.report.enabled" -> mlOpsEnabled.toString,
    "perSqlCSVReport.report.enabled" -> reportSqlLevel.toString,
    "perSqlCSVReport.column.sql_description.max.length" -> maxSQLDescLength.toString
  )

  /**
   * Called once an app is successful. It creates a summary object and adds it to the
   * concurrent summary map. Then, it updates the global progress bar.
   * @param app the qualification app Info containing  reference to raw data
   * @param summaryRec The recorded created by the aggregator
   */
  def appendAppSummary(app: QualificationAppInfo, summaryRec: QualificationSummaryInfo): Unit = {
    // check if the app is already in the map
    val appExisted = qualResultBuilder.appendAppSummary(summaryRec)
    if (appExisted) {
      // fix the progress bar counts
      progressBar.foreach(_.adjustCounterForMultipleAttempts())
      logInfo(s"Removing older app summary for app: ${app.appId} " +
        s"before adding the new one with attempt: ${app.attemptId}")
    }
    progressBar.foreach(_.reportSuccessfulProcess())
  }

  override def getNumThreads: Int = nThreads

  private class QualifyThread(path: EventLogInfo) extends Runnable {
    def run(): Unit = qualifyApp(path, hadoopConf)
  }

  def qualifyApps(allPaths: Seq[EventLogInfo]): QualToolResult = {
    if (enablePB && allPaths.nonEmpty) { // total count to start the PB cannot be 0
      progressBar = Some(new ConsoleProgressBar("Qual Tool", allPaths.length))
    }
    // generate metadata
    // TODO: Consider moving this to the global QualToolReport generator
    generateRuntimeReport(Option(hadoopConf))

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
    val qualResults = qualResultBuilder.build(
      generateStatusResults(appStatusReporter.asScala.values.toSeq))
    generateQualificationReport(qualResults)
    qualResults
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
      // we need a platform per application because it's storing cluster information which could
      // vary between applications, especially when using dynamic allocation
      val platform = {
        val clusterPropsOpt = workerInfoPath.flatMap(
          PropertiesLoader[ClusterProperties].loadFromFile)
        val targetClusterPropsOpt = targetClusterInfoPath.flatMap(
          PropertiesLoader[TargetClusterProps].loadFromFile)
        PlatformFactory.createInstance(platformArg, clusterPropsOpt, targetClusterPropsOpt)
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
                outputDir, app)
            }.getOrElse(Seq.empty)
          val qualSumInfo = app.aggregateStats()
          AppSubscriber.withSafeValidAttempt(app.appId, app.attemptId) { () =>
            tunerContext.foreach { tuner =>
              // Load any user provided tuning configurations
              val userProvidedTuningConfigs = tuningConfigsPath.flatMap(
                PropertiesLoader[TuningConfigsProvider].loadFromFile)
              // Run the autotuner if it is enabled.
              // Note that we call the autotuner anyway without checking the aggregate results
              // because the Autotuner can still make some recommendations based on the information
              // enclosed by the QualificationInfo object
              tuner.tuneApplication(app, qualSumInfo, appIndex, dsInfo, platform,
                userProvidedTuningConfigs)
            }
          }
          if (qualSumInfo.isDefined) {
            // add the recommend cluster info into the summary
            val newQualSummary = qualSumInfo.get
            // If the recommended cluster info does not have a driver node type,
            // set it to the platform default.
            platform.setDefaultDriverNodeToRecommendedClusterIfMissing()
            val newClusterSummary = newQualSummary.clusterSummary.copy(
              recommendedClusterInfo = platform.recommendedClusterInfo)
            AppSubscriber.withSafeValidAttempt(app.appId, app.attemptId) { () =>
              newQualSummary.updateClusterSummary(newClusterInfo = newClusterSummary)
              appendAppSummary(app, newQualSummary)
              // generate report for the current QualApp.
              QualPerAppReportGenerator.build(
                newQualSummary, outputDir, Option(hadoopConf), qualAppReportOptions)
              AppBase.toSuccessAppResult(app)
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
   * Generates a qualification tool report based on the provided summary information.
   */
  private def generateQualificationReport(qualResult: QualToolResult): Unit = {
    QualToolReportGenerator.build(qualResult, outputPath,
      Option(hadoopConf), reportOptions = qualAppReportOptions)
  }
}
