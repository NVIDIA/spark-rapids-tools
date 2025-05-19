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

package com.nvidia.spark.rapids.tool.profiling

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.{AppSummaryInfoBaseProvider, EventLogInfo, EventLogPathProcessor, FailedEventLog, PlatformFactory, ToolBase}
import com.nvidia.spark.rapids.tool.tuning.{AutoTuner, ProfilingAutoTunerConfigsProvider, TuningEntryTrait}
import com.nvidia.spark.rapids.tool.views._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.rapids.tool.{AppBase, FailureApp, IncorrectAppStatusException}
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.ui.ConsoleProgressBar
import org.apache.spark.sql.rapids.tool.util._

/**
 * Represents the complete profiling results for a Spark application.
 * Contains the raw application information, processed summary, and diagnostic data.
 *
 * @param app Raw application information including events, metrics, and configuration
 * @param summary Processed summary information including aggregated metrics,
 *                health checks, and properties
 * @param diagnostics Diagnostic information including stage diagnostics and I/O metrics
 */
case class ProfilerResult(
    app: ApplicationInfo,
    summary: ApplicationSummaryInfo,
    diagnostics: DiagnosticSummaryInfo)

class Profiler(hadoopConf: Configuration, appArgs: ProfileArgs, enablePB: Boolean)
  extends ToolBase(appArgs.timeout.toOption) {

  override val simpleName: String = "profileTool"
  override val outputDir: String = appArgs.outputDirectory().stripSuffix("/") +
    s"/${Profiler.SUBDIR}"
  private val numOutputRows = appArgs.numOutputRows.getOrElse(1000)
  private val outputCSV: Boolean = appArgs.csv()
  private val useAutoTuner: Boolean = appArgs.autoTuner()
  private val outputAlignedSQLIds: Boolean = appArgs.outputSqlIdsAligned()

  override def getNumThreads: Int = appArgs.numThreads.getOrElse(
    Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)

  /**
   * Profiles application by creating the ApplicationInfo by processing the event logs in parallel.
   */
  def profile(eventLogInfos: Seq[EventLogInfo]): Unit = {
    generateRuntimeReport()
    if (enablePB && eventLogInfos.nonEmpty) { // total count to start the PB cannot be 0
      progressBar = Some(new ConsoleProgressBar("Profile Tool", eventLogInfos.length))
    }
    // Read each application and process it separately to save memory.
    // Memory usage depends on the amount of threads running.
    eventLogInfos.foreach { log =>
      createAppAndProcess(log)
    }
    // wait for the threads to finish processing the files
    threadPool.shutdown()
    if (!threadPool.awaitTermination(waitTimeInSec, TimeUnit.SECONDS)) {
      logError(s"Processing log files took longer then $waitTimeInSec seconds," +
        " stopping processing any more event logs")
      threadPool.shutdownNow()
    }
    progressBar.foreach(_.finishAll())

    // Write status reports for all event logs to a CSV file
    logOutputPath()
    val reportResults = generateStatusResults(appStatusReporter.asScala.values.toSeq)
    ProfileOutputWriter.writeCSVTable("Profiling Status", reportResults, outputDir)
  }

  def profileDriver(
      driverLogInfos: String,
      hadoopConf: Option[Configuration],
      eventLogsEmpty: Boolean): Unit = {
    val profileOutputWriter = new ProfileOutputWriter(s"$outputDir/driver",
      Profiler.DRIVER_LOG_NAME, numOutputRows, true)
    try {
      val driverLogProcessor =
        BaseDriverLogInfoProvider(
          logPath = Option(driverLogInfos),
          hadoopConf = hadoopConf)
      val unsupportedDriverOperators = driverLogProcessor.getUnsupportedOperators
      profileOutputWriter.writeTable(s"Unsupported operators in driver log",
        unsupportedDriverOperators)
      if (eventLogsEmpty && useAutoTuner) {
        // Since event logs are empty, AutoTuner will not run while processing event logs.
        // We need to run it here explicitly.
        val (properties, comments) = runAutoTuner(None, driverLogProcessor)
        profileOutputWriter.writeText("\n### A. Recommended Configuration ###\n")
        profileOutputWriter.writeText(Profiler.getAutoTunerResultsAsString(properties, comments))
      }
    } finally {
      profileOutputWriter.close()
    }
  }

  /**
   * Process a created ApplicationInfo object and handles all exceptions.
   */
  private def profileApp(
      path: EventLogInfo,
      processSuccessApp: ApplicationInfo => Unit): Unit = {
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
      val appOpt = createApp(path, hadoopConf)
      val profAppResult = appOpt match {
        case Left(FailureApp("skipped", errorMessage)) =>
          // Case to be skipped
          progressBar.foreach(_.reportSkippedProcess())
          SkippedAppResult(pathStr, errorMessage)
        case Left(FailureApp(_, errorMessage)) =>
          // Case when other error occurred during ApplicationInfo creation
          progressBar.foreach(_.reportUnkownStatusProcess())
          UnknownAppResult(pathStr, "", errorMessage)
        case Right(app: ApplicationInfo) =>
          // Case with successful creation of ApplicationInfo
          processSuccessApp(app)
          progressBar.foreach(_.reportSuccessfulProcess())
          val endTime = System.currentTimeMillis()
          SuccessAppResult(pathStr, app.appId, app.attemptId,
            s"Took ${endTime - startTime}ms to process")
      }
      // Log the information to the console
      profAppResult.logMessage()
      // Update the appStatusReporter with the result of Application processing
      appStatusReporter.put(pathStr, profAppResult)
    } catch {
      case NonFatal(e) =>
        progressBar.foreach(_.reportFailedProcess())
        val failureAppResult = FailureAppResult(pathStr,
          s"Unexpected exception processing log, skipping!")
        failureAppResult.logMessage(Some(new Exception(e.getMessage, e)))
        appStatusReporter.put(pathStr, failureAppResult)
      case oom: OutOfMemoryError =>
        logError(s"OOM error while processing large file: $pathStr." +
            s"Increase heap size. Exiting.", oom)
        System.exit(1)
      case o: Throwable =>
        logError(s"Error occurred while processing file: $pathStr", o)
        System.exit(1)
    }
  }

  private def createAppAndProcess(
      evLogPath: EventLogInfo): Unit = {
    class ProfileProcessThread(path: EventLogInfo) extends Runnable {
      def run(): Unit = profileApp(path, { app =>
        val profileOutputWriter = new ProfileOutputWriter(s"$outputDir/${app.appId}",
          Profiler.PROFILE_LOG_NAME, numOutputRows, outputCSV = outputCSV)
        try {
          val (sum, diagnostics) =
            processApp(Seq(app), appArgs.printPlans(), profileOutputWriter)
          writeSafelyToOutput(profileOutputWriter, ProfilerResult(app, sum, diagnostics))
        } finally {
          profileOutputWriter.close()
        }
      })
    }
    try {
      threadPool.submit(new ProfileProcessThread(evLogPath))
    } catch {
      case e: Exception =>
        logError(s"Unexpected exception submitting log ${evLogPath.eventLog.toString}, skipping!",
          e)
    }
  }

  private def createApp(path: EventLogInfo,
      hadoopConf: Configuration): Either[FailureApp, ApplicationInfo] = {
    try {
      // These apps only contains 1 app in each loop.
      val startTime = System.currentTimeMillis()
      // we need a platform per application because it is storing cluster information, which could
      // vary between applications, especially when using dynamic allocation.
      val platform = {
        val workerInfoPath = appArgs.workerInfo
          .getOrElse(ProfilingAutoTunerConfigsProvider.DEFAULT_WORKER_INFO_PATH)
        val clusterPropsOpt = ProfilingAutoTunerConfigsProvider.loadClusterProps(workerInfoPath)
        PlatformFactory.createInstance(appArgs.platform(), clusterPropsOpt)
      }
      val app = new ApplicationInfo(hadoopConf, path, platform = platform)
      EventLogPathProcessor.logApplicationInfo(app)
      val endTime = System.currentTimeMillis()
      if (!app.isAppMetaDefined) {
        throw IncorrectAppStatusException()
      }
      logInfo(s"Took ${endTime - startTime}ms to create App for ${path.eventLog.toString}")
      Right(app)
    } catch {
      case e: Exception =>
        Left(AppBase.handleException(e, path))
    }
  }

  /**
   * Function to process ApplicationInfo. Collects all the application information
   * and returns the summary information. The summary information is much smaller than
   * the ApplicationInfo because it has processed and combined many of the raw events.
   */
  private def processApp(
      analyzedApps: Seq[ApplicationInfo],
      printPlans: Boolean,
      profileOutputWriter: ProfileOutputWriter): (ApplicationSummaryInfo, DiagnosticSummaryInfo) = {
    val startTime = System.currentTimeMillis()
    val collect = new CollectInformation(analyzedApps)

    val healthCheck = new HealthCheck(analyzedApps)

    if (printPlans) {
      CollectInformation.printSQLPlans(analyzedApps, outputDir)
    }

    if (appArgs.generateDot()) {
      analyzedApps.foreach { app =>
        val start = System.nanoTime()
        GenerateDot(app, s"$outputDir/${app.appId}")
        val duration = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        profileOutputWriter.writeText(s"Generated DOT graphs for app ${app.appId} " +
          s"to $outputDir in $duration second(s)\n")
      }
    }

    if (appArgs.generateTimeline()) {
      analyzedApps.foreach { app =>
        val start = System.nanoTime()
        GenerateTimeline.generateFor(app, s"$outputDir/${app.appId}")
        val duration = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        profileOutputWriter.writeText(s"Generated timeline graphs for app ${app.appId} " +
          s"to $outputDir in $duration second(s)\n")
      }
    }
    val analysis = RawMetricProfilerView.getAggMetrics(analyzedApps)
    val maxTaskInputInfo = if (useAutoTuner) {
      analysis.maxTaskInputSizes
    } else {
      Seq.empty
    }
    val sqlIdAlign = if (outputAlignedSQLIds) {
      collect.getSQLCleanAndAligned
    } else {
      Seq.empty
    }
    val endTime = System.currentTimeMillis()
    val appInfo = collect.getAppInfo
    val sqlMetrics = collect.getSQLPlanMetrics
    logInfo(s"Time to collect Profiling Info [${appInfo.head.appId}]: ${endTime - startTime}.")
    val appInfoSummary = ApplicationSummaryInfo(
      appInfo = appInfo,
      dsInfo = collect.getDataSourceInfo(sqlMetrics),
      execInfo = collect.getExecutorInfo,
      jobInfo = collect.getJobInfo,
      rapidsProps = collect.getRapidsProperties,
      rapidsJar = collect.getRapidsJARInfo,
      sqlMetrics = sqlMetrics,
      stageMetrics = collect.getStageLevelMetrics,
      jobAggMetrics = analysis.jobAggs,
      stageAggMetrics = analysis.stageAggs,
      sqlTaskAggMetrics = analysis.sqlAggs,
      durAndCpuMet = analysis.sqlDurAggs,
      skewInfo = analysis.taskShuffleSkew,
      failedTasks = healthCheck.getFailedTasks,
      failedStages = healthCheck.getFailedStages,
      failedJobs = healthCheck.getFailedJobs,
      removedBMs = healthCheck.getRemovedBlockManager,
      removedExecutors = healthCheck.getRemovedExecutors,
      unsupportedOps = healthCheck.getPossibleUnsupportedSQLPlan,
      sparkProps = collect.getSparkProperties,
      sqlStageInfo = collect.getSQLToStage,
      wholeStage = collect.getWholeStageCodeGenMapping,
      maxTaskInputBytesRead = maxTaskInputInfo,
      appLogPath = collect.getAppLogPath,
      ioMetrics = analysis.ioAggs,
      sysProps = collect.getSystemProperties,
      sqlCleanedAlignedIds = sqlIdAlign,
      sparkRapidsBuildInfo = collect.getSparkRapidsInfo,
      writeOpsInfo = collect.getWriteOperationInfo,
      sqlPlanInfo = collect.getSQLPlanInfoTruncated)
    (appInfoSummary,
      DiagnosticSummaryInfo(analysis.stageDiagnostics, collect.getIODiagnosticMetrics))
  }

  /**
   * A wrapper method to run the AutoTuner.
   * @param profilerResult     ProfilerResult object storing the app info, summary and diagnostics
   * @param driverInfoProvider Entity that implements APIs needed to extract information from the
   *                           driver log if any
   */
  private def runAutoTuner(
      profilerResult: Option[ProfilerResult],
      driverInfoProvider: DriverLogInfoProvider = BaseDriverLogInfoProvider.noneDriverLog)
  : (Seq[TuningEntryTrait], Seq[RecommendedCommentResult]) = {
    // only run the auto tuner on GPU event logs for profiling tool right now. There are
    // assumptions made in the code
    val appInfoFromSummary = profilerResult.flatMap(_.summary.appInfo.headOption)
    if (appInfoFromSummary.isDefined && appInfoFromSummary.get.pluginEnabled) {
      val appInfoProvider = AppSummaryInfoBaseProvider.fromAppInfo(profilerResult)
      val platform = profilerResult.get.app.platform.getOrElse {
        throw new IllegalStateException("Profiling AutoTuner requires a platform. " +
          "Please provide a valid platform using --platform option.")
      }
      val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider.buildAutoTuner(appInfoProvider,
        platform, driverInfoProvider)

      // The autotuner allows skipping some properties,
      // e.g., getRecommendedProperties(Some(Seq("spark.executor.instances"))) skips the
      // recommendation related to executor instances.
      autoTuner.getRecommendedProperties()
    } else {
      logWarning("The Profiling tool AutoTuner is only available for GPU event logs, " +
        "skipping recommendations!")
      (Seq.empty, Seq.empty)
    }
  }

  private def writeOutput(
      profileOutputWriter: ProfileOutputWriter,
      profilerResult: ProfilerResult): Unit = {

    val app = profilerResult.summary
    profileOutputWriter.writeText("### A. Information Collected ###")
    profileOutputWriter.writeTable(ProfInformationView.getLabel, app.appInfo)
    profileOutputWriter.writeTable(ProfLogPathView.getLabel, app.appLogPath)
    profileOutputWriter.writeTable(ProfDataSourceView.getLabel, app.dsInfo)
    profileOutputWriter.writeTable(ProfExecutorView.getLabel, app.execInfo)
    profileOutputWriter.writeTable(ProfJobsView.getLabel, app.jobInfo)
    profileOutputWriter.writeTable(ProfSQLToStageView.getLabel, app.sqlStageInfo)
    profileOutputWriter.writeTable(RapidsQualPropertiesView.getLabel, app.rapidsProps,
      Some(RapidsQualPropertiesView.getDescription))
    profileOutputWriter.writeTable(SparkQualPropertiesView.getLabel, app.sparkProps,
      Some(SparkQualPropertiesView.getDescription))
    profileOutputWriter.writeTable(SystemQualPropertiesView.getLabel, app.sysProps,
      Some(SystemQualPropertiesView.getDescription))
    profileOutputWriter.writeTable(ProfRapidsJarView.getLabel, app.rapidsJar,
      Some(ProfRapidsJarView.getDescription))
    profileOutputWriter.writeTable(ProfSQLPlanMetricsView.getLabel, app.sqlMetrics,
      Some(ProfSQLPlanMetricsView.getDescription))
    profileOutputWriter.writeTable(ProfStageMetricView.getLabel, app.stageMetrics,
      Some(ProfStageMetricView.getDescription))
    profileOutputWriter.writeTable(ProfSQLCodeGenView.getLabel, app.wholeStage,
      Some(ProfSQLCodeGenView.getDescription))
    profileOutputWriter.writeJson(ProfAppSQLPlanInfoView.getLabel, app.sqlPlanInfo, pretty = false)

    profileOutputWriter.writeText("\n### B. Analysis ###\n")
    profileOutputWriter.writeTable(JOB_AGG_LABEL, app.jobAggMetrics,
      Some(AGG_DESCRIPTION(JOB_AGG_LABEL)))
    profileOutputWriter.writeTable(STAGE_AGG_LABEL, app.stageAggMetrics,
      Some(AGG_DESCRIPTION(STAGE_AGG_LABEL)))
    profileOutputWriter.writeTable(SQL_AGG_LABEL, app.sqlTaskAggMetrics,
      Some(AGG_DESCRIPTION(SQL_AGG_LABEL)))
    profileOutputWriter.writeTable(IO_LABEL, app.ioMetrics)
    profileOutputWriter.writeTable(SQL_DUR_LABEL, app.durAndCpuMet)
    // writeOps are generated in only CSV format
    profileOutputWriter.writeCSVTable(ProfWriteOpsView.getLabel, app.writeOpsInfo)
    val skewHeader = TASK_SHUFFLE_SKEW
    val skewTableDesc = AGG_DESCRIPTION(TASK_SHUFFLE_SKEW)
    profileOutputWriter.writeTable(skewHeader, app.skewInfo, tableDesc = Some(skewTableDesc))

    profileOutputWriter.writeText("\n### C. Health Check###\n")
    profileOutputWriter.writeTable(ProfFailedTaskView.getLabel, app.failedTasks)
    profileOutputWriter.writeTable(ProfFailedStageView.getLabel, app.failedStages)
    profileOutputWriter.writeTable(ProfFailedJobsView.getLabel, app.failedJobs)
    profileOutputWriter.writeTable(ProfRemovedBLKMgrView.getLabel, app.removedBMs)
    profileOutputWriter.writeTable(ProfRemovedExecutorView.getLabel, app.removedExecutors)
    profileOutputWriter.writeTable("Unsupported SQL Plan", app.unsupportedOps,
      Some("Unsupported SQL Ops"))
    if (outputAlignedSQLIds) {
      profileOutputWriter.writeTable(
        ProfSQLPlanAlignedView.getLabel, app.sqlCleanedAlignedIds,
        Some(ProfSQLPlanAlignedView.getDescription))
    }
    if (useAutoTuner) {
      val (properties, comments) = runAutoTuner(Some(profilerResult))
      profileOutputWriter.writeText("\n### D. Recommended Configuration ###\n")
      profileOutputWriter.writeText(Profiler.getAutoTunerResultsAsString(properties, comments))
    }

    profileOutputWriter.writeJson("Spark Rapids Build Info",
      app.sparkRapidsBuildInfo, pretty = true)
    profileOutputWriter.writeCSVTable(STAGE_DIAGNOSTICS_LABEL,
      profilerResult.diagnostics.stageDiagnostics)
    profileOutputWriter.writeCSVTable(ProfIODiagnosticMetricsView.getLabel,
      profilerResult.diagnostics.IODiagnostics)

  }

  /**
   * Safely writes the application summary information to the specified profileOutputWriter.
   * If an exception occurs during the writing process, it will be caught and logged, preventing
   * it from propagating further.
   */
  private def writeSafelyToOutput(
      profileOutputWriter: ProfileOutputWriter,
      profilerResults: ProfilerResult): Unit = {
    try {
      writeOutput(profileOutputWriter, profilerResults)
    } catch {
      case e: Exception =>
        logError("Exception thrown while writing", e)
    }
  }
}

object Profiler {
  // This tool's output log file name
  private val DRIVER_LOG_NAME = "driver"
  val PROFILE_LOG_NAME = "profile"
  val SUBDIR = "rapids_4_spark_profile"

  def getAutoTunerResultsAsString(props: Seq[TuningEntryTrait],
      comments: Seq[RecommendedCommentResult]): String = {
    val propStr = if (props.nonEmpty) {
        val propertiesToStr = props.map(_.toConfString).reduce(_ + "\n" + _)
        s"\nSpark Properties:\n$propertiesToStr\n"
      } else {
        "Cannot recommend properties. See Comments.\n"
      }
    if (comments.isEmpty) { // Comments are optional
      propStr
    } else {
      val commentsToStr = comments.map(_.toString).reduce(_ + "\n" + _)
      propStr + s"\nComments:\n$commentsToStr\n"
    }
  }
}
