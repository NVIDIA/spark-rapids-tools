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

package com.nvidia.spark.rapids.tool.profiling

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.ThreadFactoryBuilder
import com.nvidia.spark.rapids.tool.{AppSummaryInfoBaseProvider, EventLogInfo, EventLogPathProcessor, PlatformFactory}
import com.nvidia.spark.rapids.tool.profiling.AutoTuner.loadClusterProps
import com.nvidia.spark.rapids.tool.views._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.rapids.tool.{AppBase, FailureApp, IncorrectAppStatusException}
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.ui.ConsoleProgressBar
import org.apache.spark.sql.rapids.tool.util._

class Profiler(hadoopConf: Configuration, appArgs: ProfileArgs, enablePB: Boolean)
  extends RuntimeReporter {

  private val nThreads = appArgs.numThreads.getOrElse(
    Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)
  private val timeout = appArgs.timeout.toOption
  private val waitTimeInSec = timeout.getOrElse(60 * 60 * 24L)

  private val threadFactory = new ThreadFactoryBuilder()
    .setDaemon(true).setNameFormat("profileTool" + "-%d").build()
  private val threadPool = Executors.newFixedThreadPool(nThreads, threadFactory)
    .asInstanceOf[ThreadPoolExecutor]
  private val numOutputRows = appArgs.numOutputRows.getOrElse(1000)

  private val outputCSV: Boolean = appArgs.csv()
  private val outputCombined: Boolean = appArgs.combined()

  private val useAutoTuner: Boolean = appArgs.autoTuner()
  private var progressBar: Option[ConsoleProgressBar] = None
  // Store application status reports indexed by event log path.
  private val appStatusReporter = new ConcurrentHashMap[String, AppResult]

  private val outputAlignedSQLIds: Boolean = appArgs.outputSqlIdsAligned()

  override val outputDir = appArgs.outputDirectory().stripSuffix("/") +
    s"/${Profiler.SUBDIR}"

  logInfo(s"Threadpool size is $nThreads")

  /**
   * Profiles application according to the mode requested. The main difference in processing for
   * the modes is which parts are done in parallel. All of them create the ApplicationInfo
   * by processing the event logs in parallel but after that it depends on the mode as to
   * what else we can do in parallel.
   */
  def profile(eventLogInfos: Seq[EventLogInfo]): Unit = {
    generateRuntimeReport()
    if (enablePB && eventLogInfos.nonEmpty) { // total count to start the PB cannot be 0
      progressBar = Some(new ConsoleProgressBar("Profile Tool", eventLogInfos.length))
    }
    if (appArgs.compare()) {
      if (outputCombined) {
        logError("Output combined option not valid with compare mode!")
      } else {
        if (useAutoTuner) {
          logError("Autotuner is currently not supported with compare mode!")
        } else {
          // create all the apps in parallel
          val apps = createApps(eventLogInfos)

          if (apps.size < 2) {
            progressBar.foreach(_.reportUnknownStatusProcesses(apps.size))
            logError("At least 2 applications are required for comparison mode. Exiting!")
          } else {
            val profileOutputWriter = new ProfileOutputWriter(s"$outputDir/compare",
              Profiler.COMPARE_LOG_FILE_NAME_PREFIX, numOutputRows, outputCSV = outputCSV)
            try {
              // we need the info for all of the apps to be able to compare so this happens serially
              val (sums, comparedRes) = processApps(apps, printPlans = false, profileOutputWriter)
              progressBar.foreach(_.reportSuccessfulProcesses(apps.size))
              writeSafelyToOutput(profileOutputWriter, Seq(sums), false, comparedRes)
            } catch {
              case _: Exception =>
                progressBar.foreach(_.reportFailedProcesses(apps.size))
            } finally {
              profileOutputWriter.close()
            }
          }
        }
      }
    } else if (outputCombined) {
      if (useAutoTuner) {
        logError("Autotuner is currently not supported with combined mode!")
      } else {
        // same as collection but combine the output so all apps are in single tables
        // We can process all the apps in parallel and get the summary for them and then
        // combine them into single tables in the output.
        val profileOutputWriter = new ProfileOutputWriter(s"$outputDir/combined",
          Profiler.COMBINED_LOG_FILE_NAME_PREFIX, numOutputRows, outputCSV = outputCSV)
        val sums = createAppsAndSummarize(eventLogInfos, profileOutputWriter)
        writeSafelyToOutput(profileOutputWriter, sums, outputCombined)
        profileOutputWriter.close()
      }
    } else {
      // Read each application and process it separately to save memory.
      // Memory usage will be controlled by number of threads running.
      // use appIndex as 1 for all since we output separate file for each now
      eventLogInfos.foreach { log =>
        createAppAndProcess(Seq(log), 1)
      }
      // wait for the threads to finish processing the files
      threadPool.shutdown()
      if (!threadPool.awaitTermination(waitTimeInSec, TimeUnit.SECONDS)) {
        logError(s"Processing log files took longer then $waitTimeInSec seconds," +
          " stopping processing any more event logs")
        threadPool.shutdownNow()
      }
    }
    progressBar.foreach(_.finishAll())
    
    // Write status reports for all event logs to a CSV file
    val reportResults = generateStatusResults(appStatusReporter.asScala.values.toSeq)
    ProfileOutputWriter.writeCSVTable("Profiling Status", reportResults, outputDir)
  }

  def profileDriver(driverLogInfos: String, eventLogsEmpty: Boolean): Unit = {
    val profileOutputWriter = new ProfileOutputWriter(s"$outputDir/driver",
      Profiler.DRIVER_LOG_NAME, numOutputRows, true)
    try {
      val driverLogProcessor = new DriverLogProcessor(driverLogInfos)
      val unsupportedDriverOperators = driverLogProcessor.getUnsupportedOperators
      profileOutputWriter.write(s"Unsupported operators in driver log",
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
      index: Int,
      processSuccessApp: ApplicationInfo => Unit): Unit = {
    val pathStr = path.eventLog.toString
    try {
      val startTime = System.currentTimeMillis()
      val appOpt = createApp(path, index, hadoopConf)
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
          SuccessAppResult(pathStr, app.appId, s"Took ${endTime - startTime}ms to process")
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
        failureAppResult.logMessage(Some(new Exception(e.getMessage(), e)))
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

  private def createApps(allPaths: Seq[EventLogInfo]): Seq[ApplicationInfo] = {
    val allApps = new ConcurrentLinkedQueue[ApplicationInfo]()

    class ProfileThread(path: EventLogInfo, index: Int) extends Runnable {
      def run: Unit = profileApp(path, index, (app => allApps.add(app)))
    }

    var appIndex = 1
    allPaths.foreach { path =>
      try {
        threadPool.submit(new ProfileThread(path, appIndex))
        appIndex += 1
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
    allApps.asScala.toSeq
  }

  private def createAppsAndSummarize(allPaths: Seq[EventLogInfo],
      profileOutputWriter: ProfileOutputWriter): Seq[ApplicationSummaryInfo] = {
    val allApps = new ConcurrentLinkedQueue[ApplicationSummaryInfo]()

    class ProfileThread(path: EventLogInfo, index: Int) extends Runnable {
      def run: Unit = profileApp(path, index, { app =>
        val (s, _) = processApps(Seq(app), false, profileOutputWriter)
        allApps.add(s)
      })
    }

    var appIndex = 1
    allPaths.foreach { path =>
      try {
        threadPool.submit(new ProfileThread(path, appIndex))
        appIndex += 1
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
    allApps.asScala.toSeq
  }

  private def createAppAndProcess(
      allPaths: Seq[EventLogInfo],
      startIndex: Int): Unit = {
    class ProfileProcessThread(path: EventLogInfo, index: Int) extends Runnable {
      def run: Unit = profileApp(path, index, { app =>
        val profileOutputWriter = new ProfileOutputWriter(s"$outputDir/${app.appId}",
          Profiler.PROFILE_LOG_NAME, numOutputRows, outputCSV = outputCSV)
        try {
          val (sum, _) = processApps(Seq(app), appArgs.printPlans(), profileOutputWriter)
          writeSafelyToOutput(profileOutputWriter, Seq(sum), false)
        } finally {
          profileOutputWriter.close()
        }
      })
    }

    allPaths.foreach { path =>
      try {
        threadPool.submit(new ProfileProcessThread(path, startIndex))
      } catch {
        case e: Exception =>
          logError(s"Unexpected exception submitting log ${path.eventLog.toString}, skipping!", e)
      }
    }
  }

  private def createApp(path: EventLogInfo, index: Int,
      hadoopConf: Configuration): Either[FailureApp, ApplicationInfo] = {
    try {
      // This apps only contains 1 app in each loop.
      val startTime = System.currentTimeMillis()
      val app = new ApplicationInfo(hadoopConf, path, index)
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
  private def processApps(
      apps: Seq[ApplicationInfo],
      printPlans: Boolean,
      profileOutputWriter: ProfileOutputWriter)
    : (ApplicationSummaryInfo, Option[CompareSummaryInfo]) = {
    val startTime = System.currentTimeMillis()

    val collect = new CollectInformation(apps)
    val appInfo = collect.getAppInfo
    val appLogPath = collect.getAppLogPath
    val dsInfo = collect.getDataSourceInfo
    val rapidsProps = collect.getRapidsProperties
    val sparkProps = collect.getSparkProperties
    val systemProps = collect.getSystemProperties
    val rapidsJar = collect.getRapidsJARInfo
    val sqlMetrics = collect.getSQLPlanMetrics
    val stageMetrics = collect.getStageLevelMetrics
    val wholeStage = collect.getWholeStageCodeGenMapping
    // for compare mode we just add in extra tables for matching across applications
    // the rest of the tables simply list all applications specified
    val compareRes = if (appArgs.compare()) {
      val compare = new CompareApplications(apps)
      val (matchingSqlIds, matchingStageIds) = compare.findMatchingStages()
      Some(CompareSummaryInfo(matchingSqlIds, matchingStageIds))
    } else {
      None
    }

    val healthCheck = new HealthCheck(apps)
    val failedTasks = healthCheck.getFailedTasks
    val failedStages = healthCheck.getFailedStages
    val failedJobs = healthCheck.getFailedJobs
    val removedBMs = healthCheck.getRemovedBlockManager
    val removedExecutors = healthCheck.getRemovedExecutors
    val unsupportedOps = healthCheck.getPossibleUnsupportedSQLPlan

    if (printPlans) {
      CollectInformation.printSQLPlans(apps, outputDir)
    }

    if (appArgs.generateDot()) {
      if (appArgs.compare() || appArgs.combined()) {
        logWarning("Dot graph does not compare or combine apps")
      }
      apps.foreach { app =>
        val start = System.nanoTime()
        GenerateDot(app, s"$outputDir/${app.appId}")
        val duration = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        profileOutputWriter.writeText(s"Generated DOT graphs for app ${app.appId} " +
          s"to $outputDir in $duration second(s)\n")
      }
    }

    if (appArgs.generateTimeline()) {
      if (appArgs.compare() || appArgs.combined()) {
        logWarning("Timeline graph does not compare or combine apps")
      }
      apps.foreach { app =>
        val start = System.nanoTime()
        GenerateTimeline.generateFor(app, s"$outputDir/${app.appId}")
        val duration = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        profileOutputWriter.writeText(s"Generated timeline graphs for app ${app.appId} " +
          s"to $outputDir in $duration second(s)\n")
      }
    }
    val analysis = RawMetricProfilerView.getAggMetrics(apps)
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
    logInfo(s"Took ${endTime - startTime}ms to Process [${appInfo.head.appId}]")
    (ApplicationSummaryInfo(appInfo, dsInfo,
      collect.getExecutorInfo, collect.getJobInfo, rapidsProps,
      rapidsJar, sqlMetrics, stageMetrics, analysis.jobAggs, analysis.stageAggs,
      analysis.sqlAggs, analysis.sqlDurAggs, analysis.taskShuffleSkew,
      failedTasks, failedStages, failedJobs, removedBMs, removedExecutors,
      unsupportedOps, sparkProps, collect.getSQLToStage, wholeStage, maxTaskInputInfo,
      appLogPath, analysis.ioAggs, systemProps, sqlIdAlign), compareRes)
  }

  /**
   * A wrapper method to run the AutoTuner.
   * @param appInfo     Summary of the application for tuning.
   * @param driverInfoProvider Entity that implements APIs needed to extract information from the
   *                           driver log if any
   */
  private def runAutoTuner(appInfo: Option[ApplicationSummaryInfo],
      driverInfoProvider: DriverLogInfoProvider = BaseDriverLogInfoProvider.noneDriverLog)
  : (Seq[RecommendedPropertyResult], Seq[RecommendedCommentResult]) = {
    // only run the auto tuner on GPU event logs for profiling tool right now. There are
    // assumptions made in the code
    if (appInfo.isDefined && appInfo.get.appInfo.head.pluginEnabled) {
      val appInfoProvider = AppSummaryInfoBaseProvider.fromAppInfo(appInfo)
      val workerInfoPath = appArgs.workerInfo.getOrElse(AutoTuner.DEFAULT_WORKER_INFO_PATH)
      val platform = appArgs.platform()
      val clusterPropsOpt = loadClusterProps(workerInfoPath)
      val autoTuner: AutoTuner = AutoTuner.buildAutoTuner(workerInfoPath, appInfoProvider,
        PlatformFactory.createInstance(platform, clusterPropsOpt), driverInfoProvider)

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

  def writeOutput(profileOutputWriter: ProfileOutputWriter,
      appsSum: Seq[ApplicationSummaryInfo], outputCombined: Boolean,
      comparedRes: Option[CompareSummaryInfo] = None): Unit = {

    val sums = if (outputCombined) {
      // the properties table here has the column names as the app indexes so we have to
      // handle special

      def combineProps(propSource: String,
          sums: Seq[ApplicationSummaryInfo]): Seq[RapidsPropertyProfileResult] = {
        var numApps = 0
        val props = HashMap[String, ArrayBuffer[String]]()
        val outputHeaders = ArrayBuffer("propertyName")
        sums.foreach { app =>
          val inputProps = if (propSource.equals("rapids")) {
            app.rapidsProps
          } else if (propSource.equals("spark")) {
            app.sparkProps
          } else { // this is for system properties
            app.sysProps
          }
          if (inputProps.nonEmpty) {
            numApps += 1
            val appMappedProps = inputProps.map { p =>
              p.rows(0) -> p.rows(1)
            }.toMap

            outputHeaders += inputProps.head.outputHeaders(1)
            CollectInformation.addNewProps(appMappedProps, props, numApps)
          }
        }
        val allRows = props.map { case (k, v) => Seq(k) ++ v }.toSeq
        val resRows = allRows.map(r => RapidsPropertyProfileResult(r(0), outputHeaders, r))
        resRows.sortBy(cols => cols.key)
      }
      val reduced = ApplicationSummaryInfo(
        appsSum.flatMap(_.appInfo).sortBy(_.appIndex),
        appsSum.flatMap(_.dsInfo).sortBy(_.appIndex),
        appsSum.flatMap(_.execInfo).sortBy(_.appIndex),
        appsSum.flatMap(_.jobInfo).sortBy(_.appIndex),
        combineProps("rapids", appsSum).sortBy(_.key),
        appsSum.flatMap(_.rapidsJar).sortBy(_.appIndex),
        appsSum.flatMap(_.sqlMetrics).sortBy(_.appIndex),
        appsSum.flatMap(_.stageMetrics).sortBy(_.appIndex),
        appsSum.flatMap(_.jobAggMetrics).sortBy(_.appIndex),
        appsSum.flatMap(_.stageAggMetrics).sortBy(_.appIndex),
        appsSum.flatMap(_.sqlTaskAggMetrics).sortBy(_.appIndex),
        appsSum.flatMap(_.durAndCpuMet).sortBy(_.appIndex),
        appsSum.flatMap(_.skewInfo).sortBy(_.appIndex),
        appsSum.flatMap(_.failedTasks).sortBy(_.appIndex),
        appsSum.flatMap(_.failedStages).sortBy(_.appIndex),
        appsSum.flatMap(_.failedJobs).sortBy(_.appIndex),
        appsSum.flatMap(_.removedBMs).sortBy(_.appIndex),
        appsSum.flatMap(_.removedExecutors).sortBy(_.appIndex),
        appsSum.flatMap(_.unsupportedOps).sortBy(_.appIndex),
        combineProps("spark", appsSum).sortBy(_.key),
        appsSum.flatMap(_.sqlStageInfo).sortBy(_.duration)(Ordering[Option[Long]].reverse),
        appsSum.flatMap(_.wholeStage).sortBy(_.appIndex),
        appsSum.flatMap(_.maxTaskInputBytesRead).sortBy(_.appIndex),
        appsSum.flatMap(_.appLogPath).sortBy(_.appIndex),
        appsSum.flatMap(_.ioMetrics).sortBy(_.appIndex),
        combineProps("system", appsSum).sortBy(_.key),
        appsSum.flatMap(_.sqlCleanedAlignedIds).sortBy(_.appIndex)
      )
      Seq(reduced)
    } else {
      appsSum
    }
    sums.foreach { app: ApplicationSummaryInfo =>
      profileOutputWriter.writeText("### A. Information Collected ###")
      profileOutputWriter.write(ProfInformationView.getLabel, app.appInfo)
      profileOutputWriter.write(ProfLogPathView.getLabel, app.appLogPath)
      profileOutputWriter.write(ProfDataSourceView.getLabel, app.dsInfo)
      profileOutputWriter.write(ProfExecutorView.getLabel, app.execInfo)
      profileOutputWriter.write(ProfJobsView.getLabel, app.jobInfo)
      profileOutputWriter.write(ProfSQLToStageView.getLabel, app.sqlStageInfo)
      profileOutputWriter.write(RapidsQualPropertiesView.getLabel, app.rapidsProps,
        Some(RapidsQualPropertiesView.getDescription))
      profileOutputWriter.write(SparkQualPropertiesView.getLabel, app.sparkProps,
        Some(SparkQualPropertiesView.getDescription))
      profileOutputWriter.write(SystemQualPropertiesView.getLabel, app.sysProps,
        Some(SystemQualPropertiesView.getDescription))
      profileOutputWriter.write(ProfRapidsJarView.getLabel, app.rapidsJar,
        Some(ProfRapidsJarView.getDescription))
      profileOutputWriter.write(ProfSQLPlanMetricsView.getLabel, app.sqlMetrics,
        Some(ProfSQLPlanMetricsView.getDescription))
      profileOutputWriter.write(ProfStageMetricView.getLabel, app.stageMetrics,
        Some(ProfStageMetricView.getDescription))
      profileOutputWriter.write(ProfSQLCodeGenView.getLabel, app.wholeStage,
        Some(ProfSQLCodeGenView.getDescription))
      comparedRes.foreach { compareSum =>
        val matchingSqlIds = compareSum.matchingSqlIds
        val matchingStageIds = compareSum.matchingStageIds
        profileOutputWriter.write("Matching SQL IDs Across Applications", matchingSqlIds)
        profileOutputWriter.write("Matching Stage IDs Across Applications", matchingStageIds)
      }

      profileOutputWriter.writeText("\n### B. Analysis ###\n")
      profileOutputWriter.write(JOB_AGG_LABEL, app.jobAggMetrics,
        Some(AGG_DESCRIPTION(JOB_AGG_LABEL)))
      profileOutputWriter.write(STAGE_AGG_LABEL, app.stageAggMetrics,
        Some(AGG_DESCRIPTION(STAGE_AGG_LABEL)))
      profileOutputWriter.write(SQL_AGG_LABEL, app.sqlTaskAggMetrics,
        Some(AGG_DESCRIPTION(SQL_AGG_LABEL)))
      profileOutputWriter.write(IO_LABEL, app.ioMetrics)
      profileOutputWriter.write(SQL_DUR_LABEL, app.durAndCpuMet)
      val skewHeader = TASK_SHUFFLE_SKEW
      val skewTableDesc = AGG_DESCRIPTION(TASK_SHUFFLE_SKEW)
      profileOutputWriter.write(skewHeader, app.skewInfo, tableDesc = Some(skewTableDesc))

      profileOutputWriter.writeText("\n### C. Health Check###\n")
      profileOutputWriter.write(ProfFailedTaskView.getLabel, app.failedTasks)
      profileOutputWriter.write(ProfFailedStageView.getLabel, app.failedStages)
      profileOutputWriter.write(ProfFailedJobsView.getLabel, app.failedJobs)
      profileOutputWriter.write(ProfRemovedBLKMgrView.getLabel, app.removedBMs)
      profileOutputWriter.write(ProfRemovedExecutorView.getLabel, app.removedExecutors)
      profileOutputWriter.write("Unsupported SQL Plan", app.unsupportedOps,
        Some("Unsupported SQL Ops"))
      if (outputAlignedSQLIds) {
        profileOutputWriter.write(ProfSQLPlanAlignedView.getLabel, app.sqlCleanedAlignedIds,
          Some(ProfSQLPlanAlignedView.getDescription))
      }
      if (useAutoTuner) {
        val (properties, comments) = runAutoTuner(Some(app))
        profileOutputWriter.writeText("\n### D. Recommended Configuration ###\n")
        profileOutputWriter.writeText(Profiler.getAutoTunerResultsAsString(properties, comments))
      }
    }
  }

  /**
   * Safely writes the application summary information to the specified profileOutputWriter.
   * If an exception occurs during the writing process, it will be caught and logged, preventing
   * it from propagating further.
   */
  private def writeSafelyToOutput(profileOutputWriter: ProfileOutputWriter,
      appsSum: Seq[ApplicationSummaryInfo], outputCombined: Boolean,
      comparedRes: Option[CompareSummaryInfo] = None): Unit = {
    try {
      writeOutput(profileOutputWriter, appsSum, outputCombined, comparedRes)
    } catch {
      case e: Exception =>
        logError("Exception thrown while writing", e)
    }
  }
}

object Profiler {
  // This tool's output log file name
  val PROFILE_LOG_NAME = "profile"
  val DRIVER_LOG_NAME = "driver"
  val COMPARE_LOG_FILE_NAME_PREFIX = "rapids_4_spark_tools_compare"
  val COMBINED_LOG_FILE_NAME_PREFIX = "rapids_4_spark_tools_combined"
  val SUBDIR = "rapids_4_spark_profile"

  def getAutoTunerResultsAsString(props: Seq[RecommendedPropertyResult],
      comments: Seq[RecommendedCommentResult]): String = {
    val propStr = if (props.nonEmpty) {
        val propertiesToStr = props.map(_.toString).reduce(_ + "\n" + _)
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
