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

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, PlatformFactory}
import com.nvidia.spark.rapids.tool.tuning.{ClusterProperties, TunerContext}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.AppFilterImpl
import org.apache.spark.sql.rapids.tool.util.{PropertiesLoader, RapidsToolsConfUtil}

/**
 * A tool to analyze Spark event logs and determine if
 * they might be a good fit for running on the GPU.
 */
object QualificationMain extends Logging {

  def main(args: Array[String]): Unit = {
    val runResult =
      mainInternal(new QualificationArgs(args), enablePB = true)
    if (runResult.isFailed) {
      System.exit(runResult.returnCode)
    }
  }

  /**
   * Entry point for tests
   */
  def mainInternal(appArgs: QualificationArgs,
      enablePB: Boolean = false): QualToolResult = {

    val eventlogPaths = appArgs.eventlog()
    val filterN = appArgs.filterCriteria
    val minEventLogSize = appArgs.minEventLogSize.toOption
    val maxEventLogSize = appArgs.maxEventLogSize.toOption
    val matchEventLogs = appArgs.matchEventLogs
    val outputDirectory = appArgs.outputDirectory().stripSuffix("/")
    val maxSQLDescLength = appArgs.maxSqlDescLength.getOrElse(100)

    val nThreads = appArgs.numThreads.getOrElse(
      Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)
    val timeout = appArgs.timeout.toOption
    val reportSqlLevel = appArgs.perSql.getOrElse(false)
    val mlOpsEnabled = appArgs.mlFunctions.getOrElse(false)
    val penalizeTransitions = appArgs.penalizeTransitions.getOrElse(true)
    val recursiveSearchEnabled = !appArgs.noRecursion()

    val hadoopConf = RapidsToolsConfUtil.newHadoopConf()
    // Note we need the platform to get the correct files we use in the PluginTypeChecker.
    // Here we create one dummy global one just to get those files as the platform should
    // be the same for all applications but then later we create a per application one for
    // properly tracking the cluster info and recommended cluster info per application.
    // This platform instance should not be used for anything other then referencing the
    // files for this particular Platform.
    val referencePlatform = try {
      val clusterPropsOpt = appArgs.workerInfo.toOption.flatMap(
        PropertiesLoader[ClusterProperties].loadFromFile)
      PlatformFactory.createInstance(appArgs.platform(), clusterPropsOpt)
    } catch {
      case NonFatal(e) =>
        logError("Error creating the platform", e)
        return QualToolResultBuilder.failedResult()
    }

    val pluginTypeChecker = try {
      new PluginTypeChecker(
        referencePlatform,
        appArgs.speedupFactorFile.toOption)
    } catch {
      case ie: IllegalStateException =>
        logError("Error creating the plugin type checker!", ie)
        return QualToolResultBuilder.failedResult()
    }

    val (eventLogFsFiltered, allEventLogs) = EventLogPathProcessor.processAllPaths(
      filterN.toOption, matchEventLogs.toOption, eventlogPaths, hadoopConf, recursiveSearchEnabled,
      maxEventLogSize, minEventLogSize, appArgs.fsStartTime.toOption, appArgs.fsEndTime.toOption)

    val filteredLogs = if (argsContainsAppFilters(appArgs)) {
      val appFilter = new AppFilterImpl(hadoopConf, timeout, nThreads)
      val finaleventlogs = if (appArgs.any() && argsContainsFSFilters(appArgs)) {
        (appFilter.filterEventLogs(allEventLogs, appArgs) ++ eventLogFsFiltered).distinct
      } else {
        appFilter.filterEventLogs(eventLogFsFiltered, appArgs)
      }
      finaleventlogs
    } else {
      eventLogFsFiltered
    }

    if (filteredLogs.isEmpty) {
      logWarning("No event logs to process after checking paths, exiting!")
      return QualToolResultBuilder.emptyResult()
    }
    // create the AutoTuner context object
    val tunerContext = if (appArgs.autoTuner()) {
      TunerContext(outputDirectory, Option(hadoopConf))
    } else {
      None
    }
    val qual = new Qualification(outputDirectory, hadoopConf, timeout,
      nThreads, pluginTypeChecker,
      enablePB, reportSqlLevel, maxSQLDescLength, mlOpsEnabled, penalizeTransitions,
      tunerContext, appArgs.clusterReport(), appArgs.platform(), appArgs.workerInfo.toOption)
    val res = qual.qualifyApps(filteredLogs)
    res
  }

  def argsContainsFSFilters(appArgs: QualificationArgs): Boolean = {
    val filterCriteria = appArgs.filterCriteria.toOption
    appArgs.matchEventLogs.isSupplied ||
      (filterCriteria.isDefined && filterCriteria.get.endsWith("-filesystem")) ||
      appArgs.maxEventLogSize.toOption.isDefined ||
      appArgs.minEventLogSize.toOption.isDefined ||
      appArgs.fsStartTime.toOption.isDefined ||
      appArgs.fsEndTime.toOption.isDefined
  }

  def argsContainsAppFilters(appArgs: QualificationArgs): Boolean = {
    val filterCriteria = appArgs.filterCriteria.toOption
    appArgs.applicationName.isSupplied || appArgs.startAppTime.isSupplied ||
        appArgs.userName.isSupplied || appArgs.sparkProperty.isSupplied ||
        (filterCriteria.isDefined && (filterCriteria.get.endsWith("-newest") ||
            filterCriteria.get.endsWith("-oldest") || filterCriteria.get.endsWith("-per-app-name")))
  }
}
