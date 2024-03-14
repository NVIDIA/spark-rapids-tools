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

package org.apache.spark.sql.rapids.tool.qualification

import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.planparser.{DataWritingCommandExecParser, ExecInfo, PlanInfo, SQLPlanParser}
import com.nvidia.spark.rapids.tool.profiling._
import com.nvidia.spark.rapids.tool.qualification._
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter.DEFAULT_JOB_FREQUENCY
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.rapids.tool.{AppBase, ClusterSummary, GpuEventLogException, SqlPlanInfoGraphBuffer, SupportedMLFuncsName, ToolUtils}


class QualificationAppInfo(
    eventLogInfo: Option[EventLogInfo],
    hadoopConf: Option[Configuration] = None,
    pluginTypeChecker: PluginTypeChecker,
    reportSqlLevel: Boolean,
    perSqlOnly: Boolean = false,
    mlOpsEnabled: Boolean = false,
    penalizeTransitions: Boolean = true)
  extends AppBase(eventLogInfo, hadoopConf) with Logging {

  var lastJobEndTime: Option[Long] = None
  var lastSQLEndTime: Option[Long] = None
  val writeDataFormat: ArrayBuffer[String] = ArrayBuffer[String]()

  var appInfo: Option[QualApplicationInfo] = None

  val sqlIDToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]
  val stageIdToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]
  val stageIdToGpuCpuTransitions: HashMap[Int, Int] = HashMap.empty[Int, Int]

  val stageIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val sqlIDtoFailures: HashMap[Long, ArrayBuffer[String]] = HashMap.empty[Long, ArrayBuffer[String]]

  val notSupportFormatAndTypes: HashMap[String, Set[String]] = HashMap[String, Set[String]]()

  // clusterTags can be redacted so try to get ClusterId and ClusterName separately
  var clusterTags: String = ""
  var clusterTagClusterId: String = ""
  var clusterTagClusterName: String = ""
  private lazy val eventProcessor =  new QualificationEventProcessor(this, perSqlOnly)

  /**
   * Get the event listener the qualification tool uses to process Spark events.
   * Install this listener in Spark.
   *
   * {{{
   *   spark.sparkContext.addSparkListener(listener)
   * }}}
   * @return SparkListener
   */
  def getEventListener: SparkListener = {
    eventProcessor
  }

  processEvents()

  override def processEvent(event: SparkListenerEvent): Boolean = {
    eventProcessor.processAnyEvent(event)
    false
  }

  override def cleanupStages(stageIds: Set[Int]): Unit = {
    stageIds.foreach { stageId =>
      stageIdToTaskEndSum.remove(stageId)
      stageIdToSqlID.remove(stageId)
    }
    super.cleanupStages(stageIds)
  }

  override def cleanupSQL(sqlID: Long): Unit = {
    sqlIDToTaskEndSum.remove(sqlID)
    sqlIDtoFailures.remove(sqlID)
    super.cleanupSQL(sqlID)
  }

  override def handleEnvUpdateForCachedProps(event: SparkListenerEnvironmentUpdate): Unit = {
    super.handleEnvUpdateForCachedProps(event)
    if (gpuMode) {
      throw GpuEventLogException(s"Cannot parse event logs from GPU run")
    }
    clusterTags = sparkProperties.getOrElse("spark.databricks.clusterUsageTags.clusterAllTags", "")
    clusterTagClusterId =
      sparkProperties.getOrElse("spark.databricks.clusterUsageTags.clusterId", "")
    clusterTagClusterName =
      sparkProperties.getOrElse("spark.databricks.clusterUsageTags.clusterName", "")
  }

  override def handleJobStartForCachedProps(event: SparkListenerJobStart): Unit = {
    super.handleJobStartForCachedProps(event)
    // If the confs are set after SparkSession initialization, it is captured in this event.
    if (clusterTags.isEmpty) {
      clusterTags = event.properties.getProperty(
        "spark.databricks.clusterUsageTags.clusterAllTags", "")
    }
    if (clusterTagClusterId.isEmpty) {
      clusterTagClusterId = event.properties.getProperty(
        "spark.databricks.clusterUsageTags.clusterId", "")
    }
    if (clusterTagClusterName.isEmpty) {
      clusterTagClusterName =event.properties.getProperty(
        "spark.databricks.clusterUsageTags.clusterName", "")
    }
  }

  // time in ms
  private def calculateAppDuration(startTime: Long): Option[Long] = {
    if (startTime > 0) {
      val estimatedResult =
        this.appEndTime match {
          case Some(_) => this.appEndTime
          case None =>
            if (lastSQLEndTime.isEmpty && lastJobEndTime.isEmpty) {
              None
            } else {
              logWarning(s"Application End Time is unknown for $appId, estimating based on" +
                " job and sql end times!")
              // estimate the app end with job or sql end times
              val sqlEndTime = if (this.lastSQLEndTime.isEmpty) 0L else this.lastSQLEndTime.get
              val jobEndTime = if (this.lastJobEndTime.isEmpty) 0L else lastJobEndTime.get
              val maxEndTime = math.max(sqlEndTime, jobEndTime)
              if (maxEndTime == 0) None else Some(maxEndTime)
            }
        }
      ProfileUtils.OptionLongMinusLong(estimatedResult, startTime)
    } else {
      None
    }
  }

  // Assume that overhead is the all time windows that do not overlap with a running job.
  private def calculateJobOverHeadTime(startTime: Long): Long = {
    // Simple algorithm:
    // 1- sort all jobs by start/endtime.
    // 2- Initialize Time(p) = app.StartTime
    // 3- loop on the sorted seq. if the job.startTime is larger than the current Time(p):
    //    then this must be considered a gap
    // 4- Update Time(p) at the end of each iteration: Time(p+1) = Max(Time(p), job.endTime)
    val sortedJobs = jobIdToInfo.values.toSeq.sortBy(_.startTime)
    var pivot = startTime
    var overhead : Long = 0

    sortedJobs.foreach { job =>
      val timeDiff = job.startTime - pivot
      if (timeDiff > 0) {
        overhead += timeDiff
      }
      // if jobEndTime is not set, use job.startTime
      pivot = Math.max(pivot, job.endTime.getOrElse(job.startTime))
    }
    overhead
  }

  // Look at the total task times for all jobs/stages that aren't SQL or
  // SQL but dataset or rdd
  private def calculateNonSQLTaskDataframeDuration(
      taskDFDuration: Long,
      totalTransitionTime: Long): Long = {
    val allTaskTime = stageIdToTaskEndSum.values.map(_.totalTaskDuration).sum + totalTransitionTime
    val res = allTaskTime - taskDFDuration
    assert(res >= 0)
    res
  }

  private def calculateCpuTimePercent(perSqlStageSummary: Seq[SQLStageSummary]): Double = {
    val totalCpuTime = perSqlStageSummary.map(_.execCPUTime).sum
    val totalRunTime = perSqlStageSummary.map(_.execRunTime).sum
    ToolUtils.calculateDurationPercent(totalCpuTime, totalRunTime)
  }

  private def calculateSQLSupportedTaskDuration(all: Seq[StageQualSummaryInfo]): Long = {
    all.map(s => s.stageTaskTime - s.unsupportedTaskDur).sum
  }

  private def calculateSQLUnsupportedTaskDuration(all: Seq[StageQualSummaryInfo]): Long = {
    all.map(_.unsupportedTaskDur).sum
  }

  private def calculateSpeedupFactor(all: Seq[StageQualSummaryInfo]): Double = {
    val allSpeedupFactors = all.filter(_.stageTaskTime > 0).map(_.averageSpeedup)
    val res = SQLPlanParser.averageSpeedup(allSpeedupFactors)
    res
  }

  private def getAllReadFileFormats: Seq[String] = {
    dataSourceInfo.map { ds =>
      s"${ds.format.toLowerCase()}[${ds.schema}]"
    }
  }

  protected def checkUnsupportedReadFormats(): Unit = {
    if (dataSourceInfo.size > 0) {
      dataSourceInfo.map { ds =>
        val (_, nsTypes) = pluginTypeChecker.scoreReadDataTypes(ds.format, ds.schema)
        if (nsTypes.nonEmpty) {
          val currentFormat = notSupportFormatAndTypes.get(ds.format).getOrElse(Set.empty[String])
          notSupportFormatAndTypes(ds.format) = (currentFormat ++ nsTypes)
        }
      }
    }
  }

  /**
   * Checks the stage ID in the execution information.
   * This function determines the associated stages for the given execution information by
   * checking the stages in the current execution information, the previous execution information,
   * and the next execution information. If there are associated stages, it returns a sequence of
   * stage ID and execution information pairs. Otherwise, it returns an optional execution
   * information(not associated with any stage). If there is stage ID associated with both the
   * previous and the next execution information, then the current execution information is
   * associated with the stage ID of the previous execution information.
   * @param prev     The previous execution information.
   * @param execInfo The current execution information.
   * @param next     The next execution information.
   * @return A tuple containing a sequence of stage ID and execution information pairs,
   *         and an optional execution information.
   */
  private def checkStageIdInExec(prev: Option[ExecInfo],
      execInfo: ExecInfo, next: Option[ExecInfo]): (Seq[(Int, ExecInfo)], Option[ExecInfo]) = {
    val associatedStages = {
      if (execInfo.stages.nonEmpty) {
        execInfo.stages.toSeq
      } else {
        if (prev.exists(_.stages.nonEmpty)) {
          prev.flatMap(_.stages.headOption).toSeq
        } else if (next.exists(_.stages.nonEmpty)) {
          next.flatMap(_.stages.headOption).toSeq
        } else {
          // we don't know what stage its in or its duration
          logDebug(s"No stage associated with ${execInfo.exec} " +
            s"so speedup factor isn't applied anywhere.")
          Seq.empty
        }
      }
    }
    if (associatedStages.nonEmpty) {
      (associatedStages.map((_, execInfo)), None)
    } else {
      (Seq.empty, Some(execInfo))
    }
  }

  private def getStageToExec(execInfos: Seq[ExecInfo]): (Map[Int, Seq[ExecInfo]], Seq[ExecInfo]) = {
    val execsWithoutStages = new ArrayBuffer[ExecInfo]()

    // This is to get the mapping between stageId and execs. This is primarily done based on
    // accumulatorId. If an Exec has some metrics generated, then an accumulatorId will be
    // generated for that Exec. This accumulatorId is used to get the stageId. If an Exec
    // doesn't have any metrics, then we will try to get the stageId by looking at the
    // neighbor Execs. If either of the neighbor Execs has a stageId, then we will use that
    // to assign the same stageId to the current Exec as it's most likely that the current
    // Exec is part of the same stage as the neighbor Exec.
    val execInfosInOrder = execInfos.reverse
    val execsToStageMap = execInfosInOrder.indices.map {
      // corner case to handle first element
      case 0 => if (execInfosInOrder.size > 1) {
        // If there are more than one Execs, then check if the next Exec has a stageId.
        checkStageIdInExec(None, execInfosInOrder(0), Some(execInfosInOrder(1)))
      } else {
        checkStageIdInExec(None, execInfosInOrder(0), None)
      }
      // corner case to handle last element
      case i if i == execInfosInOrder.size - 1 && execInfosInOrder.size > 1 =>
        // If there are more than one Execs, then check if the previous Exec has a stageId.
        checkStageIdInExec(Some(execInfosInOrder(i - 1)), execInfosInOrder(i), None)
      case i =>
        checkStageIdInExec(Some(execInfosInOrder(i - 1)),
          execInfosInOrder(i), Some(execInfosInOrder(i + 1)))
    }
    val perStageSum = execsToStageMap.map(_._1).toList.flatten
      .groupBy(_._1).map { case (stage, execInfo) =>
        (stage, execInfo.map(_._2))
      }

    // Add all the execs that don't have a stageId to execsWithoutStages.
    execsWithoutStages ++= execsToStageMap.map(_._2).toList.flatten

    (perStageSum, execsWithoutStages)
  }

  private def flattenedExecs(execs: Seq[ExecInfo]): Seq[ExecInfo] = {
    // need to remove the WholeStageCodegen wrappers since they aren't actual
    // execs that we want to get timings of
    execs.flatMap { e =>
      if (e.exec.contains("WholeStageCodegen")) {
        e.children.getOrElse(Seq.empty)
      } else {
        e.children.getOrElse(Seq.empty) :+ e
      }
    }
  }

  private def stagesSummary(execInfos: Seq[ExecInfo],
      stages: Seq[Int], estimated: Boolean): Set[StageQualSummaryInfo] = {
    val allStageTaskTime = stages.map { stageId =>
      stageIdToTaskEndSum.get(stageId).map(_.totalTaskDuration).getOrElse(0L)
    }.sum
    val allSpeedupFactorAvg = SQLPlanParser.averageSpeedup(execInfos.map(_.speedupFactor))
    val allFlattenedExecs = flattenedExecs(execInfos)
    // Add penalty if there are UDF's. The time taken in stage with UDF's is usually more than on
    // GPU due to fallback. So, we are adding a penalty to the speedup factor if there are UDF's.
    val udfs = allFlattenedExecs.filter(x => x.udf == true)
    val stageFinalSpeedupFactor = if (udfs.nonEmpty) {
      allSpeedupFactorAvg / 2.0
    } else {
      allSpeedupFactorAvg
    }
    val numUnsupported = allFlattenedExecs.filterNot(_.isSupported)
    val unsupportedExecs = numUnsupported
    // if we have unsupported try to guess at how much time.  For now divide
    // time by number of execs and give each one equal weight
    val eachExecTime = allStageTaskTime / allFlattenedExecs.size
    val unsupportedDur = eachExecTime * numUnsupported.size
    // split unsupported per stage
    val eachStageUnsupported = unsupportedDur / stages.size
    stages.map { stageId =>
      val stageTaskTime = stageIdToTaskEndSum.get(stageId)
        .map(_.totalTaskDuration).getOrElse(0L)
      val numTransitions = penalizeTransitions match {
        case true => stageIdToGpuCpuTransitions.getOrElse(stageId, 0)
        case false => 0
      }
      val transitionsTime = numTransitions match {
        case 0 => 0L // no transitions
        case _ =>
          // Duration to transfer data from GPU to CPU and vice versa.
          // Assuming it's a PCI-E Gen3, but also assuming that some of the result could be
          // spilled to disk.
          // Duration in Spark metrics is in milliseconds and CPU-GPU transfer rate is in bytes/sec.
          // So we need to convert the transitions time to milliseconds.
          val totalBytesRead = {
            stageIdToTaskEndSum.get(stageId).map(_.totalbytesRead).getOrElse(0L)
          }
          if (totalBytesRead > 0) {
            val transitionTime = (totalBytesRead /
              QualificationAppInfo.CPU_GPU_TRANSFER_RATE.toDouble) * numTransitions
            (transitionTime * 1000).toLong // convert to milliseconds
          } else {
            0L
          }
      }
      val finalEachStageUnsupported = if (transitionsTime != 0) {
        // Add 50% penalty for unsupported duration if there are transitions. This number
        // was randomly picked because it matched roughly what we saw on the experiments
        // with customer/nds event logs
        (eachStageUnsupported * 0.5 + eachStageUnsupported).toLong
      } else {
        eachStageUnsupported
      }

      // Get stage info for the given stageId.
      val stageInfos = stageIdToInfo.filterKeys { case (id, _) => id == stageId }
      val wallclockStageDuration = stageInfos.values.map(x => x.duration.getOrElse(0L)).sum

      StageQualSummaryInfo(stageId, stageFinalSpeedupFactor, stageTaskTime,
        finalEachStageUnsupported, numTransitions, transitionsTime, estimated,
        wallclockStageDuration, unsupportedExecs)
    }.toSet
  }

  private def calculateNumberOfTransitions(allStagesToExecs: Map[Int, Seq[ExecInfo]]): Unit = {
    allStagesToExecs.foreach { case (stageId, execs) =>
      // Flatten all the Execs within a stage.
      // Example: Exchange;WholeStageCodegen (14);Exchange;WholeStageCodegen (13);Exchange
      // will be flattened to Exchange;Sort;Exchange;Sort;SortMergeJoin;SortMergeJoin;Exchange;
      val allExecs = execs.map(x => if (x.exec.startsWith("WholeStage")) {
        x.children.getOrElse(Seq.empty).reverse
      } else {
        Seq(x)
      }).flatten

      // If it's a shuffle stage, then we need to keep the first and last Exchange and remove
      // all the intermediate Exchanges as input size is captured in Exchange node.
      val dedupedExecs = if (allExecs.size > 2) {
        allExecs.head +:
          allExecs.tail.init.filter(x => x.exec != "Exchange") :+ allExecs.last
      } else {
        allExecs
      }
      // Create a list of transitions by zipping allExecs with itself but with the first element
      // This will create a list of adjacent pairs.
      // Example: If allExecs = (ScanExec, FilterExec, SortExec, ProjectExec), then it will create
      // a list of tuples as follows:
      // (ScanExec, FilterExec), (FilterExec, SortExec), (SortExec, ProjectExec)
      val transitions = dedupedExecs.zip(dedupedExecs.drop(1)).count {
        // If the current execution (currExec) is supported, and the next execution (nextExec)
        // is not supported, or if the current execution is not supported and the next execution
        // is supported, then we consider this as a transition.
        case (currExec, nextExec) => (currExec.isSupported && !nextExec.isSupported) ||
          (!currExec.isSupported && nextExec.isSupported)
      }
      stageIdToGpuCpuTransitions(stageId) = transitions
    }
  }

  def summarizeStageLevel(execInfos: Seq[ExecInfo], sqlID: Long): Set[StageQualSummaryInfo] = {
    val (allStagesToExecs, execsNoStage) = getStageToExec(execInfos)

    // Get the total number of transitions between CPU and GPU for each stage and
    // store it in a Map.
    calculateNumberOfTransitions(allStagesToExecs)

    if (allStagesToExecs.isEmpty) {
      // use job level
      // also get the job ids associated with the SQLId
      val allStagesBasedOnJobs = getAllStagesForJobsInSqlQuery(sqlID)
      if (allStagesBasedOnJobs.isEmpty) {
        Set.empty
      } else {
        // we don't know which execs map to each stage so we are going to cheat somewhat and
        // apply equally and then just split apart for per stage reporting
        stagesSummary(execInfos, allStagesBasedOnJobs, true)
      }
    } else {
      val stageIdsWithExecs = allStagesToExecs.keys.toSet
      val allStagesBasedOnJobs = getAllStagesForJobsInSqlQuery(sqlID)
      val stagesNotAccounted = allStagesBasedOnJobs.toSet -- stageIdsWithExecs
      val stageSummaryNotMapped = if (stagesNotAccounted.nonEmpty) {
        if (execsNoStage.nonEmpty) {
          stagesSummary(execsNoStage, stagesNotAccounted.toSeq, true)
        } else {
          // weird case, stages but not associated execs, not sure what to do with this so
          // just drop for now
          Set.empty
        }
      } else {
        Set.empty
      }
      // if it doesn't have a stage id associated we can't calculate the time spent in that
      // SQL so we just drop it
      val stagesFromExecs = stageIdsWithExecs.flatMap { stageId =>
        val execsForStage = allStagesToExecs.getOrElse(stageId, Seq.empty)
        stagesSummary(execsForStage, Seq(stageId), false)
      }
      stagesFromExecs ++ stageSummaryNotMapped
    }
  }

  def summarizeSQLStageInfo(planInfos: Seq[PlanInfo]): Seq[SQLStageSummary] = {
    planInfos.flatMap { pInfo =>
      val perSQLId = pInfo.execInfo.groupBy(_.sqlID)
      perSQLId.map { case (sqlID, execInfos) =>
        val sqlWallClockDuration = sqlIdToInfo.get(sqlID).flatMap(_.duration).getOrElse(0L)
        // There are issues with duration in whole stage code gen where duration of multiple
        // execs is more than entire stage time, for now ignore the exec duration and just
        // calculate based on average applied to total task time of each stage.
        // Also note that the below can map multiple stages to the same exec for things like
        // shuffle.

        // if it doesn't have a stage id associated we can't calculate the time spent in that
        // SQL so we just drop it
        val stageSum = summarizeStageLevel(execInfos, sqlID)

        val numUnsupportedExecs = execInfos.filterNot(_.isSupported).size
        // This is a guestimate at how much wall clock was supported
        val numExecs = execInfos.size.toDouble
        val numSupportedExecs = (numExecs - numUnsupportedExecs)
        val ratio = numSupportedExecs / numExecs
        val estimateWallclockSupported = (sqlWallClockDuration * ratio).toInt
        // don't worry about supported execs for these are these are mostly indicator of I/O
        val execRunTime = sqlIDToTaskEndSum.get(sqlID).map(_.executorRunTime).getOrElse(0L)
        val execCPUTime = sqlIDToTaskEndSum.get(sqlID).map(_.executorCPUTime).getOrElse(0L)
        SQLStageSummary(stageSum, sqlID, estimateWallclockSupported,
          execCPUTime, execRunTime)
      }
    }
  }

  def removeExecsShouldRemove(origPlanInfos: Seq[PlanInfo]): Seq[PlanInfo] = {
    origPlanInfos.map { p =>
      val execFilteredChildren = p.execInfo.map { e =>
        val filteredChildren = e.children.map { c =>
          c.filterNot(_.shouldRemove)
        }
        e.copy(children = filteredChildren)
      }
      val filteredPlanInfos = execFilteredChildren.filterNot(_.shouldRemove)
      p.copy(execInfo = filteredPlanInfos)
    }
  }

  private def prepareClusterTags: Map[String, String] = {
    val initialClusterTagsMap = if (clusterTags.nonEmpty) {
      ToolUtils.parseClusterTags(clusterTags)
    } else {
      Map.empty[String, String]
    }

    val tagsMapWithClusterId = if (!initialClusterTagsMap.contains(QualOutputWriter.CLUSTER_ID)
      && clusterTagClusterId.nonEmpty) {
      initialClusterTagsMap + (QualOutputWriter.CLUSTER_ID -> clusterTagClusterId)
    } else {
      initialClusterTagsMap
    }

    if (!tagsMapWithClusterId.contains(QualOutputWriter.JOB_ID) && clusterTagClusterName.nonEmpty) {
      val clusterTagJobId = ToolUtils.parseClusterNameForJobId(clusterTagClusterName)
      clusterTagJobId.map { jobId =>
        tagsMapWithClusterId + (QualOutputWriter.JOB_ID -> jobId)
      }.getOrElse(tagsMapWithClusterId)
    } else {
      tagsMapWithClusterId
    }
  }

  /**
   * Aggregate and process the application after reading the events.
   * @return Option of QualificationSummaryInfo, Some if we were able to process the application
   *         otherwise None.
   */
  def aggregateStats(): Option[QualificationSummaryInfo] = {
    appInfo.map { info =>
      val appDuration = calculateAppDuration(info.startTime).getOrElse(0L)

      // if either job or stage failures then we mark as N/A
      // TODO - what about incomplete, do we want to change those?
      val sqlIdsWithFailures = sqlIDtoFailures.filter { case (_, v) =>
        v.size > 0
      }.keys.map(_.toString).toSeq

      // a bit odd but force filling in notSupportFormatAndTypes
      // TODO - make this better
      checkUnsupportedReadFormats()
      val notSupportFormatAndTypesString = notSupportFormatAndTypes.map { case (format, types) =>
        val typeString = types.mkString(":").replace(",", ":")
        s"${format}[$typeString]"
      }.toSeq
      val writeFormat = writeFormatNotSupported(writeDataFormat)
      val (allComplexTypes, nestedComplexTypes) = reportComplexTypes
      val problems = getAllPotentialProblems(getPotentialProblemsForDf, nestedComplexTypes)

      val origPlanInfos = sqlPlans.collect {
        case (id, plan) if sqlIdToInfo.contains(id) =>
          val sqlDesc = sqlIdToInfo(id).description
          SQLPlanParser.parseSQLPlan(appId, plan, id, sqlDesc, pluginTypeChecker, this)
      }.toSeq

      // get summary of each SQL Query for original plan
      val origPlanInfosSummary = summarizeSQLStageInfo(origPlanInfos)
      // filter out any execs that should be removed
      val planInfos = removeExecsShouldRemove(origPlanInfos)
      // get a summary of each SQL Query
      val perSqlStageSummary = summarizeSQLStageInfo(planInfos)
      // wall clock time
      val executorCpuTimePercent = calculateCpuTimePercent(perSqlStageSummary)

      // Using the spark SQL reported duration, this could be a bit off from the
      // task times because it relies on the stage times and we might not have
      // a stage for every exec
      val allSQLDurations = getAllSQLDurations

      val appName = appInfo.map(_.appName).getOrElse("")

      val allClusterTagsMap = prepareClusterTags

      val perSqlInfos = if (reportSqlLevel) {
        Some(planInfos.flatMap { pInfo =>
          sqlIdToInfo.get(pInfo.sqlID).map { info =>
            val wallClockDur = info.duration.getOrElse(0L)
            // get task duration ratio
            val sqlStageSums = perSqlStageSummary.filter(_.sqlID == pInfo.sqlID)
            val estimatedInfo = getPerSQLWallClockSummary(sqlStageSums, wallClockDur,
              sqlIDtoFailures.get(pInfo.sqlID).nonEmpty, appName)
            EstimatedPerSQLSummaryInfo(pInfo.sqlID, pInfo.sqlDesc, estimatedInfo)
          }
        })
      } else {
        None
      }

      val sparkSQLDFWallClockDuration = allSQLDurations.sum
      val longestSQLDuration = if (allSQLDurations.size > 0) {
        allSQLDurations.max
      } else {
        0L
      }

      // the same stage might be referenced from multiple sql queries, we have to dedup them
      // with the assumption the stage was reused so time only counts once
      val allStagesSummary = perSqlStageSummary.flatMap(_.stageSum)
        .map(sum => sum.stageId -> sum).toMap.values.toSeq
      val sqlDataframeTaskDuration = allStagesSummary.map(s => s.stageTaskTime).sum
      val totalTransitionsTime = allStagesSummary.map(s => s.transitionTime).sum
      val unsupportedSQLTaskDuration = calculateSQLUnsupportedTaskDuration(allStagesSummary)
      val endDurationEstimated = this.appEndTime.isEmpty && appDuration > 0
      val jobOverheadTime = calculateJobOverHeadTime(info.startTime)
      val nonSQLDataframeTaskDuration =
        calculateNonSQLTaskDataframeDuration(sqlDataframeTaskDuration, totalTransitionsTime)
      val nonSQLTaskDuration = nonSQLDataframeTaskDuration + jobOverheadTime
      // note that these ratios are based off the stage times which may be missing some stage
      // overhead or execs that didn't have associated stages
      val supportedSQLTaskDuration = calculateSQLSupportedTaskDuration(allStagesSummary)
      val taskSpeedupFactor = calculateSpeedupFactor(allStagesSummary)
      // Get all the unsupported Execs from the plan
      val unSupportedExecs = planInfos.flatMap { p =>
        // WholeStageCodeGen is excluded from the result.
        val topLevelExecs = p.execInfo.filterNot(_.isSupported).filterNot(
          x => x.exec.startsWith("WholeStage"))
        val childrenExecs = p.execInfo.flatMap { e =>
          e.children.map(x => x.filterNot(_.isSupported))
        }.flatten
        topLevelExecs ++ childrenExecs
      }.map(_.exec).toSet.mkString(";").trim.replaceAll("\n", "").replace(",", ":")

      // Get all the unsupported Expressions from the plan
      val unSupportedExprs = origPlanInfos.map(_.execInfo.flatMap(
        _.unsupportedExprs)).flatten.filter(_.nonEmpty).toSet.mkString(";")
        .trim.replaceAll("\n", "").replace(",", ":")

      // Get all unsupported execs and expressions from the plan in form of map[exec -> exprs]
      val unsupportedExecExprsMap = planInfos.flatMap { p =>
        val topLevelExecs = p.execInfo.filterNot(_.isSupported).filterNot(
          x => x.exec.startsWith("WholeStage"))
        val childrenExecs = p.execInfo.flatMap { e =>
          e.children.map(x => x.filterNot(_.isSupported))
        }.flatten
        val execs = topLevelExecs ++ childrenExecs
        val exprs = execs.filter(_.unsupportedExprs.nonEmpty).map(
          e => e.exec -> e.unsupportedExprs.mkString(";")).toMap
        exprs
      }.toMap

      // check if there are any SparkML/XGBoost functions or expressions if the mlOpsEnabled
      // config is true
      val mlFunctions = if (mlOpsEnabled) {
        getMlFuntions
      } else {
        None
      }

      val mlTotalStageDuration = if (mlFunctions.isDefined) {
        getMlTotalStageDuration(mlFunctions.get)
      } else {
        None
      }

      val mlSpeedup = if (mlTotalStageDuration.nonEmpty) {
        getMlSpeedUp(mlTotalStageDuration.get, mlEventLogType)
      } else {
        None
      }

      // get the ratio based on the Task durations that we will use for wall clock durations
      // totalTransitionTime is the overhead time for ColumnarToRow/RowToColumnar transitions
      // which impacts the GPU ratio.
      val estimatedGPURatio = if (sqlDataframeTaskDuration > 0) {
        supportedSQLTaskDuration.toDouble / (
          sqlDataframeTaskDuration.toDouble + totalTransitionsTime.toDouble)
      } else {
        1
      }

      val wallClockSqlDFToUse = QualificationAppInfo.wallClockSqlDataFrameToUse(
        sparkSQLDFWallClockDuration, appDuration)

      val estimatedInfo = QualificationAppInfo.calculateEstimatedInfoSummary(estimatedGPURatio,
        sparkSQLDFWallClockDuration, appDuration, taskSpeedupFactor, appName, appId,
        sqlIdsWithFailures.nonEmpty, mlSpeedup, unSupportedExecs, unSupportedExprs,
        allClusterTagsMap)

      val clusterSummary = ClusterSummary(info.appName, appId,
        eventLogInfo.map(_.eventLog.toString), getClusterInfo)

      QualificationSummaryInfo(info.appName, appId, problems,
        executorCpuTimePercent, endDurationEstimated, sqlIdsWithFailures,
        notSupportFormatAndTypesString, getAllReadFileFormats, writeFormat,
        allComplexTypes, nestedComplexTypes, longestSQLDuration, sqlDataframeTaskDuration,
        nonSQLTaskDuration, unsupportedSQLTaskDuration, supportedSQLTaskDuration,
        taskSpeedupFactor, info.sparkUser, info.startTime, wallClockSqlDFToUse,
        origPlanInfos, origPlanInfosSummary.map(_.stageSum).flatten,
        perSqlStageSummary.map(_.stageSum).flatten, estimatedInfo, perSqlInfos,
        unSupportedExecs, unSupportedExprs, clusterTags, allClusterTagsMap, mlFunctions,
        mlTotalStageDuration, unsupportedExecExprsMap, clusterSummary)
    }
  }

  def getPerSQLWallClockSummary(sqlStageSums: Seq[SQLStageSummary], sqlDataFrameDuration: Long,
      hasFailures: Boolean, appName: String): EstimatedAppInfo = {
    val allStagesSummary = sqlStageSums.flatMap(_.stageSum)
    val sqlDataframeTaskDuration = allStagesSummary.map(_.stageTaskTime).sum
    val supportedSQLTaskDuration = calculateSQLSupportedTaskDuration(allStagesSummary)
    val taskSpeedupFactor = calculateSpeedupFactor(allStagesSummary)

    // get the ratio based on the Task durations that we will use for wall clock durations
    val estimatedGPURatio = if (sqlDataframeTaskDuration > 0) {
      supportedSQLTaskDuration.toDouble / sqlDataframeTaskDuration.toDouble
    } else {
      1
    }
    // reusing the same function here (calculateEstimatedInfoSummary) as the app level,
    // there is no app duration so just set it to sqlDataFrameDuration
    QualificationAppInfo.calculateEstimatedInfoSummary(estimatedGPURatio,
      sqlDataFrameDuration, sqlDataFrameDuration, taskSpeedupFactor, appName,
      appId, hasFailures)
  }

  private def getAllSQLDurations: Seq[Long] = {
    sqlIdToInfo.flatMap { case (_, info) =>
      info.rootExecutionID match {
        // We return the duration if sqlId doesn't have a rootExecutionID or if the rootExecutionID
        // is the same as the sqlId. In some cases, the child completes the execution before the
        // parent, so we need to check if the child's duration is within the parent's duration.
        // We add the child's duration to the parent's duration if it's not within the parent's
        // duration.
        case Some(rootExecutionID) if rootExecutionID != info.sqlID =>
          sqlIdToInfo.get(rootExecutionID).flatMap { rootExecutionInfo =>
            val rootExecutionStartTime = rootExecutionInfo.startTime
            val rootExecutionEndTime = rootExecutionInfo.endTime.getOrElse(0L)
            val sqlStartTime = info.startTime
            val sqlEndTime = info.endTime.getOrElse(0L)
            //  Below check will be true if the child is not completely inside the root.
            //  Nevertheless we still account for its total duration and not the overlap.
            if (sqlStartTime < rootExecutionStartTime || sqlEndTime > rootExecutionEndTime) {
              Some(info.duration.getOrElse(0L))
            } else {
              Some(0L)
            }
          }
        case _ =>
          Some(info.duration.getOrElse(0L))
      }
    }.toSeq
  }

  private def getMlFuntions: Option[Seq[MLFunctions]] = {
    val mlFunctions = stageIdToInfo.flatMap { case ((appId, _), stageInfo) =>
      checkMLOps(appId, stageInfo)
    }
    if (mlFunctions.nonEmpty) {
      Some(mlFunctions.toSeq.sortBy(mlops => mlops.stageId))
    } else {
      None
    }
  }

  private def getMlTotalStageDuration(
      mlFunctions: Seq[MLFunctions]): Option[Seq[MLFuncsStageDuration]] = {
    // Get ML function names, durations and stage Id's
    val mlFuncs = mlFunctions.map(mlFun => (mlFun.mlOps, mlFun.duration, mlFun.stageId))
    // Check if the ML function is supported on GPU, if so then save it as corresonding simple
    // function name along with it's duration and stage Id.
    // Example: (org.apache.spark.ml.feature.PCA.fit, 200) becomes (PCA, 200)
    val supportedMlFuncsStats = SupportedMLFuncsName.funcName.map(
      supportedMlfunc => mlFuncs.filter(mlfunc =>
        mlfunc._1.contains(supportedMlfunc._1)).map(
        x => (SupportedMLFuncsName.funcName(supportedMlfunc._1), x._2, x._3))).flatten

    // group by ML function name, capture it's stageId and add duration of corresponding functions
    val mlFuncsResult = supportedMlFuncsStats.groupBy(mlfunc => mlfunc._1).map(
      mlfunc => (mlfunc._1, mlfunc._2.map(
        duration => duration._2).sum, mlfunc._2.map(stageId => stageId._3)))
    if (mlFuncsResult.nonEmpty) {
      Some(mlFuncsResult.map(
        result => MLFuncsStageDuration(result._1, result._2, result._3.toArray)).toSeq)
    } else {
      None
    }
  }

  private def getMlSpeedUp(mlTotalStageDuration: Seq[MLFuncsStageDuration],
      mlEventlogType: String): Option[MLFuncsSpeedupAndDuration] = {
    val mlFuncAndDuration = mlTotalStageDuration.map(x => (x.mlFuncName, x.duration))

    val speedupFactors = mlFuncAndDuration.map(
      mlFunc => mlFunc._1).map(mlFuncName => {
      // speedup of pyspark and scala are different
      val mlFuncNameWithType = s"${mlFuncName}-${mlEventlogType}"
      pluginTypeChecker.getSpeedupFactor(mlFuncNameWithType)
    })
    val avgMlSpeedup = SQLPlanParser.averageSpeedup(speedupFactors)
    // return None if the average speedup < 1. If it's less than 1, then running it on GPU is
    // not recommended.
    if (avgMlSpeedup >= 1.0) {
      val mlFuncDuration = mlFuncAndDuration.map(mlFunc => mlFunc._2).sum
      Some(MLFuncsSpeedupAndDuration(avgMlSpeedup, mlFuncDuration))
    } else {
      None
    }
  }

  private[qualification] def processSQLPlan(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    val sqlPlanInfoGraphEntry = SqlPlanInfoGraphBuffer.createEntry(sqlID, planInfo)
    checkMetadataForReadSchema(sqlPlanInfoGraphEntry)
    for (node <- sqlPlanInfoGraphEntry.sparkPlanGraph.allNodes) {
      checkGraphNodeForReads(sqlID, node)
      val issues = findPotentialIssues(node.desc)
      if (issues.nonEmpty) {
        val existingIssues = sqlIDtoProblematic.getOrElse(sqlID, Set.empty[String])
        sqlIDtoProblematic(sqlID) = existingIssues ++ issues
      }
      // Get the write data format
      if (!perSqlOnly) {
        DataWritingCommandExecParser.getWriteCMDWrapper(node).map { wWrapper =>
          writeDataFormat += wWrapper.dataFormat
        }
      }
    }
  }

  private def writeFormatNotSupported(writeFormat: ArrayBuffer[String]): Seq[String] = {
    // Filter unsupported write data format
    val unSupportedWriteFormat = pluginTypeChecker.isWriteFormatSupported(writeFormat)
    unSupportedWriteFormat.distinct
  }
}

// Estimate based on wall clock times
case class EstimatedAppInfo(
    appName: String,
    appId: String,
    appDur: Long,
    sqlDfDuration: Long,
    gpuOpportunity: Long, // Projected opportunity for acceleration on GPU in ms
    estimatedGpuDur: Double, // Predicted runtime for the app if it was run on the GPU
    estimatedGpuSpeedup: Double, // app_duration / estimated_gpu_duration
    estimatedGpuTimeSaved: Double, // app_duration - estimated_gpu_duration
    recommendation: String,
    unsupportedExecs: String,
    unsupportedExprs: String,
    allTagsMap: Map[String, String])

// Used by writers, estimated app summary with estimated frequency
case class EstimatedSummaryInfo(
    estimatedInfo: EstimatedAppInfo,
    estimatedFrequency: Long = DEFAULT_JOB_FREQUENCY)

// Estimate based on wall clock times for each SQL query
case class EstimatedPerSQLSummaryInfo(
    sqlID: Long,
    sqlDesc: String,
    info: EstimatedAppInfo)

case class SQLStageSummary(
    stageSum: Set[StageQualSummaryInfo],
    sqlID: Long,
    estimateWallClockSupported: Long,
    execCPUTime: Long,
    execRunTime: Long)

case class MLFunctions(
    appID: Option[String],
    stageId: Int,
    mlOps: Array[String],
    duration: Long
)

case class MLFuncsStageDuration(
    mlFuncName: String,
    duration: Long,
    stageIds: Array[Int]
)

case class MLFuncsSpeedupAndDuration(
    averageSpeedup: Double,
    duration: Long
)

class StageTaskQualificationSummary(
    val stageId: Int,
    val stageAttemptId: Int,
    var executorRunTime: Long,
    var executorCPUTime: Long,
    var totalTaskDuration: Long,
    var totalbytesRead: Long)

case class QualApplicationInfo(
    appName: String,
    appId: Option[String],
    startTime: Long,
    sparkUser: String,
    endTime: Option[Long], // time in ms
    duration: Option[Long],
    endDurationEstimated: Boolean)

case class QualSQLExecutionInfo(
    sqlID: Long,
    startTime: Long,
    endTime: Option[Long],
    duration: Option[Long],
    durationStr: String,
    sqlQualDuration: Option[Long],
    hasDataset: Boolean,
    problematic: String = "")

// Case class representing status summary information for a particular application.
case class StatusSummaryInfo(
    path: String,
    status: String,
    appId: String = "",
    message: String = "")

case class QualificationSummaryInfo(
    appName: String,
    appId: String,
    potentialProblems: Seq[String],
    executorCpuTimePercent: Double,
    endDurationEstimated: Boolean,
    failedSQLIds: Seq[String],
    readFileFormatAndTypesNotSupported: Seq[String],
    readFileFormats: Seq[String],
    writeDataFormat: Seq[String],
    complexTypes: Seq[String],
    nestedComplexTypes: Seq[String],
    longestSqlDuration: Long,
    sqlDataframeTaskDuration: Long,
    nonSqlTaskDurationAndOverhead: Long,
    unsupportedSQLTaskDuration: Long,
    supportedSQLTaskDuration: Long,
    taskSpeedupFactor: Double,
    user: String,
    startTime: Long,
    sparkSqlDFWallClockDuration: Long,
    planInfo: Seq[PlanInfo],
    origPlanStageInfo: Seq[StageQualSummaryInfo],
    stageInfo: Seq[StageQualSummaryInfo],
    estimatedInfo: EstimatedAppInfo,
    perSQLEstimatedInfo: Option[Seq[EstimatedPerSQLSummaryInfo]],
    unSupportedExecs: String,
    unSupportedExprs: String,
    clusterTags: String,
    allClusterTagsMap: Map[String, String],
    mlFunctions: Option[Seq[MLFunctions]],
    mlFunctionsStageDurations: Option[Seq[MLFuncsStageDuration]],
    unsupportedExecstoExprsMap: Map[String, String],
    clusterSummary: ClusterSummary,
    estimatedFrequency: Option[Long] = None)

case class StageQualSummaryInfo(
    stageId: Int,
    averageSpeedup: Double,
    stageTaskTime: Long,
    unsupportedTaskDur: Long,
    numTransitions: Int,
    transitionTime: Long,
    estimated: Boolean = false,
    stageWallclockDuration: Long = 0,
    unsupportedExecs: Seq[ExecInfo] = Seq.empty)

object QualificationAppInfo extends Logging {
  // define recommendation constants
  val RECOMMENDED = "Recommended"
  val NOT_RECOMMENDED = "Not Recommended"
  val STRONGLY_RECOMMENDED = "Strongly Recommended"
  val NOT_APPLICABLE = "Not Applicable"
  val LOWER_BOUND_RECOMMENDED = 1.3
  val LOWER_BOUND_STRONGLY_RECOMMENDED = 2.5
  // Below is the total time taken whenever there are ColumnarToRow or RowToColumnar transitions
  // This includes the time taken to convert the data from one format to another and the time taken
  // to transfer the data from CPU to GPU and vice versa. Current transfer rate is 1GB/s and is
  // based on the testing on few candidate eventlogs.
  val CPU_GPU_TRANSFER_RATE = 1000000000L

  private def handleException(e: Exception, path: EventLogInfo): String = {
    val message: String = e match {
      case gpuLog: GpuEventLogException =>
        gpuLog.message
      case _: com.fasterxml.jackson.core.JsonParseException =>
        s"Error parsing JSON: ${path.eventLog.toString}"
      case _: IllegalArgumentException =>
        s"Error parsing file: ${path.eventLog.toString}"
      case _: Exception =>
        // catch all exceptions and skip that file
        s"Got unexpected exception processing file: ${path.eventLog.toString}"
    }

    s"${e.getClass.getSimpleName}: $message"
  }

  def getRecommendation(totalSpeedup: Double,
      hasFailures: Boolean): String = {
    if (hasFailures) {
      NOT_APPLICABLE
    } else if (totalSpeedup >= LOWER_BOUND_STRONGLY_RECOMMENDED) {
      STRONGLY_RECOMMENDED
    } else if (totalSpeedup >= LOWER_BOUND_RECOMMENDED) {
      RECOMMENDED
    } else {
      NOT_RECOMMENDED
    }
  }

  def wallClockSqlDataFrameToUse(sqlDataFrameDuration: Long, appDuration: Long): Long = {
    // If our app duration is shorter than our sql duration, estimate the sql duration down
    // to app duration
    math.min(sqlDataFrameDuration, appDuration)
  }

  // Summarize and estimate based on wall clock times
  def calculateEstimatedInfoSummary(estimatedRatio: Double, sqlDataFrameDuration: Long,
      appDuration: Long, sqlSpeedupFactor: Double, appName: String, appId: String,
      hasFailures: Boolean, mlSpeedupFactor: Option[MLFuncsSpeedupAndDuration] = None,
      unsupportedExecs: String = "", unsupportedExprs: String = "",
      allClusterTagsMap: Map[String, String] = Map.empty[String, String]): EstimatedAppInfo = {
    val sqlDataFrameDurationToUse = wallClockSqlDataFrameToUse(sqlDataFrameDuration, appDuration)

    // get the average speedup and duration for ML funcs supported on GPU
    val (mlSpeedup, mlDuration) = if (mlSpeedupFactor.isDefined) {
      val speedUp = mlSpeedupFactor.get.averageSpeedup
      val duration = mlSpeedupFactor.get.duration
      (speedUp, duration.toDouble)
    } else {
      (1.0, 0.0)
    }

    val speedUpOpportunitySQL = sqlDataFrameDurationToUse * estimatedRatio
    val speedupOpportunityWallClock = speedUpOpportunitySQL + mlDuration
    val estimated_wall_clock_dur_not_on_gpu = appDuration - speedupOpportunityWallClock
    val estimated_gpu_duration = (speedUpOpportunitySQL / sqlSpeedupFactor) +
      estimated_wall_clock_dur_not_on_gpu + (mlDuration / mlSpeedup)
    val estimated_gpu_speedup = if (appDuration == 0 || estimated_gpu_duration == 0) {
      0
    } else {
      appDuration / estimated_gpu_duration
    }
    val estimated_gpu_timesaved = appDuration - estimated_gpu_duration
    val recommendation = getRecommendation(estimated_gpu_speedup, hasFailures)

    // truncate the double fields to double precision to ensure that unit-tests do not explicitly
    // set the format to match the output. Removing the truncation from here requires modifying
    // TestQualificationSummary to truncate the same fields to match the CSV static samples.
    EstimatedAppInfo(appName, appId, appDuration,
      sqlDataFrameDurationToUse,
      speedupOpportunityWallClock.toLong,
      estimated_gpu_duration,
      estimated_gpu_speedup,
      estimated_gpu_timesaved,
      recommendation,
      unsupportedExecs,
      unsupportedExprs,
      allClusterTagsMap)
  }

  /**
   * Create a QualificationAppInfo object based on the provided parameters.
   *
   * @return Either a Right with the created QualificationAppInfo if successful,
   *         or a Left with an error message if an exception occurs during creation.
   */
  def createApp(
      path: EventLogInfo,
      hadoopConf: Configuration,
      pluginTypeChecker: PluginTypeChecker,
      reportSqlLevel: Boolean,
      mlOpsEnabled: Boolean,
      penalizeTransitions: Boolean): Either[String, QualificationAppInfo] = {
    try {
        val app = new QualificationAppInfo(Some(path), Some(hadoopConf), pluginTypeChecker,
          reportSqlLevel, false, mlOpsEnabled, penalizeTransitions)
        logInfo(s"${path.eventLog.toString} has App: ${app.appId}")
        Right(app)
      } catch {
        case e: Exception =>
          Left(handleException(e, path))
      }
  }
}
