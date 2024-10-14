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

package com.nvidia.spark.rapids.tool.analysis

import scala.collection.mutable.{AbstractSet, ArrayBuffer, HashMap, LinkedHashSet}

import com.nvidia.spark.rapids.tool.planparser.SQLPlanParser
import com.nvidia.spark.rapids.tool.profiling.{AccumProfileResults, SQLAccumProfileResults, SQLMetricInfoCase, SQLStageInfoProfileResult, UnsupportedSQLPlan, WholeStageCodeGenResults}
import com.nvidia.spark.rapids.tool.qualification.QualSQLPlanAnalyzer

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphNode}
import org.apache.spark.sql.rapids.tool.{AppBase, RDDCheckHelper, SqlPlanInfoGraphBuffer, SqlPlanInfoGraphEntry}
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo
import org.apache.spark.sql.rapids.tool.store.DataSourceRecord
import org.apache.spark.sql.rapids.tool.util.ToolsPlanGraph

/**
 * This class processes SQL plan to build some information such as: metrics, wholeStage nodes, and
 * connecting operators to nodes. The implementation used to be directly under Profiler's
 * ApplicationInfo class. Moving the logic and the data structure to this new class is part of
 * refactor to have a SQLPlan processor that can produce the same analysis for both the Prof/Qual
 * tools.
 * Calling processSQLPlanMetrics() has a side effect on the app object:
 * 1- it updates dataSourceInfo with V2 and V1 data sources
 * 2- it updates sqlIDtoProblematic the map between SQL ID and potential problems
 * 3- it updates sqlIdToInfo.DsOrRdd as boolean to indicate whether a sql is an RDD/DS or not
 * TODO: this class should extend the trait SparkSQLPlanInfoVisitor[T]
 * @param app the Application info objects that contains the SQL plans to be processed
 */
class AppSQLPlanAnalyzer(app: AppBase, appIndex: Int) extends AppAnalysisBase(app) {
  // A map between (SQL ID, Node ID) and the set of stage IDs
  // TODO: The Qualification should use this map instead of building a new set for each exec.
  private val sqlPlanNodeIdToStageIds: HashMap[(Long, Long), Set[Int]] =
    HashMap.empty[(Long, Long), Set[Int]]
  var wholeStage: ArrayBuffer[WholeStageCodeGenResults] = ArrayBuffer[WholeStageCodeGenResults]()
  // A list of UnsupportedSQLPlan that contains the SQL ID, node ID, node name.
  // TODO: for now, unsupportedSQLPlan is kept here for now to match the legacy Profiler's
  //      output but we need to revisit this in the future to see if we can move it to a
  //      different place or fix any inconsistent issues between this implementation and
  //      SQLPlanParser.
  var unsupportedSQLPlan: ArrayBuffer[UnsupportedSQLPlan] = ArrayBuffer[UnsupportedSQLPlan]()
  var allSQLMetrics: ArrayBuffer[SQLMetricInfoCase] = ArrayBuffer[SQLMetricInfoCase]()

  val stageToNodeNames: HashMap[Long, Seq[String]] = HashMap.empty[Long, Seq[String]]
  val stageToGpuSemaphoreWaitTime: HashMap[Long, Long] = HashMap.empty[Long, Long]

  /**
   * Connects Operators to Stages using AccumulatorIDs.
   * TODO: This function can be fused in the visitNode function to avoid the extra iteration.
   *
   * @param cb function that creates a SparkPlanGraph. This can be used as a cacheHolder for the
   *           object created to be used later.
   */
  private def connectOperatorToStage(cb: (Long, SparkPlanInfo) => SparkPlanGraph): Unit = {
    for ((sqlId, planInfo) <- app.sqlPlans) {
      val planGraph: SparkPlanGraph = cb.apply(sqlId, planInfo)
      // Maps stages to operators by checking for non-zero intersection
      // between nodeMetrics and stageAccumulateIDs
      val nodeIdToStage = planGraph.allNodes.map { node =>
        val mappedStages = SQLPlanParser.getStagesInSQLNode(node, app)
        ((sqlId, node.id), mappedStages)
      }.toMap
      sqlPlanNodeIdToStageIds ++= nodeIdToStage
    }
  }

  /**
   * Update the potential problems in the app object. This is a common function that is used by
   * Both Qual/Prof analysis.
   * For Qual apps, the app.sqlIDtoProblematic won't be set because it is done later during the
   * aggregation phase.
   *
   * @param sqlId             the SQL ID being analyzed
   * @param potentialProblems a set of strings that represent the potential problems found in the
   *                          SQL plan.
   */
  private def updateAppPotentialProblems(sqlId: Long,
      potentialProblems: AbstractSet[String]): Unit = {
    // Append problematic issues to the global variable for that SqlID
    val existingIssues = app.sqlIDtoProblematic.getOrElse(sqlId, LinkedHashSet[String]())
    app.sqlIDtoProblematic(sqlId) = existingIssues ++ potentialProblems
  }

  // A visitor context to hold the state of the SQL plan visitor.
  // The fields are updated and modified by the visitNode function.
  // sqlIsDsOrRDD is initialized to False, and it is set only once to True when a node is detected
  // as RDD or DS.
  protected case class SQLPlanVisitorContext(
      sqlPIGEntry: SqlPlanInfoGraphEntry,
      sqlDataSources: ArrayBuffer[DataSourceRecord] = ArrayBuffer[DataSourceRecord](),
      potentialProblems: LinkedHashSet[String] = LinkedHashSet[String](),
      var sqlIsDsOrRDD: Boolean = false)

  /**
   * The method is called for each node in the SparkGraph plan.
   * It visits the node to extract the following information
   * 1- the metrics;
   * 2- potential problems.
   * 3- data sources
   *
   * It updates the following fields defined in AppSQLPlanAnalyzer:
   * 1- allSQLMetrics: a list of SQLMetricInfoCase
   * 2- wholeStage: a list of WholeStageCodeGenResults
   * 3- unsupportedSQLPlan: a list of UnsupportedSQLPlan that contains the SQL ID, node ID,
   * node name.
   * TODO: Consider handling the construction of this list in a different way for the
   * Qualification
   * 4- sqlPlanNodeIdToStageIds: A map between (SQL ID, Node ID) and the set of stage IDs
   *
   * It has the following effect on the visitor object:
   * 1- It updates the sqlIsDsOrRDD argument to True when the visited node is an RDD or Dataset.
   * 2- If the SQLID is an RDD, the potentialProblems is cleared because once SQL is marked as RDD,
   * all the other problems are ignored. Note that we need to set that flag only once to True
   * for the given SQLID.
   * 3- It appends the current node's potential problems to the SQLID problems only if the SQL is
   * visitor.sqlIsDsOrRDD is False. Otherwise, it is kind of redundant to keep checking for
   * potential problems for every node when they get to be ignored.
   *
   * It has the following effect on the app object:
   * 1- it updates dataSourceInfo with V2 and V1 data sources
   * 2- it updates sqlIDtoProblematic the map between SQL ID and potential problems
   *
   * @param visitor the visitor context defined per SQLPlan
   * @param node    the node being currently visited.
   */
  protected def visitNode(visitor: SQLPlanVisitorContext,
      node: SparkPlanGraphNode): Unit = {
    node match {
      case cluster: SparkPlanGraphCluster =>
        val ch = cluster.nodes
        ch.foreach { c =>
          wholeStage += WholeStageCodeGenResults(
            appIndex, visitor.sqlPIGEntry.sqlID, node.id, node.name, c.name, c.id)
        }
      case _ =>
    }
    // get V2 dataSources for that node
    val nodeV2Reads = app.checkGraphNodeForReads(visitor.sqlPIGEntry.sqlID, node)
    if (nodeV2Reads.isDefined) {
      visitor.sqlDataSources += nodeV2Reads.get
    }

    val nodeIsDsOrRDD = RDDCheckHelper.isDatasetOrRDDPlan(node.name, node.desc).isRDD
    if (nodeIsDsOrRDD) {
      // we want to report every node that is an RDD
      val thisPlan = UnsupportedSQLPlan(visitor.sqlPIGEntry.sqlID, node.id, node.name, node.desc,
        "Contains Dataset or RDD")
      unsupportedSQLPlan += thisPlan
      // If one node is RDD, the Sql should be set too
      if (!visitor.sqlIsDsOrRDD) { // We need to set the flag only once for the given sqlID
        visitor.sqlIsDsOrRDD = true
        app.sqlIdToInfo.get(visitor.sqlPIGEntry.sqlID).foreach { sql =>
          sql.setDsOrRdd(visitor.sqlIsDsOrRDD)
          app.sqlIDToDataSetOrRDDCase += visitor.sqlPIGEntry.sqlID
          // Clear the potential problems since it is an RDD to free memory
          visitor.potentialProblems.clear()
        }
      }
    }
    if (!visitor.sqlIsDsOrRDD) {
      // Append current node's potential problems to the Sql problems only if the SQL is not an
      // RDD. This is an optimization since the potentialProblems won't be used any more.
      visitor.potentialProblems ++= app.findPotentialIssues(node.desc)
    }
    // Then process SQL plan metric type
    for (metric <- node.metrics) {
      val stages =
        sqlPlanNodeIdToStageIds.getOrElse((visitor.sqlPIGEntry.sqlID, node.id), Set.empty)
      val allMetric = SQLMetricInfoCase(visitor.sqlPIGEntry.sqlID, metric.name,
        metric.accumulatorId, metric.metricType, node.id, node.name, node.desc, stages)

      allSQLMetrics += allMetric
      if (app.sqlPlanMetricsAdaptive.nonEmpty) {
        val adaptive = app.sqlPlanMetricsAdaptive.filter { adaptiveMetric =>
          adaptiveMetric.sqlID == visitor.sqlPIGEntry.sqlID &&
            adaptiveMetric.accumulatorId == metric.accumulatorId
        }
        adaptive.foreach { adaptiveMetric =>
          val allMetric = SQLMetricInfoCase(visitor.sqlPIGEntry.sqlID, adaptiveMetric.name,
            adaptiveMetric.accumulatorId, adaptiveMetric.metricType, node.id,
            node.name, node.desc, stages)
          // could make this more efficient but seems ok for now
          val exists = allSQLMetrics.filter { a =>
            ((a.accumulatorId == adaptiveMetric.accumulatorId)
              && (a.sqlID == visitor.sqlPIGEntry.sqlID)
              && (a.nodeID == node.id && adaptiveMetric.metricType == a.metricType))
          }
          if (exists.isEmpty) {
            allSQLMetrics += allMetric
          }
        }
      }
    }
  }

  /**
   * Function to process SQL Plan Metrics after all events are processed
   */
  def processSQLPlanMetrics(): Unit = {
    // Define a buffer to cache the SQLPlanInfoGraphs
    val sqlPlanInfoBuffer = SqlPlanInfoGraphBuffer()
    // Define a function used to fill in the buffer while executing "connectOperatorToStage"
    val createGraphFunc = (sqlId: Long, planInfo: SparkPlanInfo) => {
      sqlPlanInfoBuffer.addSqlPlanInfoGraph(sqlId, planInfo).sparkPlanGraph
    }
    connectOperatorToStage(createGraphFunc)
    for (sqlPIGEntry <- sqlPlanInfoBuffer.sqlPlanInfoGraphs) {
      // store all dataSources of the given SQL in a variable so that we won't have to iterate
      // through the entire list
      // get V1 dataSources for that SQLId
      val visitorContext = SQLPlanVisitorContext(sqlPIGEntry,
        app.checkMetadataForReadSchema(sqlPIGEntry))
      for (node <- sqlPIGEntry.sparkPlanGraph.allNodes) {
        visitNode(visitorContext, node)
      }
      if (visitorContext.sqlDataSources.nonEmpty) {
        val sqlNestedComplexTypes =
          AppBase.parseReadSchemaForNestedTypes(
            visitorContext.sqlDataSources.map { ds => ds.schema })
        // Append problematic issues to the global variable for that SqlID
        if (sqlNestedComplexTypes._2.nonEmpty) {
          visitorContext.potentialProblems += "NESTED COMPLEX TYPE"
        }
      }
      // Finally, update the potential problems in the app object
      // Note that the implementation depends on teh type of the AppBase
      if (visitorContext.potentialProblems.nonEmpty) {
        updateAppPotentialProblems(sqlPIGEntry.sqlID, visitorContext.potentialProblems)
      }
    }
  }

  def aggregateSQLStageInfo: Seq[SQLStageInfoProfileResult] = {
    val jobsWithSQL = app.jobIdToInfo.filter { case (_, j) =>
      j.sqlID.nonEmpty
    }
    val sqlToStages = jobsWithSQL.flatMap { case (jobId, j) =>
      val stages = j.stageIds
      val stagesInJob = app.stageManager.getStagesByIds(stages)
      stagesInJob.map { sModel =>
        val nodeIds = sqlPlanNodeIdToStageIds.filter { case (_, v) =>
          v.contains(sModel.stageInfo.stageId)
        }.keys.toSeq
        val nodeNames = app.sqlManager.applyToPlanInfo(j.sqlID.get) { planInfo =>
          val nodes = ToolsPlanGraph(planInfo).allNodes
          val validNodes = nodes.filter { n =>
            nodeIds.contains((j.sqlID.get, n.id))
          }
          validNodes.map(n => s"${n.name}(${n.id.toString})")
        }.getOrElse(Seq.empty)
        stageToNodeNames(sModel.stageInfo.stageId) = nodeNames
        SQLStageInfoProfileResult(appIndex, j.sqlID.get, jobId, sModel.stageInfo.stageId,
          sModel.stageInfo.attemptNumber(), sModel.duration, nodeNames)
      }
    }
    sqlToStages.toSeq
  }

  def generateSQLAccums(): Seq[SQLAccumProfileResults] = {
    allSQLMetrics.flatMap { metric =>
      val accumTaskStats = app.accumManager.calculateAccStats(metric.accumulatorId)
      // local mode driver gets updates
      val driverAccumsOpt = app.driverAccumMap.get(metric.accumulatorId)
      val driverMax = driverAccumsOpt match {
        case Some(accums) =>
          val filtered = accums.filter { a =>
            a.sqlID == metric.sqlID
          }
          val accumValues = filtered.map(_.value).sortWith(_ < _)
          if (accumValues.isEmpty) {
            None
          } else if (accumValues.length <= 1) {
            Some(StatisticsMetrics(0L, 0L, 0L, accumValues.sum))
          } else {
            Some(StatisticsMetrics(accumValues(0), accumValues(accumValues.size / 2),
              accumValues(accumValues.size - 1), accumValues.sum))
          }
        case None =>
          None
      }

      if (accumTaskStats.isDefined || driverMax.isDefined) {
        val taskInfo = accumTaskStats.getOrElse(StatisticsMetrics.ZERO_RECORD)
        val driverInfo = driverMax.getOrElse(StatisticsMetrics.ZERO_RECORD)

        val max = Math.max(taskInfo.max, driverInfo.max)
        val min = Math.max(taskInfo.min, driverInfo.min)
        val med = Math.max(taskInfo.med, driverInfo.med)
        val total = Math.max(taskInfo.total, driverInfo.total)

        // TODO - use this to create view 2
        Some(SQLAccumProfileResults(appIndex, metric.sqlID,
          metric.nodeID, metric.nodeName, metric.accumulatorId, metric.name,
          min, med, max, total, metric.metricType, metric.stageIds.mkString(",")))
      } else {
        None
      }
    }
  }

  /**
   * Generate the stage level metrics for the SQL plan including GPU metrics if applicable.
   * Along with Spark defined metrics, below is the list of GPU metrics that are collected if they
   * are present in the eventlog:
   * gpuSemaphoreWait, gpuRetryCount, gpuSplitAndRetryCount, gpuRetryBlockTime,
   * gpuRetryComputationTime, gpuSpillToHostTime, gpuSpillToDiskTime, gpuReadSpillFromHostTime,
   * gpuReadSpillFromDiskTime
   *
   * @return a sequence of AccumProfileResults
   */
  def generateStageLevelAccums(): Seq[AccumProfileResults] = {
    app.accumManager.accumInfoMap.flatMap { accumMapEntry =>
      val accumInfo = accumMapEntry._2
      accumInfo.stageValuesMap.keySet.flatMap( stageId => {
        val stageTaskIds = app.taskManager.getAllTasksStageAttempt(stageId).map(_.taskId).toSet
        // get the task updates that belong to that stage
        val taskUpatesSubset =
          accumInfo.taskUpdatesMap.filterKeys(stageTaskIds.contains).values.toSeq.sorted
        if (taskUpatesSubset.isEmpty) {
          None
        } else {
          val min = taskUpatesSubset.head
          val max = taskUpatesSubset.last
          val sum = taskUpatesSubset.sum
          val median = if (taskUpatesSubset.size % 2 == 0) {
            val mid = taskUpatesSubset.size / 2
            (taskUpatesSubset(mid) + taskUpatesSubset(mid - 1)) / 2
          } else {
            taskUpatesSubset(taskUpatesSubset.size / 2)
          }
          if (accumInfo.infoRef.getName.contains("gpuSemaphoreWait")) {
            stageToGpuSemaphoreWaitTime(stageId) = sum
          }
          Some(AccumProfileResults(
            appIndex,
            stageId,
            accumInfo.infoRef,
            min = min,
            median = median,
            max = max,
            total = sum))
        }
      })
    }
  }.toSeq
}

object AppSQLPlanAnalyzer {
  def apply(app: AppBase, appIndex: Integer = 1): AppSQLPlanAnalyzer = {
    val sqlAnalyzer = app match {
      case qApp: QualificationAppInfo =>
        new QualSQLPlanAnalyzer(qApp, appIndex)
      case pApp: ApplicationInfo =>
        new AppSQLPlanAnalyzer(pApp, pApp.index)
    }
    sqlAnalyzer.processSQLPlanMetrics()
    sqlAnalyzer
  }
}
