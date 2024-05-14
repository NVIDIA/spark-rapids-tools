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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.planparser.SQLPlanParser
import com.nvidia.spark.rapids.tool.profiling.{SQLAccumProfileResults, SQLMetricInfoCase, SQLStageInfoProfileResult, UnsupportedSQLPlan, WholeStageCodeGenResults}

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.{AppBase, RDDCheckHelper, SQLMetricsStats, SqlPlanInfoGraphBuffer, ToolUtils}
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.util.ToolsPlanGraph

/**
 * This class processes SQL plan to build some information such as: metrics, wholestage nodes, and
 * connecting operators to nodes. The implementation used to be directly under profiler's
 * ApplicationInfo class. Moving the logic and the data structure to this new class is part of
 * refactor to have a SQLPlan processor that can produce the same analysis for both the Prof/Qual
 * tools.
 * TODO: 1- Make the processor accepts appBase instead of applicationInfo. The tricky part here
 *       that Qual has its own way of reporting Problematic SQLs and identifying RDDs.
 *       2- Restructure the implementation similar to AppSparkMetricsAnalysis to separate between
 *       analysis and views.
 * @param app the Application into objects that contains the SQL plans to be processed
 */
class AppSQLPlanAnalyzer(app: AppBase, appIndex: Int) extends AppAnalysisBase(app) {
  private val sqlPlanNodeIdToStageIds: mutable.HashMap[(Long, Long), Set[Int]] =
    mutable.HashMap.empty[(Long, Long), Set[Int]]
  var wholeStage: ArrayBuffer[WholeStageCodeGenResults] = ArrayBuffer[WholeStageCodeGenResults]()
  var unsupportedSQLPlan: ArrayBuffer[UnsupportedSQLPlan] = ArrayBuffer[UnsupportedSQLPlan]()
  var allSQLMetrics: ArrayBuffer[SQLMetricInfoCase] = ArrayBuffer[SQLMetricInfoCase]()

  /**
   * Connects Operators to Stages using AccumulatorIDs
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
      var sqlIsDsOrRDD = false
      val potentialProblems = collection.mutable.Set[String]()
      // store all datasources of the given SQL in a variable so that we won't have to iterate
      // through the entire list
      // get V1 dataSources for that SQLId
      val sqlDataSources = app.checkMetadataForReadSchema(sqlPIGEntry)
      for (node <- sqlPIGEntry.sparkPlanGraph.allNodes) {
        var nodeIsDsOrRDD = false
        if (node.isInstanceOf[org.apache.spark.sql.execution.ui.SparkPlanGraphCluster]) {
          val ch = node.asInstanceOf[org.apache.spark.sql.execution.ui.SparkPlanGraphCluster].nodes
          ch.foreach { c =>
            wholeStage += WholeStageCodeGenResults(
              appIndex, sqlPIGEntry.sqlID, node.id, node.name, c.name, c.id)
          }
        }
        // get V2 dataSources for that node
        val nodeV2Reads = app.checkGraphNodeForReads(sqlPIGEntry.sqlID, node)
        if (nodeV2Reads.isDefined) {
          sqlDataSources += nodeV2Reads.get
        }
        nodeIsDsOrRDD = RDDCheckHelper.isDatasetOrRDDPlan(node.name, node.desc).isRDD
        if (nodeIsDsOrRDD) {
          if (app.gpuMode) { // we want to report every node that is an RDD
            val thisPlan = UnsupportedSQLPlan(sqlPIGEntry.sqlID, node.id, node.name, node.desc,
              "Contains Dataset or RDD")
            unsupportedSQLPlan += thisPlan
          }
          // If one node is RDD, the Sql should be set too
          if (!sqlIsDsOrRDD) { // We need to set the flag only once for the given sqlID
            sqlIsDsOrRDD = true
            app.sqlIdToInfo.get(sqlPIGEntry.sqlID).foreach { sql =>
              sql.setDsOrRdd(sqlIsDsOrRDD)
              app.sqlIDToDataSetOrRDDCase += sqlPIGEntry.sqlID
              // Clear the potential problems since it is an RDD to free memory
              potentialProblems.clear()
            }
          }
        }
        if (!sqlIsDsOrRDD) {
          // Append current node's potential problems to the Sql problems only if the SQL is not an
          // RDD. This is an optimization since the potentialProblems won't be used any more.
          potentialProblems ++= app.findPotentialIssues(node.desc)
        }
        // Then process SQL plan metric type
        for (metric <- node.metrics) {
          val stages =
            sqlPlanNodeIdToStageIds.get((sqlPIGEntry.sqlID, node.id)).getOrElse(Set.empty)
          val allMetric = SQLMetricInfoCase(sqlPIGEntry.sqlID, metric.name,
            metric.accumulatorId, metric.metricType, node.id,
            node.name, node.desc, stages)

          allSQLMetrics += allMetric
          if (app.sqlPlanMetricsAdaptive.nonEmpty) {
            val adaptive = app.sqlPlanMetricsAdaptive.filter { adaptiveMetric =>
              adaptiveMetric.sqlID == sqlPIGEntry.sqlID &&
                adaptiveMetric.accumulatorId == metric.accumulatorId
            }
            adaptive.foreach { adaptiveMetric =>
              val allMetric = SQLMetricInfoCase(sqlPIGEntry.sqlID, adaptiveMetric.name,
                adaptiveMetric.accumulatorId, adaptiveMetric.metricType, node.id,
                node.name, node.desc, stages)
              // could make this more efficient but seems ok for now
              val exists = allSQLMetrics.filter { a =>
                ((a.accumulatorId == adaptiveMetric.accumulatorId) && (a.sqlID == sqlPIGEntry.sqlID)
                  && (a.nodeID == node.id && adaptiveMetric.metricType == a.metricType))
              }
              if (exists.isEmpty) {
                allSQLMetrics += allMetric
              }
            }
          }
        }
      }
      if (app.isInstanceOf[ApplicationInfo]) {
        // TODO: We should clean this in better way so that sqlItoProblematic is handled similar
        //      way in both Qual/Prof tools.
        //      This is a hack to get the processSQLPlanMetrics() method to work for both Qual/Prof
        //       - we check if the app is AppInfo, then we add the potential problems.
        //       - If not, then we do not set the problematic issues because this will cause
        //        records to be duplicated in the Qualification tool.
        // Check if readsSchema is complex for the given sql
        val sqlNestedComplexTypes =
          AppBase.parseReadSchemaForNestedTypes(sqlDataSources.map { ds => ds.schema })
        // Append problematic issues to the global variable for that SqlID
        if (sqlNestedComplexTypes._2.nonEmpty) {
          potentialProblems += "NESTED COMPLEX TYPE"
        }
        // Finally, add the local potentialProblems to the global data structure if any.
        app.sqlIDtoProblematic(sqlPIGEntry.sqlID) = potentialProblems.toSet
        // Convert the problematic issues to a string and update the SQLInfo
        app.sqlIdToInfo.get(sqlPIGEntry.sqlID).foreach { sqlInfoClass =>
          sqlInfoClass.problematic = ToolUtils.formatPotentialProblems(potentialProblems.toSeq)
        }
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
          v.contains(sModel.sId)
        }.keys.toSeq
        val nodeNames = app.sqlPlans.get(j.sqlID.get).map { planInfo =>
          val nodes = ToolsPlanGraph(planInfo).allNodes
          val validNodes = nodes.filter { n =>
            nodeIds.contains((j.sqlID.get, n.id))
          }
          validNodes.map(n => s"${n.name}(${n.id.toString})")
        }.getOrElse(Seq.empty)
        SQLStageInfoProfileResult(appIndex, j.sqlID.get, jobId, sModel.sId,
          sModel.attemptId, sModel.duration, nodeNames)
      }
    }
    sqlToStages.toSeq
  }

  // Store (min, median, max, total) for a given metric
  private case class statisticsMetrics(min: Long, med:Long, max:Long, total: Long)

  def generateSQLAccums(): Seq[SQLAccumProfileResults] = {
    allSQLMetrics.flatMap { metric =>
      val jobsForSql = app.jobIdToInfo.filter { case (_, jc) =>
        // Avoid getOrElse to reduce memory allocations
        jc.sqlID.isDefined && jc.sqlID.get == metric.sqlID
      }
      val stageIdsForSQL = jobsForSql.flatMap(_._2.stageIds).toSet
      val accumsOpt = app.taskStageAccumMap.get(metric.accumulatorId)
      val taskMax = accumsOpt match {
        case Some(accums) =>
          val filtered = accums.filter { a =>
            stageIdsForSQL.contains(a.stageId)
          }
          // If metricType is size, average or timing, we want to read field `update` value
          // to get the min, median, max, and total. Otherwise, we want to use field `value`.
          if (SQLMetricsStats.hasStats(metric.metricType)) {
            val accumValues = filtered.map(_.update.getOrElse(0L)).sortWith(_ < _)
            if (accumValues.isEmpty) {
              None
            }
            else if (accumValues.length <= 1) {
              Some(statisticsMetrics(0L, 0L, 0L, accumValues.sum))
            } else {
              Some(statisticsMetrics(accumValues(0), accumValues(accumValues.size / 2),
                accumValues(accumValues.size - 1), accumValues.sum))
            }
          } else {
            val accumValues = filtered.map(_.value.getOrElse(0L))
            if (accumValues.isEmpty) {
              None
            } else {
              Some(statisticsMetrics(0L, 0L, 0L, accumValues.max))
            }
          }
        case None => None
      }

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
            Some(statisticsMetrics(0L, 0L, 0L, accumValues.sum))
          } else {
            Some(statisticsMetrics(accumValues(0), accumValues(accumValues.size / 2),
              accumValues(accumValues.size - 1), accumValues.sum))
          }
        case None =>
          None
      }

      if (taskMax.isDefined || driverMax.isDefined) {
        val taskInfo = taskMax match {
          case Some(task) => task
          case None => statisticsMetrics(0L, 0L, 0L, 0L)
        }
        val driverInfo = driverMax match {
          case Some(driver) => driver
          case None => statisticsMetrics(0L, 0L, 0L, 0L)
        }

        val max = Math.max(taskInfo.max, driverInfo.max)
        val min = Math.max(taskInfo.min, driverInfo.min)
        val med = Math.max(taskInfo.med, driverInfo.med)
        val total = Math.max(taskInfo.total, driverInfo.total)

        Some(SQLAccumProfileResults(appIndex, metric.sqlID,
          metric.nodeID, metric.nodeName, metric.accumulatorId, metric.name,
          min, med, max, total, metric.metricType, metric.stageIds.mkString(",")))
      } else {
        None
      }
    }
  }
}

object AppSQLPlanAnalyzer {
  def processSQLPlan(app: ApplicationInfo): AppSQLPlanAnalyzer = {
    val sqlProcessor = new AppSQLPlanAnalyzer(app, app.index)
    sqlProcessor.processSQLPlanMetrics()
    sqlProcessor
  }
  def apply(app: AppBase, appIndex: Int): AppSQLPlanAnalyzer = {
    val sqlProcessor = new AppSQLPlanAnalyzer(app, appIndex)
    sqlProcessor.processSQLPlanMetrics()
    sqlProcessor
  }
}
