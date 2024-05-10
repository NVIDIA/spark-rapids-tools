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

package org.apache.spark.sql.rapids.tool.profiling

import scala.collection.{Map, mutable}
import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.planparser.{ReadParser, SQLPlanParser}
import com.nvidia.spark.rapids.tool.profiling._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.{AppBase, RDDCheckHelper, ToolUtils}
import org.apache.spark.sql.rapids.tool.SqlPlanInfoGraphBuffer
import org.apache.spark.sql.rapids.tool.util.ToolsPlanGraph


class SparkPlanInfoWithStage(
    nodeName: String,
    simpleString: String,
    override val children: Seq[SparkPlanInfoWithStage],
    metadata: scala.Predef.Map[String, String],
    metrics: Seq[SQLMetricInfo],
    val stageId: Option[Int]) extends SparkPlanInfo(nodeName, simpleString, children,
  metadata, metrics) {

  import SparkPlanInfoWithStage._

  def debugEquals(other: Any, depth: Int = 0): Boolean = {
    System.err.println(s"${" " * depth}DOES $this == $other?")
    other match {
      case o: SparkPlanInfo =>
        if (nodeName != o.nodeName) {
          System.err.println(s"${" " * depth}" +
              s"$this != $o (names different) ${this.nodeName} != ${o.nodeName}")
          return false
        }
        if (simpleString != o.simpleString) {
          System.err.println(s"${" " * depth}$this != $o (simpleString different) " +
              s"${this.simpleString} != ${o.simpleString}")
          return false
        }
        if (children.length != o.children.length) {
          System.err.println(s"${" " * depth}$this != $o (different num children) " +
              s"${this.children.length} ${o.children.length}")
          return false
        }
        children.zip(o.children).zipWithIndex.forall {
          case ((t, o), index) =>
            System.err.println(s"${" " * depth}COMPARING CHILD $index")
            t.debugEquals(o, depth = depth + 1)
        }
      case _ =>
        System.err.println(s"${" " * depth}NOT EQUAL WRONG TYPE")
        false
    }
  }

  override def toString: String = {
    s"PLAN NODE: '$nodeName' '$simpleString' $stageId"
  }

  /**
   * Create a new SparkPlanInfoWithStage that is normalized in a way so that CPU and GPU
   * plans look the same. This should really only be used when matching stages/jobs between
   * different plans.
   */
  def normalizeForStageComparison: SparkPlanInfoWithStage = {
    nodeName match {
      case "GpuColumnarToRow" | "GpuRowToColumnar" | "ColumnarToRow" | "RowToColumnar" |
           "AdaptiveSparkPlan" | "AvoidAdaptiveTransitionToRow" |
           "HostColumnarToGpu" | "GpuBringBackToHost" |
           "GpuCoalesceBatches" | "GpuShuffleCoalesce" |
           "InputAdapter" | "Subquery" | "ReusedSubquery" |
           "CustomShuffleReader" | "GpuCustomShuffleReader" | "ShuffleQueryStage" |
           "BroadcastQueryStage" |
           "Sort" | "GpuSort" =>
        // Remove data format, grouping changes because they don't impact the computation
        // Remove CodeGen stages (not important to actual query execution)
        // Remove AQE fix-up parts.
        // Remove sort because in a lot of cases (like sort merge join) it is not part of the
        // actual computation and it is hard to tell which sort is important to the query result
        // and which is not
        children.head.normalizeForStageComparison
      case name if name.startsWith("WholeStageCodegen") =>
        // Remove whole stage codegen (It includes an ID afterwards that we should ignore too)
        children.head.normalizeForStageComparison
      case name if name.contains("Exchange") =>
        // Drop all exchanges, a broadcast could become a shuffle/etc
        children.head.normalizeForStageComparison
      case "GpuTopN" if isShuffledTopN(this) =>
        val coalesce = this.children.head
        val shuffle = coalesce.children.head
        val firstTopN = shuffle.children.head
        firstTopN.normalizeForStageComparison
      case name =>
        val newName = normalizedNameRemapping.getOrElse(name,
          if (name.startsWith("Gpu")) {
            name.substring(3)
          } else {
            name
          })

        val normalizedChildren = children.map(_.normalizeForStageComparison)
        // We are going to ignore the simple string because it can contain things in it that
        // are specific to a given run
        new SparkPlanInfoWithStage(newName, newName, normalizedChildren, metadata,
          metrics, stageId)
    }
  }

  /**
   * Walk the tree depth first and get all of the stages for each node in the tree
   */
  def depthFirstStages: Seq[Option[Int]] =
    Seq(stageId) ++ children.flatMap(_.depthFirstStages)
}

object SparkPlanInfoWithStage {
  def apply(plan: SparkPlanInfo, accumIdToStageId: Map[Long, Int]): SparkPlanInfoWithStage = {
    // In some cases Spark will do a shuffle in the middle of an operation,
    // like TakeOrderedAndProject. In those cases the node is associated with the
    // min stage ID, just to remove ambiguity

    val newChildren = plan.children.map(SparkPlanInfoWithStage(_, accumIdToStageId))

    val stageId = plan.metrics.flatMap { m =>
      accumIdToStageId.get(m.accumulatorId)
    }.reduceOption((l, r) => Math.min(l, r))

    new SparkPlanInfoWithStage(plan.nodeName, plan.simpleString, newChildren, plan.metadata,
      plan.metrics, stageId)
  }

  private val normalizedNameRemapping: Map[String, String] = Map(
    "Execute GpuInsertIntoHadoopFsRelationCommand" -> "Execute InsertIntoHadoopFsRelationCommand",
    "GpuTopN" -> "TakeOrderedAndProject",
    "SortMergeJoin" -> "Join",
    "ShuffledHashJoin" -> "Join",
    "GpuShuffledHashJoin" -> "Join",
    "BroadcastHashJoin" -> "Join",
    "GpuBroadcastHashJoin" -> "Join",
    "HashAggregate" -> "Aggregate",
    "SortAggregate" -> "Aggregate",
    "GpuHashAggregate" -> "Aggregate",
    "RunningWindow" -> "Window", //GpuWindow and Window are already covered
    "GpuRunningWindow" -> "Window")

  private def isShuffledTopN(info: SparkPlanInfoWithStage): Boolean = {
    if (info.children.length == 1 && // shuffle coalesce
        info.children.head.children.length == 1 && // shuffle
        info.children.head.children.head.children.length == 1) { // first top n
      val coalesce = info.children.head
      val shuffle = coalesce.children.head
      val firstTopN = shuffle.children.head
      info.nodeName == "GpuTopN" &&
          (coalesce.nodeName == "GpuShuffleCoalesce" ||
              coalesce.nodeName == "GpuCoalesceBatches") &&
          shuffle.nodeName == "GpuColumnarExchange" && firstTopN.nodeName == "GpuTopN"
    } else {
      false
    }
  }
}

/**
 * ApplicationInfo class saves all parsed events for future use.
 * Used only for Profiling.
 */
class ApplicationInfo(
    hadoopConf: Configuration,
    eLogInfo: EventLogInfo,
    val index: Int)
  extends AppBase(Some(eLogInfo), Some(hadoopConf)) with Logging {

  // resourceprofile id to resource profile info
  val resourceProfIdToInfo = new HashMap[Int, ResourceProfileInfoCase]()

  var blockManagersRemoved: ArrayBuffer[BlockManagerRemovedCase] =
     ArrayBuffer[BlockManagerRemovedCase]()
  
  // physicalPlanDescription stores HashMap (sqlID <-> physicalPlanDescription)
  var physicalPlanDescription: mutable.HashMap[Long, String] = mutable.HashMap.empty[Long, String]

  var allSQLMetrics: ArrayBuffer[SQLMetricInfoCase] = ArrayBuffer[SQLMetricInfoCase]()
  var sqlPlanMetricsAdaptive: ArrayBuffer[SQLPlanMetricsCase] = ArrayBuffer[SQLPlanMetricsCase]()

  var taskEnd: ArrayBuffer[TaskCase] = ArrayBuffer[TaskCase]()
  var unsupportedSQLplan: ArrayBuffer[UnsupportedSQLPlan] = ArrayBuffer[UnsupportedSQLPlan]()
  var wholeStage: ArrayBuffer[WholeStageCodeGenResults] = ArrayBuffer[WholeStageCodeGenResults]()
  val sqlPlanNodeIdToStageIds: mutable.HashMap[(Long, Long), Set[Int]] =
    mutable.HashMap.empty[(Long, Long), Set[Int]]

  private lazy val eventProcessor =  new EventsProcessor(this)

  // Process all events
  processEvents()
  // Process SQL Plan Metrics after all events are processed
  processSQLPlanMetrics()
  aggregateAppInfo

  override def processEvent(event: SparkListenerEvent) = {
    eventProcessor.processAnyEvent(event)
    false
  }

  override def postCompletion(): Unit = {
    clusterInfo = buildClusterInfo
  }

  /**
   * Connects Operators to Stages using AccumulatorIDs
   * @param cb function that creates a SparkPlanGraph. This can be used as a cacheHolder for the
   *           object created to be used later.
   */
  private def connectOperatorToStage(cb: (Long, SparkPlanInfo) => SparkPlanGraph): Unit = {
    for ((sqlId, planInfo) <- sqlPlans) {
      val planGraph: SparkPlanGraph = cb.apply(sqlId, planInfo)
      // Maps stages to operators by checking for non-zero intersection
      // between nodeMetrics and stageAccumulateIDs
      val nodeIdToStage = planGraph.allNodes.map { node =>
        val mappedStages = SQLPlanParser.getStagesInSQLNode(node, this)
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
      val sqlDataSources = checkMetadataForReadSchema(sqlPIGEntry)
      for (node <- sqlPIGEntry.sparkPlanGraph.allNodes) {
        var nodeIsDsOrRDD = false
        if (node.isInstanceOf[org.apache.spark.sql.execution.ui.SparkPlanGraphCluster]) {
          val ch = node.asInstanceOf[org.apache.spark.sql.execution.ui.SparkPlanGraphCluster].nodes
          ch.foreach { c =>
            wholeStage += WholeStageCodeGenResults(
              index, sqlPIGEntry.sqlID, node.id, node.name, c.name, c.id)
          }
        }
        // get V2 dataSources for that node
        val nodeV2Reads = checkGraphNodeForReads(sqlPIGEntry.sqlID, node)
        if (nodeV2Reads.isDefined) {
          sqlDataSources += nodeV2Reads.get
        }
        nodeIsDsOrRDD = RDDCheckHelper.isDatasetOrRDDPlan(node.name, node.desc).isRDD
        if (nodeIsDsOrRDD) {
          if (gpuMode) { // we want to report every node that is an RDD
            val thisPlan = UnsupportedSQLPlan(sqlPIGEntry.sqlID, node.id, node.name, node.desc,
              "Contains Dataset or RDD")
            unsupportedSQLplan += thisPlan
          }
          // If one node is RDD, the Sql should be set too
          if (!sqlIsDsOrRDD) { // We need to set the flag only once for the given sqlID
            sqlIsDsOrRDD = true
            sqlIdToInfo.get(sqlPIGEntry.sqlID).foreach { sql =>
              sql.setDsOrRdd(sqlIsDsOrRDD)
              sqlIDToDataSetOrRDDCase += sqlPIGEntry.sqlID
              // Clear the potential problems since it is an RDD to free memory
              potentialProblems.clear()
            }
          }
        }
        if (!sqlIsDsOrRDD) {
          // Append current node's potential problems to the Sql problems only if the SQL is not an
          // RDD. This is an optimization since the potentialProblems won't be used any more.
          potentialProblems ++= findPotentialIssues(node.desc)
        }
        // Then process SQL plan metric type
        for (metric <- node.metrics) {
          val stages =
            sqlPlanNodeIdToStageIds.get((sqlPIGEntry.sqlID, node.id)).getOrElse(Set.empty)
          val allMetric = SQLMetricInfoCase(sqlPIGEntry.sqlID, metric.name,
            metric.accumulatorId, metric.metricType, node.id,
            node.name, node.desc, stages)

          allSQLMetrics += allMetric
          if (this.sqlPlanMetricsAdaptive.nonEmpty) {
            val adaptive = sqlPlanMetricsAdaptive.filter { adaptiveMetric =>
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
      // Check if readsSchema is complex for the given sql
      val sqlNestedComplexTypes =
        AppBase.parseReadSchemaForNestedTypes(sqlDataSources.map { ds => ds.schema })
      // Append problematic issues to the global variable for that SqlID
      if (sqlNestedComplexTypes._2.nonEmpty) {
        potentialProblems += "NESTED COMPLEX TYPE"
      }
      // Finally, add the local potentialProblems to the global data structure if any.
      sqlIDtoProblematic(sqlPIGEntry.sqlID) = potentialProblems.toSet
      // Convert the problematic issues to a string and update the SQLInfo
      sqlIdToInfo.get(sqlPIGEntry.sqlID).foreach { sqlInfoClass =>
        sqlInfoClass.problematic = ToolUtils.formatPotentialProblems(potentialProblems.toSeq)
      }
    }
  }

  /**
   * This function is meant to clean up Delta log execs so that you could align
   * SQL ids between CPU and GPU eventlogs. It attempts to remove any delta log
   * only SQL ids. This includes reading checkpoints, delta_log json files,
   * updating Delta state cache/table. There is also one special case for
   * MergeIntoCommandEdge that I think showed up because some eventlogs might not
   * be on the exact same data.
   */
  def cleanSQLIdsToAlign(): Seq[SQLCleanAndAlignIdsProfileResult] = {
    val noDeltaLogs = sqlPlans.filter {
      case (id, planInfo) =>
        val planGraph = ToolsPlanGraph(planInfo)
        val sqlDesc = sqlIdToInfo(id).physicalPlanDesc
        val leftoverNodes = planGraph.allNodes.filter { node =>
          val normalizedNodeName = node.name.stripSuffix("$")
          normalizedNodeName match {
            case "RDDScanExec" =>
              if (sqlDesc.contains("Delta Table State") ||
                sqlDesc.contains("Delta Table Checkpoint") ||
                sqlDesc.contains("delta_log")) {
                false
              } else {
                true
              }
            case s if s.contains("WholeStageCodegen") =>
              if (sqlDesc.contains("_delta_log")) {
                false
              } else {
                true
              }
            case s if s.contains("MergeIntoCommandEdge") =>
              // this is a bit odd but GPU doesn't accelerate anyway, I think this might be
              // due to differences in data between runs
              // Execute MergeIntoCommandEdge (1)
              //   +- MergeIntoCommandEdge (2)
              if (planGraph.allNodes.size < 2) {
                false
              } else {
                true
              }
            case "LocalTableScan" =>
              if (sqlDesc.contains("stats_parsed.numRecords")) {
                false
              } else if (planGraph.allNodes.size == 1) {
                false
              } else {
                true
              }
            case s if ReadParser.isScanNode(s) =>
              if (sqlDesc.contains("_delta_log") || sqlDesc.contains("_databricks_internal")) {
                false
              } else if (sqlDesc.contains("checkpoint")) {
                // double check it has parquet - regex are expensive though so only do
                // if necessary
                val checkpoint = ".*Location:(.*)checkpoint(.*).parquet(.*)".r
                if (checkpoint.findFirstIn(sqlDesc).isDefined) {
                  false
                } else {
                  true
                }
              } else {
                true
              }
            case _ =>
              true
          }
        }
        if (leftoverNodes.size < planGraph.allNodes.size) {
          false
        } else {
          true
        }
      case _ =>
        true
    }.toSeq
    noDeltaLogs.map(_._1).sorted.map { id =>
      SQLCleanAndAlignIdsProfileResult(index, id)
    }
  }

  def aggregateSQLStageInfo: Seq[SQLStageInfoProfileResult] = {
    val jobsWithSQL = jobIdToInfo.filter { case (_, j) =>
      j.sqlID.nonEmpty
    }
    val sqlToStages = jobsWithSQL.flatMap { case (jobId, j) =>
      val stages = j.stageIds
      val stagesInJob = stageManager.getStagesByIds(stages)
      stagesInJob.map { sModel =>
        val nodeIds = sqlPlanNodeIdToStageIds.filter { case (_, v) =>
          v.contains(sModel.sId)
        }.keys.toSeq
        val nodeNames = sqlPlans.get(j.sqlID.get).map { planInfo =>
          val nodes = ToolsPlanGraph(planInfo).allNodes
          val validNodes = nodes.filter { n =>
            nodeIds.contains((j.sqlID.get, n.id))
          }
          validNodes.map(n => s"${n.name}(${n.id.toString})")
        }.getOrElse(Seq.empty)
        SQLStageInfoProfileResult(index, j.sqlID.get, jobId, sModel.sId,
          sModel.attemptId, sModel.duration, nodeNames)
      }
    }
    sqlToStages.toSeq
  }

  private def aggregateAppInfo: Unit = {
    estimateAppEndTime { () =>
      val jobEndTimes = jobIdToInfo.map { case (_, jc) => jc.endTime }.filter(_.isDefined)
      val sqlEndTimes = sqlIdToInfo.map { case (_, sc) => sc.endTime }.filter(_.isDefined)
      val estimatedResult =
        if (sqlEndTimes.size == 0 && jobEndTimes.size == 0) {
          None
        } else {
          logWarning("Application End Time is unknown, estimating based on" +
            " job and sql end times!")
          // estimate the app end with job or sql end times
          val sqlEndTime = if (sqlEndTimes.size == 0) 0L else sqlEndTimes.map(_.get).max
          val jobEndTime = if (jobEndTimes.size == 0) 0L else jobEndTimes.map(_.get).max
          val maxEndTime = math.max(sqlEndTime, jobEndTime)
          if (maxEndTime == 0) None else Some(maxEndTime)
        }
      estimatedResult
    }
  }
}
