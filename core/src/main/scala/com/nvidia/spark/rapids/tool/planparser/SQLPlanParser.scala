/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, WeakHashMap}
import scala.util.control.NonFatal
import scala.util.matching.Regex

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphNode}
import org.apache.spark.sql.rapids.tool.{AppBase, BuildSide, ExecHelper, JoinType, RDDCheckHelper, ToolUtils, UnsupportedExpr}
import org.apache.spark.sql.rapids.tool.util.ToolsPlanGraph

object OpActions extends Enumeration {
  type OpAction = Value
  val NONE, IgnoreNoPerf, IgnorePerf, Triage = Value
}

object OpTypes extends Enumeration {
  type OpType = Value
  val ReadExec, ReadRDD, WriteExec, Exec, Expr, UDF, DataSet = Value
}

object UnsupportedReasons extends Enumeration {
  type UnsupportedReason = Value
  val IS_UDF, CONTAINS_UDF,
      IS_DATASET, CONTAINS_DATASET,
      IS_UNSUPPORTED, CONTAINS_UNSUPPORTED_EXPR,
      UNSUPPORTED_IO_FORMAT = Value

  // Mutable map to cache custom reasons
  private val customReasonsCache = WeakHashMap.empty[String, Value]

  // Method to get or create a custom reason
  def CUSTOM_REASON(reason: String): Value = {
    customReasonsCache.getOrElseUpdate(reason, new Val(nextId, reason))
  }

  def reportUnsupportedReason(unsupportedReason: UnsupportedReason): String = {
    unsupportedReason match {
      case IS_UDF => "Is UDF"
      case CONTAINS_UDF => "Contains UDF"
      case IS_DATASET => "Is Dataset or RDD"
      case CONTAINS_DATASET => "Contains Dataset or RDD"
      case IS_UNSUPPORTED => "Unsupported"
      case CONTAINS_UNSUPPORTED_EXPR => "Contains unsupported expr"
      case UNSUPPORTED_IO_FORMAT => "Unsupported IO format"
      case customReason @ _  => customReason.toString
    }
  }
}

case class UnsupportedExecSummary(
    sqlId: Long,
    execId: Long,
    execValue: String,
    opType: OpTypes.OpType,
    reason: UnsupportedReasons.UnsupportedReason,
    opAction: OpActions.OpAction,
    isExpression: Boolean = false) {

  val finalOpType: String = if (opType.equals(OpTypes.UDF) || opType.equals(OpTypes.DataSet)) {
    s"${OpTypes.Exec.toString}"
  } else {
    s"${opType.toString}"
  }

  val unsupportedOperator: String = execValue

  val details: String = UnsupportedReasons.reportUnsupportedReason(reason)
}

case class ExecInfo(
    sqlID: Long,
    exec: String,
    expr: String,
    speedupFactor: Double,
    duration: Option[Long],
    nodeId: Long,
    opType: OpTypes.OpType,
    isSupported: Boolean,
    children: Option[Seq[ExecInfo]], // only one level deep
    var stages: Set[Int],
    var shouldRemove: Boolean,
    var unsupportedExecReason: String,
    unsupportedExprs: Seq[UnsupportedExpr],
    dataSet: Boolean,
    udf: Boolean,
    shouldIgnore: Boolean) {

  private def childrenToString = {
    val str = children.map { c =>
      c.map("       " + _.toString).mkString("\n")
    }.getOrElse("")
    if (str.nonEmpty) {
      "\n" + str
    } else {
      str
    }
  }

  override def toString: String = {
    s"exec: $exec, expr: $expr, sqlID: $sqlID , speedupFactor: $speedupFactor, " +
      s"duration: $duration, nodeId: $nodeId, " +
      s"isSupported: $isSupported, children: " +
      s"$childrenToString, stages: ${stages.mkString(",")}, " +
      s"shouldRemove: $shouldRemove, shouldIgnore: $shouldIgnore"
  }

  def setStages(stageIDs: Set[Int]): Unit = {
    stages = stageIDs
  }

  def setShouldRemove(value: Boolean): Unit = {
    shouldRemove ||= value
  }

  def setUnsupportedExecReason(reason: String): Unit = {
    unsupportedExecReason = reason
  }

  // Helper function to determine the unsupported reason
  def determineUnsupportedReason(reason: String,
      knownReason: UnsupportedReasons.Value): UnsupportedReasons.Value = {
    if (reason.nonEmpty) UnsupportedReasons.CUSTOM_REASON(reason) else knownReason
  }

  def getOpAction: OpActions.OpAction = {
    // shouldRemove is checked first because sometimes an exec could have both flag set to true,
    // but then we care about having the "NoPerf" part
    if (isSupported) {
      OpActions.NONE
    } else {
      if (shouldRemove) {
        OpActions.IgnoreNoPerf
      } else if (shouldIgnore) {
        OpActions.IgnorePerf
      } else  {
        OpActions.Triage
      }
    }
  }

  private def getUnsupportedReason: UnsupportedReasons.UnsupportedReason = {
    if (children.isDefined) {
      // TODO: Handle the children
    }

    if (udf) {
      UnsupportedReasons.CONTAINS_UDF
    } else if (dataSet) {
      if (unsupportedExprs.isEmpty) { // case when the node itself is a DataSet or RDD
        UnsupportedReasons.IS_DATASET
      } else {
        UnsupportedReasons.CONTAINS_DATASET
      }
    } else if (unsupportedExprs.nonEmpty) {
      UnsupportedReasons.CONTAINS_UNSUPPORTED_EXPR
    } else {
      opType match {
        case OpTypes.ReadExec | OpTypes.WriteExec => UnsupportedReasons.UNSUPPORTED_IO_FORMAT
        case _ => UnsupportedReasons.IS_UNSUPPORTED
      }
    }
  }

  def getUnsupportedExecSummaryRecord(execId: Long): Seq[UnsupportedExecSummary] = {
    // Get the custom reason if it exists
    val execUnsupportedReason = determineUnsupportedReason(unsupportedExecReason,
      getUnsupportedReason)

    // Initialize the result with the exec summary
    val res = ArrayBuffer(UnsupportedExecSummary(sqlID, execId, exec, opType,
      execUnsupportedReason, getOpAction))

    // TODO: Should we iterate on exec children?
    // add the unsupported expressions to the results, if there are any custom reasons add them
    // to the result appropriately
    if (unsupportedExprs.nonEmpty) {
      val exprKnownReason = execUnsupportedReason match {
        case UnsupportedReasons.CONTAINS_UDF => UnsupportedReasons.IS_UDF
        case UnsupportedReasons.CONTAINS_DATASET => UnsupportedReasons.IS_DATASET
        case UnsupportedReasons.UNSUPPORTED_IO_FORMAT => UnsupportedReasons.UNSUPPORTED_IO_FORMAT
        case _ => UnsupportedReasons.IS_UNSUPPORTED
      }

      unsupportedExprs.foreach { expr =>
        val exprUnsupportedReason = determineUnsupportedReason(expr.unsupportedReason,
          exprKnownReason)
        res += UnsupportedExecSummary(sqlID, execId, expr.exprName, OpTypes.Expr,
          exprUnsupportedReason, getOpAction, isExpression = true)
      }
    }
    res
  }
}

object ExecInfo {
  // Used to create an execInfo without recalculating the dataSet or Udf.
  // This is helpful when we know that node description may contain some patterns that can be
  // mistakenly identified as UDFs 
  def createExecNoNode(sqlID: Long,
      exec: String,
      expr: String,
      speedupFactor: Double,
      duration: Option[Long],
      nodeId: Long,
      opType: OpTypes.OpType,
      isSupported: Boolean,
      children: Option[Seq[ExecInfo]], // only one level deep
      stages: Set[Int] = Set.empty,
      shouldRemove: Boolean = false,
      unsupportedExecReason: String = "",
      unsupportedExprs: Seq[UnsupportedExpr] = Seq.empty,
      dataSet: Boolean = false,
      udf: Boolean = false): ExecInfo = {
    // Set the ignoreFlag
    // 1- we ignore any exec with UDF
    // 2- we ignore any exec with dataset
    // 3- Finally we ignore any exec matching the lookup table
    // if the opType is RDD, then we automatically enable the datasetFlag
    val finalDataSet = dataSet || opType.equals(OpTypes.ReadRDD)
    val shouldIgnore = udf || finalDataSet || ExecHelper.shouldIgnore(exec)
    val removeFlag = shouldRemove || ExecHelper.shouldBeRemoved(exec)
    val finalOpType = if (udf) {
      OpTypes.UDF
    } else if (dataSet) {
      // we still want the ReadRDD to stand out from other RDDs. So, we use the original
      // dataSetFlag
      OpTypes.DataSet
    } else {
      opType
    }
    // Set the supported Flag
    val supportedFlag = isSupported && !udf && !finalDataSet
    ExecInfo(
      sqlID,
      exec,
      expr,
      speedupFactor,
      duration,
      nodeId,
      finalOpType,
      supportedFlag,
      children,
      stages,
      removeFlag,
      unsupportedExecReason,
      unsupportedExprs,
      finalDataSet,
      udf,
      shouldIgnore
    )
  }

  def apply(
      node: SparkPlanGraphNode,
      sqlID: Long,
      exec: String,
      expr: String,
      speedupFactor: Double,
      duration: Option[Long],
      nodeId: Long,
      isSupported: Boolean,
      children: Option[Seq[ExecInfo]], // only one level deep
      stages: Set[Int] = Set.empty,
      shouldRemove: Boolean = false,
      unsupportedExecReason:String = "",
      unsupportedExprs: Seq[UnsupportedExpr] = Seq.empty,
      dataSet: Boolean = false,
      udf: Boolean = false,
      opType: OpTypes.OpType = OpTypes.Exec): ExecInfo = {
    // Some execs need to be trimmed such as "Scan"
    // Example: Scan parquet . ->  Scan parquet.
    // scan nodes needs trimming
    val nodeName = node.name.trim
    // we don't want to mark the *InPandas and ArrowEvalPythonExec as unsupported with UDF
    val containsUDF = udf || ExecHelper.isUDF(node)
    // check is the node has a dataset operations and if so change to not supported
    val rddCheckRes = RDDCheckHelper.isDatasetOrRDDPlan(nodeName, node.desc)
    val ds = dataSet || rddCheckRes.isRDD

    // if the expression is RDD because of the node name, then we do not want to add the
    // unsupportedExpressions because it becomes bogus.
    val finalUnsupportedExpr = if (rddCheckRes.nodeDescRDD) {
      Seq.empty[UnsupportedExpr]
    } else {
      unsupportedExprs
    }
    createExecNoNode(
      sqlID,
      exec,
      expr,
      speedupFactor,
      duration,
      nodeId,
      opType,
      isSupported,
      children,
      stages,
      shouldRemove,
      unsupportedExecReason,
      finalUnsupportedExpr,
      ds,
      containsUDF
    )
  }
}

case class PlanInfo(
    appID: String,
    sqlID: Long,
    sqlDesc: String,
    execInfo: Seq[ExecInfo]
)

object SQLPlanParser extends Logging {

  val equiJoinRegexPattern = """\[([\w#, +*\\\-\.<>=$\`\(\)]+\])""".r

  val functionPattern = """(\w+)\(.*\)""".r

  val functionPrefixPattern = """(\w+)\(""".r // match words preceded by parenthesis

  val windowFunctionPattern = """(\w+)\(""".r

  val aggregatePrefixes = Set(
    "finalmerge_", // DB specific prefix for final merge agg functions
    "partial_",    // used for partials
    "merge_"       // Used for partial merge
  )

  val ignoreExpressions = Set("any", "cast", "ansi_cast", "decimal", "decimaltype", "every",
    "some",
    "list",
    // some ops turn into literals and they should not cause any fallbacks
    "current_database", "current_user", "current_timestamp",
    // ArrayBuffer is a Scala function and may appear in some of the JavaRDDs/UDAFs)
    "arraybuffer", "arraytype",
    // TODO: we may need later to consider that structs indicate unsupported data types,
    //  but for now we just ignore it to avoid false positives.
    //  StructType and StructField showup from expressions like ("from_json").
    //  We do not want them to appear as independent expressions.
    "structfield", "structtype")

  /**
   * This function is used to create a set of nodes that should be skipped while parsing the Execs
   * of a specific node.
   * When a reused expression appears in a SparkPlan, the sparkPlanGraph constructed from the
   * eventlog will have duplicates for all the ancestors of the exec (i.e., "ReusedExchange").
   * This leads to a gap in the GPU speedups across different platforms which generate the graph
   * without duplicates.
   * A work around is to detect all the duplicates nodes so that we can mark them as "shouldRemove".
   * If a wholeGen node has all the children labeled as ancestor of reused-exchange, then the
   * wholeGen should also be added to the same set.
   * @param planGraph the graph generated for a spark plan. This graph can be different depending on
   *                  the spark-sql jar version used to construct the graph from existing eventlogs.
   * @return a set of node IDS to be skipped during the aggregation of the speedups.
   */
  private def buildSkippedReusedNodesForPlan(planGraph: SparkPlanGraph): Set[Long] = {
    def findNodeAncestors(planGraph: SparkPlanGraph,
        graphNode: SparkPlanGraphNode): mutable.Set[Long] = {
      // Given a node in the graph, this function is to go backward to find all the ancestors of
      // the node including the node, itself.
      val visited = mutable.Set[Long](graphNode.id)
      val q1 = mutable.Queue[Long](graphNode.id)
      while (q1.nonEmpty) {
        val curNode = q1.dequeue()
        val allSinkEdges = planGraph.edges
          .filter(e => e.toId == curNode)
          .filterNot(e => visited.contains(e.fromId))
        for (currEdge <- allSinkEdges) {
          q1.enqueue(currEdge.fromId)
          visited += currEdge.fromId
        }
      }
      // Loop on the wholeGen to see if any of them is covered by the ancestors path.
      // This implies that the wholeStageCodeGen is also reused.
      // Note that the following logic can be moved to the WholeStageCodegen parser. Handling the
      // logic here has advantages:
      //   1- no need to append to the final set
      //   2- keep the logic in one place.
      val allStageNodes = planGraph.nodes.filter(
        stageNode => stageNode.name.contains("WholeStageCodegen"))
      allStageNodes.filter { n =>
        n.asInstanceOf[SparkPlanGraphCluster].nodes.forall(c => visited.contains(c.id))
      }.foreach(wNode => visited += wNode.id)
      visited
    }

    // create a list of all the candidate leaf nodes. This includes wholeStageCodeGen.
    val candidateNodes = planGraph.allNodes.filter(n => reuseExecs.contains(n.name))
    candidateNodes.flatMap(findNodeAncestors(planGraph, _)).toSet
  }

  def parseSQLPlan(
      appID: String,
      planInfo: SparkPlanInfo,
      sqlID: Long,
      sqlDesc: String,
      checker: PluginTypeChecker,
      app: AppBase): PlanInfo = {
    val planGraph = ToolsPlanGraph(planInfo)
    // Find all the node graphs that should be excluded and send it to the parsePlanNode
    val excludedNodes = buildSkippedReusedNodesForPlan(planGraph)
    // we want the sub-graph nodes to be inside of the wholeStageCodeGen so use nodes
    // vs allNodes
    val execInfos = planGraph.nodes.flatMap { node =>
      parsePlanNode(node, sqlID, checker, app, reusedNodeIds = excludedNodes)
    }
    PlanInfo(appID, sqlID, sqlDesc, execInfos)
  }

  def getStagesInSQLNode(node: SparkPlanGraphNode, app: AppBase): Set[Int] = {
    val nodeAccums = node.metrics.map(_.accumulatorId)
    nodeAccums.flatMap { nodeAccumId =>
      app.accumulatorToStages.get(nodeAccumId)
    }.flatten.toSet
  }

  // Set containing execs that refers to other expressions. We need this to be a list to allow
  // appending more execs in teh future as necessary.
  // Note that Spark graph may create duplicate nodes when any of the following execs exists.
  private val reuseExecs = Set("ReusedExchange")

  def parsePlanNode(
      node: SparkPlanGraphNode,
      sqlID: Long,
      checker: PluginTypeChecker,
      app: AppBase,
      reusedNodeIds: Set[Long]
  ): Seq[ExecInfo] = {
    // Avoid counting duplicate nodes. We mark them as shouldRemove to neutralize their impact on
    // speedups.
    val isDupNode = reusedNodeIds.contains(node.id)
    // Normalize the execName by removing the trailing '$' character, if present.
    // This is necessary because in Scala, the '$' character is often appended to the names of
    // generated classes or objects, and we want to match the base name regardless of this suffix.
    val normalizedNodeName = node.name.stripSuffix("$")
    if (isDupNode) {
      // log that information. This should not cause significant increase in log size.
      logDebug(s"Marking [sqlID = ${sqlID}, node = ${normalizedNodeName}] as shouldRemove. " +
        s"Reason: duplicate - ancestor of ReusedExchange")
    }
    if (normalizedNodeName.contains("WholeStageCodegen")) {
      // this is special because it is a SparkPlanGraphCluster vs SparkPlanGraphNode
      WholeStageExecParser(node.asInstanceOf[SparkPlanGraphCluster], checker, sqlID, app,
        reusedNodeIds).parse
    } else {
      val execInfos = try {
        normalizedNodeName match {
          case "AggregateInPandas" =>
            AggregateInPandasExecParser(node, checker, sqlID).parse
          case "ArrowEvalPython" =>
            ArrowEvalPythonExecParser(node, checker, sqlID).parse
          case "BatchScan" =>
            BatchScanExecParser(node, checker, sqlID, app).parse
          case "BroadcastExchange" =>
            BroadcastExchangeExecParser(node, checker, sqlID, app).parse
          case "BroadcastHashJoin" =>
            BroadcastHashJoinExecParser(node, checker, sqlID).parse
          case "BroadcastNestedLoopJoin" =>
            BroadcastNestedLoopJoinExecParser(node, checker, sqlID).parse
          case "CartesianProduct" =>
            CartesianProductExecParser(node, checker, sqlID).parse
          case "Coalesce" =>
            CoalesceExecParser(node, checker, sqlID).parse
          case "CollectLimit" =>
            CollectLimitExecParser(node, checker, sqlID).parse
          case "CustomShuffleReader" | "AQEShuffleRead" =>
            CustomShuffleReaderExecParser(node, checker, sqlID).parse
          case "Exchange" =>
            ShuffleExchangeExecParser(node, checker, sqlID, app).parse
          case "Expand" =>
            ExpandExecParser(node, checker, sqlID).parse
          case "Filter" =>
            FilterExecParser(node, checker, sqlID).parse
          case "FlatMapGroupsInPandas" =>
            FlatMapGroupsInPandasExecParser(node, checker, sqlID).parse
          case "Generate" =>
            GenerateExecParser(node, checker, sqlID).parse
          case "GlobalLimit" =>
            GlobalLimitExecParser(node, checker, sqlID).parse
          case "HashAggregate" =>
            HashAggregateExecParser(node, checker, sqlID, app).parse
          case "LocalLimit" =>
            LocalLimitExecParser(node, checker, sqlID).parse
          case "InMemoryTableScan" =>
            InMemoryTableScanExecParser(node, checker, sqlID).parse
          case i if DataWritingCommandExecParser.isWritingCmdExec(i) =>
            DataWritingCommandExecParser.parseNode(node, checker, sqlID)
          case "MapInPandas" =>
            MapInPandasExecParser(node, checker, sqlID).parse
          case "ObjectHashAggregate" =>
            ObjectHashAggregateExecParser(node, checker, sqlID, app).parse
          case "Project" =>
            ProjectExecParser(node, checker, sqlID).parse
          case "Range" =>
            RangeExecParser(node, checker, sqlID).parse
          case "Sample" =>
            SampleExecParser(node, checker, sqlID).parse
          case "ShuffledHashJoin" =>
            ShuffledHashJoinExecParser(node, checker, sqlID, app).parse
          case "Sort" =>
            SortExecParser(node, checker, sqlID).parse
          case s if ReadParser.isScanNode(s) =>
            FileSourceScanExecParser(node, checker, sqlID, app).parse
          case "SortAggregate" =>
            SortAggregateExecParser(node, checker, sqlID).parse
          case smj if SortMergeJoinExecParser.accepts(smj) =>
            SortMergeJoinExecParser(node, checker, sqlID).parse
          case "SubqueryBroadcast" =>
            SubqueryBroadcastExecParser(node, checker, sqlID, app).parse
          case sqe if SubqueryExecParser.accepts(sqe) =>
            SubqueryExecParser.parseNode(node, checker, sqlID, app)
          case "TakeOrderedAndProject" =>
            TakeOrderedAndProjectExecParser(node, checker, sqlID).parse
          case "Union" =>
            UnionExecParser(node, checker, sqlID).parse
          case "Window" =>
            WindowExecParser(node, checker, sqlID).parse
          case "WindowInPandas" =>
            WindowInPandasExecParser(node, checker, sqlID).parse
          case wfe if WriteFilesExecParser.accepts(wfe) =>
            WriteFilesExecParser(node, checker, sqlID).parse
          case _ =>
            // Execs that are members of reuseExecs (i.e., ReusedExchange) should be marked as
            // supported but with shouldRemove flag set to True.
            // Setting the "shouldRemove" is handled at the end of the function.
            ExecInfo(node, sqlID, normalizedNodeName, expr = "", 1, duration = None, node.id,
              isSupported = reuseExecs.contains(normalizedNodeName), None)
        }
      } catch {
        // Error parsing expression could trigger an exception. If the exception is not handled,
        // the application will be skipped. We need to suppress exceptions here to avoid
        // sacrificing the entire app analysis.
        // Note that:
        //  - The exec will be considered unsupported.
        //  - No need to add the SQL to the failed SQLs, because this will cause the app to be
        //    labeled as "Not Applicable" which is not preferred at this point.
        case NonFatal(e) =>
          logWarning(s"Unexpected error parsing plan node ${normalizedNodeName}. " +
          s" sqlID = ${sqlID}", e)
          ExecInfo(node, sqlID, normalizedNodeName, expr = "", 1, duration = None, node.id,
            isSupported = false, None)
      }
      val stagesInNode = getStagesInSQLNode(node, app)
      execInfos.setStages(stagesInNode)
      // shouldRemove is set to true if the exec is a member of "execsToBeRemoved" or if the node
      // is a duplicate
      execInfos.setShouldRemove(isDupNode)
      // Set the custom reasons for unsupported execs
      val unsupportedExecsReason = checker.getNotSupportedExecsReason(execInfos.exec)
      execInfos.setUnsupportedExecReason(unsupportedExecsReason)
      Seq(execInfos)
    }
  }

  /**
   * This function is used to calculate an average speedup factor. The input
   * is assumed to an array of doubles where each element is >= 1. If the input array
   * is empty we return 1 because we assume we don't slow things down. Generally
   * the array shouldn't be empty, but if there is some weird case we don't want to
   * blow up, just say we don't speed it up.
   */
  def averageSpeedup(arr: Seq[Double]): Double = {
    if (arr.isEmpty) {
      1.0
    } else {
      val sum = arr.sum
      ToolUtils.calculateAverage(sum, arr.size, 2)
    }
  }

  /**
   * Get the total duration by finding the accumulator with the largest value.
   * This is because each accumulator has a value and an update. As tasks end
   * they just update the value = value + update, so the largest value will be
   * the duration.
   */
  def getTotalDuration(accumId: Option[Long], app: AppBase): Option[Long] = {
    val taskForAccum = accumId.flatMap(id => app.taskStageAccumMap.get(id))
      .getOrElse(ArrayBuffer.empty)
    val accumValues = taskForAccum.map(_.value.getOrElse(0L))
    val maxDuration = if (accumValues.isEmpty) {
      None
    } else {
      Some(accumValues.max)
    }
    maxDuration
  }

  def getDriverTotalDuration(accumId: Option[Long], app: AppBase): Option[Long] = {
    val accums = accumId.flatMap(id => app.driverAccumMap.get(id))
      .getOrElse(ArrayBuffer.empty)
    val accumValues = accums.map(_.value)
    val maxDuration = if (accumValues.isEmpty) {
      None
    } else {
      Some(accumValues.max)
    }
    maxDuration
  }

  private def ignoreExpression(expr:String): Boolean = {
    ignoreExpressions.contains(expr.toLowerCase)
  }

  private def getFunctionName(functionPattern: Regex, expr: String): Option[String] = {
    val funcName = functionPattern.findFirstMatchIn(expr) match {
      case Some(func) =>
        val func1 = func.group(1)
        // There are some functions which are not expressions hence should be ignored.
        // For example: In the physical plan cast is usually presented as function call
        // `cast(value#9 as date)`. We add other function names to the result.
        if (!ignoreExpression(func1)) {
          Some(func1)
        } else {
          None
        }
      case _ => logDebug(s"Incorrect expression - $expr")
        None
    }
    funcName
  }

  // This method aims at doing some common processing to an expression before
  // we start parsing it. For example, some special handling is required for some functions.
  private def processSpecialFunctions(expr: String): String = {
    // For parse_url, we only support parse_url(*,Host,*); parse_url(*,Protocol,*)
    // So we want to be able to define that parse_url(*,QUERY,*) is not supported.

    // The following regex uses forward references to find matches for parse_url(*)
    // we need to use forward references because otherwise multiple occurrences will be matched
    // only once.
    // https://stackoverflow.com/questions/47162098/is-it-possible-to-match-nested-brackets-with-a-
    // regex-without-using-recursion-or/47162099#47162099
    // example parse_url:
    // Project [url_col#7, parse_url(url_col#7, HOST, false) AS HOST#9,
    //          parse_url(url_col#7, QUERY, false) AS QUERY#10]
    val parseURLPattern = ("parse_url(?=\\()(?:(?=.*?\\((?!.*?\\1)(.*\\)(?!.*\\2).*))(?=.*?\\)" +
      "(?!.*?\\2)(.*)).)+?.*?(?=\\1)[^(]*(?=\\2$)").r
    var newExpr = expr
    parseURLPattern.findAllMatchIn(expr).foreach { parse_call =>
      // iterate on all matches replacing parse_url by parse_url_query
      // note that we do replaceFirst because we want to map 1-to-1 and the order does
      // not matter here.
      if (parse_call.matched.matches("parse_url\\(.*,\\s*(?i)query\\s*,.*\\)")) {
        newExpr = newExpr.replaceFirst("parse_url\\(", "parse_url_query(")
      }
    }
    newExpr
  }

  private def getAllFunctionNames(regPattern: Regex, expr: String,
      groupInd: Int = 1, isAggr: Boolean = true): Set[String] = {
    // Returns all matches in an expression. This can be used when the SQL expression is not
    // tokenized.
    val newExpr = processSpecialFunctions(expr)

    // first get all the functionNames
    val exprss =
      regPattern.findAllMatchIn(newExpr).map(_.group(groupInd)).toSet

    // For aggregate expressions we want to process the results to remove the prefix
    // DB: remove the "^partial_" and "^finalmerge_" prefixes
    // TODO:
    //    for performance sake, we can turn off the aggregate processing by enabling it only
    //    when needed. However, for now, we always do this processing until we are confident we know
    //    the correct place to turn on/off that flag.we can use the argument isAgg only when needed
    val results = if (isAggr) {
      exprss.collect {
        case func =>
          aggregatePrefixes.find(func.startsWith(_)).map(func.replaceFirst(_, "")).getOrElse(func)
      }
    } else {
      exprss
    }
    results.filterNot(ignoreExpression(_))
  }

  def parseProjectExpressions(exprStr: String): Array[String] = {
    // Project [cast(value#136 as string) AS value#144, CEIL(value#136) AS CEIL(value)#143L]
    // This is to split the string such that only function names are extracted. The pattern is
    // such that function name is succeeded by `(`. We use regex to extract all the function names
    // below:
    getAllFunctionNames(functionPrefixPattern, exprStr).toArray
  }

  // This parser is used for SortAggregateExec, HashAggregateExec and ObjectHashAggregateExec
  def parseAggregateExpressions(exprStr: String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()
    // (keys=[num#83], functions=[partial_collect_list(letter#84, 0, 0), partial_count(letter#84)])
    // Currently we only parse the functions expressions.
    // "Keys" parsing is disabled for now because we won't be able to detect the types

    // A map (value -> parseEnabled) between the group and the parsing metadata
    val patternMap = Map(
      "functions" -> true,
      "keys" -> false
    )
    // It won't hurt to define a pattern that is neutral to the order of the functions/keys.
    // This can avoid mismatches when exprStr comes in the fom of (functions=[], keys=[]).
    val pattern = """^\((keys|functions)=\[(.*)\]\s*,\s*(keys|functions)=\[(.*)\]\s*\)$""".r
    // Iterate through the matches and exclude disabled clauses
    pattern.findAllMatchIn(exprStr).foreach { m =>
      // The matching groups are:
      // 0 -> entire expression
      // 1 -> "keys"; 2 -> keys' expression
      // 3 -> "functions"; 4 -> functions' expression
      Array(1, 3).foreach { group_ind =>
        val group_value = m.group(group_ind)
        if (patternMap.getOrElse(group_value, false)) {
          val clauseExpr = m.group(group_ind + 1)
          // No need to split the expr any further because we are only interested in function names
          val used_functions = getAllFunctionNames(functionPrefixPattern, clauseExpr)
          parsedExpressions ++= used_functions
        }
      }
    }
    parsedExpressions.distinct.toArray
  }

  def parseWindowExpressions(exprStr:String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()
    // [sum(cast(level#30 as bigint)) windowspecdefinition(device#29, id#28 ASC NULLS FIRST,
    // specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS sum#35L,
    // row_number() windowspecdefinition(device#29, id#28 ASC NULLS FIRST, specifiedwindowframe
    // (RowFrame, unboundedpreceding$(), currentrow$())) AS row_number#41], [device#29],
    // [id#28 ASC NULLS FIRST]

    // This splits the string to get only the expressions in WindowExec. So we first split the
    // string on closing bracket ] and get the first element from the array. This is followed
    // by removing the first and last parenthesis and removing the cast as it is not an expr.
    // Lastly we split the string by keyword windowsspecdefinition so that each array element
    // except the last element contains one window aggregate function.
    // sum(level#30 as bigint))
    // (device#29, id#28 ASC NULLS FIRST, .....  AS sum#35L, row_number()
    // (device#29, id#28 ASC NULLS FIRST, ......  AS row_number#41
    val windowExprs = exprStr.split("(?<=\\])")(0).
        trim.replaceAll("""^\[+""", "").replaceAll("""\]+$""", "").
        replaceAll("cast\\(", "").split("windowspecdefinition").map(_.trim)

    // Get function name from each array element except the last one as it doesn't contain
    // any window function
    if (windowExprs.nonEmpty) {
      windowExprs.dropRight(1).foreach { windowExprString =>
        val windowFunc = windowFunctionPattern.findAllIn(windowExprString).toList
        val expr = windowFunc.lastOption.getOrElse("")
        val functionName = getFunctionName(windowFunctionPattern, expr)
        functionName match {
          case Some(func) => parsedExpressions += func
          case _ => // NO OP
        }
      }
    }
    parsedExpressions.distinct.toArray
  }

  def parseExpandExpressions(exprStr: String): Array[String] = {
    // [List(x#1564, hex(y#1455L)#1565, CEIL(z#1456)#1566L, 0),
    // List(x#1564, hex(y#1455L)#1565, null, 1), .......
    // , spark_grouping_id#1567L]
    // For Spark320+, the expandExpressions has different format
    //  [[x#23, CEIL(y#11L)#24L, hex(cast(z#12 as bigint))#25, 0]
    // Parsing:
    // The goal is to extract all valid functions from the expand.
    // It is important to take the following into considerations:
    //  - Some values can be NULLs. That's why we cannot limit the extract to the first row.
    //  - Nested brackets/parenthesis makes it challenging to use regex that contains
    //    brackets/parenthesis to extract expressions.
    // The implementation Use regex to extract all function names and return distinct set of
    // function names.
    // This implementation is 1 line implementation, but it can be a memory/time bottleneck.
    getAllFunctionNames(functionPrefixPattern, exprStr).toArray
  }

  def parseTakeOrderedExpressions(exprStr: String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()
    // (limit=2, orderBy=[FLOOR(z#796) ASC NULLS FIRST,
    // CEIL(y#795L) ASC NULLS FIRST,y#1588L ASC NULLS FIRST], output=[x#794,y#796L,z#795])
    val pattern = """orderBy=\[([\w#, \(\)]+\])""".r
    val orderString = pattern.findFirstMatchIn(exprStr)
    // This is to split multiple column names in orderBy clause of parse TakeOrderedAndProjectExec.
    // First we remove orderBy from the string and then split the resultant string.
    // The string is split on delimiter containing FIRST, OR LAST, which is the last string
    // of each column in this Exec that produces an array containing
    // column names. Finally we remove the parentheses from the beginning and end to get only
    // the expressions. Result will be as below.
    // Array(FLOOR(z#796) ASC NULLS FIRST,, CEIL(y#795L) ASC NULLS FIRST)
    if (orderString.isDefined) {
      val parenRemoved = orderString.get.toString.replaceAll("orderBy=", "").
        split("(?<=FIRST,)|(?<=LAST,)").map(_.trim).map(
        _.replaceAll("""^\[+""", "").replaceAll("""\]+$""", ""))
      parenRemoved.foreach { expr =>
        val functionName = getFunctionName(functionPattern, expr)
        functionName match {
          case Some(func) => parsedExpressions += func
          case _ => // NO OP
        }
      }
    }
    parsedExpressions.distinct.toArray
  }

  def parseGenerateExpressions(exprStr: String): Array[String] = {
    // Get the function names from the GenerateExec. The GenerateExec has the following format:
    // 1. Generate explode(arrays#1306), [id#1304], true, [col#1426]
    // 2. Generate json_tuple(values#1305, Zipcode, ZipCodeType, City), [id#1304],
    // false, [c0#1407, c1#1408, c2#1409]
    getAllFunctionNames(functionPrefixPattern, exprStr).toArray
  }

   // This parser is used for BroadcastHashJoin, ShuffledHashJoin and SortMergeJoin
   def parseEquijoinsExpressions(exprStr: String): (Array[String], Boolean) = {
     // ShuffledHashJoin [name#11, CEIL(DEPT#12)], [name#28, CEIL(DEPT_ID#27)], Inner, BuildLeft
     // SortMergeJoin [name#11, CEIL(dept#12)], [name#28, CEIL(dept_id#27)], Inner
     // BroadcastHashJoin [name#11, CEIL(dept#12)], [name#28, CEIL(dept_id#27)], Inner,
     // BuildRight, false
     // BroadcastHashJoin exprString: [i_item_id#56], [i_item_id#56#86], ExistenceJoin(exists#86),
     // BuildRight
     val parsedExpressions = ArrayBuffer[String]()
     // Get all the join expressions and split it with delimiter :: so that it could be used to
     // parse function names (if present) later.
     val joinExprs = equiJoinRegexPattern.findAllMatchIn(exprStr).mkString("::")
     // Get joinType and buildSide(if applicable)
     val joinParams = equiJoinRegexPattern.replaceAllIn(
       exprStr, "").split(",").map(_.trim).filter(_.nonEmpty)
     val joinType = if (joinParams.nonEmpty) {
       joinParams(0).split("\\(")(0).trim
     } else {
       ""
     }
     // SortMergeJoin doesn't have buildSide, assign empty string in that case
     val buildSide = if (joinParams.length > 1) {
       joinParams(1).trim
     } else {
       ""
     }
     // Get individual expressions which is later used to get the function names.
     val expressions = joinExprs.split("::").map(_.trim).map(
       _.replaceAll("""^\[+|\]+$""", "")).map(_.split(",")).flatten.map(_.trim)

     expressions.foreach { expr =>
       val functionName = getFunctionName(functionPattern, expr)
       functionName.foreach(parsedExpressions += _)
     }

     (parsedExpressions.distinct.toArray, equiJoinSupportedTypes(buildSide, joinType))
   }

  def parseNestedLoopJoinExpressions(exprStr: String): (Array[String], Boolean) = {
    // BuildRight, LeftOuter, ((CEIL(cast(id1#1490 as double)) <= cast(id2#1496 as bigint))
    // AND (cast(id1#1490 as bigint) < CEIL(cast(id2#1496 as double))))
    // Get joinType and buildSide by splitting the input string.
    val nestedLoopParameters = exprStr.split(",", 3)
    val buildSide = nestedLoopParameters(0).trim
    val joinType = nestedLoopParameters(1).trim

    // Check if condition present on join columns else return empty array
    val parsedExpressions = if (nestedLoopParameters.size > 2) {
      parseConditionalExpressions(exprStr)
    } else {
      Array[String] ()
    }
    (parsedExpressions, nestedLoopJoinSupportedTypes(buildSide, joinType))
  }

  private def isJoinTypeSupported(joinType: String): Boolean = {
    // There is caveat for FullOuter join for equiJoins.
    // FullOuter join id not supported with struct keys but we are sending true for all
    // data structures.
    joinType match {
      case JoinType.Cross => true
      case JoinType.Inner => true
      case JoinType.LeftSemi => true
      case JoinType.FullOuter => true
      case JoinType.LeftOuter => true
      case JoinType.RightOuter => true
      case JoinType.LeftAnti => true
      case JoinType.ExistenceJoin => true
      case _ => false
    }
  }

  private def equiJoinSupportedTypes(buildSide: String, joinType: String): Boolean = {
    val joinTypeSupported = isJoinTypeSupported(joinType)
    // We are checking if the joinType is supported for the buildSide. If the buildSide is not
    // in the supportedBuildSides map then we are assuming that the
    // joinType is supported for that buildSide.
    val buildSideSupported = BuildSide.supportedBuildSides.getOrElse(
      buildSide, JoinType.allsupportedJoinType).contains(joinType)

    joinTypeSupported && buildSideSupported
  }

  private def nestedLoopJoinSupportedTypes(buildSide: String, joinType: String): Boolean = {
    // Full Outer join not supported in BroadcastNestedLoopJoin
    val joinTypeSupported = if (joinType != JoinType.FullOuter) {
      isJoinTypeSupported(joinType)
    } else {
      false
    }
    // This is from GpuBroadcastNestedLoopJoinMeta.tagPlanForGpu where join is
    // not supported on GPU if below condition is met.
    val buildSideNotSupported = if (buildSide == BuildSide.BuildLeft) {
      joinType == JoinType.LeftOuter || joinType == JoinType.LeftSemi ||
        joinType == JoinType.LeftAnti
    } else if (buildSide == BuildSide.BuildRight) {
      joinType == JoinType.RightOuter
    } else {
      false
    }
    joinTypeSupported && !buildSideNotSupported
  }

  def parseSortExpressions(exprStr: String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()
    // Sort [round(num#126, 0) ASC NULLS FIRST, letter#127 DESC NULLS LAST], true, 0
    val pattern = """\[([\w#, \(\)]+\])""".r
    val sortString = pattern.findFirstMatchIn(exprStr)
    // This is to split multiple column names in SortExec. Project may have a function on a column.
    // The string is split on delimiter containing FIRST, OR LAST, which is the last string
    // of each column in SortExec that produces an array containing
    // column names. Finally we remove the parentheses from the beginning and end to get only
    // the expressions. Result will be as below.
    // paranRemoved = Array(round(num#7, 0) ASC NULLS FIRST,, letter#8 DESC NULLS LAST)
    if (sortString.isDefined) {
      val paranRemoved = sortString.get.toString.split("(?<=FIRST,)|(?<=LAST,)").
          map(_.trim).map(_.replaceAll("""^\[+""", "").replaceAll("""\]+$""", ""))
      paranRemoved.foreach { expr =>
        val functionName = getFunctionName(functionPattern, expr)
        functionName match {
          case Some(func) => parsedExpressions += func
          case _ => // NO OP
        }
      }
    }
    parsedExpressions.distinct.toArray
  }

  def parseFilterExpressions(exprStr: String): Array[String] = {
    // Filter ((isnotnull(s_state#68) AND (s_state#68 = TN)) OR (hex(cast(value#0 as bigint)) = B))
    parseConditionalExpressions(exprStr)
  }

  // The scope is to extract expressions from a conditional expression.
  // Ideally, parsing conditional expressions needs to build a tree. The current implementation is
  // a simplified version that does not accurately pickup the LHS and RHS of each predicate.
  // Instead, it extracts function names, and expressions in best effort.
  def parseConditionalExpressions(exprStr: String): Array[String] = {
    // Captures any word followed by '('
    // isnotnull(, StringEndsWith(
    val functionsRegEx = """((\w+))\(""".r
    // Captures binary operators followed by '('
    // AND(, OR(, NOT(, =(, <(, >(
    val binaryOpsNoSpaceRegEx = """(^|\s+)((AND|OR|NOT|IN|=|<=>|<|>|>=|\++|-|\*+))(\(+)""".r
    // Capture reserved words at the end of expression. Those should be considered literal
    // and hence are ignored.
    // Binary operators cannot be at the end of the string, or end of expression.
    // For example we know that the following AND is a literal value, not the operator AND.
    // So, we can filter that out from the results.
    //     PushedFilters: [IsNotNull(c_customer_id), StringEndsWith(c_customer_id,AND)]
    //     Filter (isnotnull(names#15) AND StartsWith(names#15, AND))
    // AND), AND$
    val nonBinaryOperatorsRegEx = """\s+((AND|OR|NOT|=|<=>|<|>|>=|\++|-|\*+))($|\)+)""".r
    // Capture all "("
    val parenthesisStartRegEx = """(\(+)""".r
    // Capture all ")"
    val parenthesisEndRegEx = """(\)+)""".r

    val parsedExpressions = ArrayBuffer[String]()
    var processedExpr = exprStr
    // Step-1: make sure that any binary operator won't mix up with functionNames
    // For example AND(, isnotNull()
    binaryOpsNoSpaceRegEx.findAllMatchIn(exprStr).foreach { m =>
      // replace things like 'AND(' with 'AND ('
      val str = s"${m.group(2)}\\(+"
      processedExpr = str.r.replaceAllIn(processedExpr, s"${m.group(2)} \\(")
    }

    // Step-2: Extract function names from the expression
    val functionMatches = functionsRegEx.findAllMatchIn(processedExpr)
    parsedExpressions ++=
      functionMatches.map(_.group(1)).filterNot(ignoreExpression(_))
    // remove all function calls. No need to keep them in the expression
    processedExpr = functionsRegEx.replaceAllIn(processedExpr, " ")

    // Step-3: remove literal variables so we do not treat them as Binary operators
    // Simply replace them by white space.
    processedExpr = nonBinaryOperatorsRegEx.replaceAllIn(processedExpr, " ")

    // Step-4: remove remaining parentheses '(', ')' and commas if we had functionCalls
    if (!functionMatches.isEmpty) {
      // remove ","
      processedExpr = processedExpr.replaceAll(",", " ")
    }
    processedExpr = parenthesisStartRegEx.replaceAllIn(processedExpr, " ")
    processedExpr = parenthesisEndRegEx.replaceAllIn(processedExpr, " ")

    // Step-5: now we should have a simplified expression that can be tokenized on white
    // space delimiter
    processedExpr.split("\\s+").foreach { token =>
      token match {
        case "NOT" => parsedExpressions += "Not"
        case "=" => parsedExpressions += "EqualTo"
        case "<=>" => parsedExpressions += "EqualNullSafe"
        case "<" => parsedExpressions += "LessThan"
        case ">" => parsedExpressions += "GreaterThan"
        case "<=" => parsedExpressions += "LessThanOrEqual"
        case ">=" => parsedExpressions += "GreaterThanOrEqual"
        case "+" => parsedExpressions += "Add"
        case "-" => parsedExpressions += "Subtract"
        case "*" => parsedExpressions += "Multiply"
        case "IN" => parsedExpressions += "In"
        case "OR" | "||" =>
          // Some Spark2.x eventlogs may have '||' instead of 'OR'
          parsedExpressions += "Or"
        case "&&" | "AND" =>
          // Some Spark2.x eventlogs may have '&&' instead of 'AND'
          parsedExpressions += "And"
        case t if t.contains("#") =>
          // This is a variable name. Ignore those ones.
        case _ =>
          // anything else could be a literal value or we do not handle yet. Ignore them for now.
          logDebug(s"Unrecognized Token - $token")
      }
    }

    parsedExpressions.distinct.toArray
  }
}
