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

package org.apache.spark.sql.rapids.tool

import java.io.InputStream
import java.util.zip.GZIPInputStream

import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, LinkedHashSet, Map, SortedMap}

import com.nvidia.spark.rapids.SparkRapidsBuildInfoEvent
import com.nvidia.spark.rapids.tool.{DatabricksEventLog, DatabricksRollingEventLogFilesFileReader, EventLogInfo}
import com.nvidia.spark.rapids.tool.planparser.{HiveParseHelper, ReadParser}
import com.nvidia.spark.rapids.tool.planparser.HiveParseHelper.isHiveTableScanNode
import com.nvidia.spark.rapids.tool.profiling.{BlockManagerRemovedCase, DataSourceCase, DriverAccumCase, JobInfoClass, ResourceProfileInfoCase, SQLExecutionInfoClass, SQLPlanMetricsCase}
import com.nvidia.spark.rapids.tool.qualification.AppSubscriber
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.deploy.history.{EventLogFileReader, EventLogFileWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListenerEvent, StageInfo}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.store.{AccumManager, StageModel, StageModelManager, TaskModelManager}
import org.apache.spark.sql.rapids.tool.util.{EventUtils, RapidsToolsConfUtil, ToolsPlanGraph, UTF8Source}
import org.apache.spark.util.Utils

abstract class AppBase(
    val eventLogInfo: Option[EventLogInfo],
    val hadoopConf: Option[Configuration]) extends Logging with ClusterTagPropHandler {

  var appMetaData: Option[AppMetaData] = None

  // appId is string is stored as a field in the AppMetaData class
  def appId: String = {
    appMetaData match {
      case Some(meta) => meta.appId.getOrElse("")
      case _ => ""
    }
  }

  lazy val attemptId: Int = appMetaData.map(_.attemptId).getOrElse(1)


  // This is to keep track of the high water mark for maximum number of executors
  // active at any point in time.
  // Dynamic allocation and failures could change the number of
  // executors over time so we are tracking what was the maximum used.
  var maxNumExecutorsRunning = 0
  // high water mark for maximum number of nodes in use at any point in time
  var maxNumNodesRunning = 0
  // Store map of executorId to executor info
  val executorIdToInfo = new HashMap[String, ExecutorInfoClass]()
  // resourceProfile id to resource profile info
  val resourceProfIdToInfo = new HashMap[Int, ResourceProfileInfoCase]()
  var blockManagersRemoved: ArrayBuffer[BlockManagerRemovedCase] =
    ArrayBuffer[BlockManagerRemovedCase]()
  // The data source information
  val dataSourceInfo: ArrayBuffer[DataSourceCase] = ArrayBuffer[DataSourceCase]()

  // jobId to job info
  val jobIdToInfo = new HashMap[Int, JobInfoClass]()
  val jobIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]

  // SQL containing any Dataset operation or RDD to DataSet/DataFrame operation
  val sqlIDToDataSetOrRDDCase: HashSet[Long] = HashSet[Long]()
  // Map (sqlID <-> String(problematic issues))
  // Use LinkedHashSet of Strings to preserve the order of insertion.
  val sqlIDtoProblematic: HashMap[Long, LinkedHashSet[String]] =
    HashMap[Long, LinkedHashSet[String]]()
  // sqlId to sql info
  val sqlIdToInfo = new HashMap[Long, SQLExecutionInfoClass]()
  val sqlIdToStages = new HashMap[Long, ArrayBuffer[Int]]()
  // sqlPlans stores HashMap (sqlID <-> SparkPlanInfo)
  // SortedMap is used to keep the order of the sqlPlans since AQEs can overrides the existing ones
  var sqlPlans: Map[Long, SparkPlanInfo] = SortedMap[Long, SparkPlanInfo]()
  var sqlPlanMetricsAdaptive: ArrayBuffer[SQLPlanMetricsCase] = ArrayBuffer[SQLPlanMetricsCase]()

  // accum id to task stage accum info
  lazy val accumManager: AccumManager = new AccumManager()

  lazy val stageManager: StageModelManager = new StageModelManager()
  // Container that manages TaskIno including SparkMetrics.
  // A task is added during a TaskEnd eventLog
  lazy val taskManager: TaskModelManager = new TaskModelManager()

  var driverAccumMap: HashMap[Long, ArrayBuffer[DriverAccumCase]] =
    HashMap[Long, ArrayBuffer[DriverAccumCase]]()

  var sparkRapidsBuildInfo: SparkRapidsBuildInfoEvent = SparkRapidsBuildInfoEvent(immutable.Map(),
    immutable.Map(), immutable.Map(), immutable.Map())

  // Returns the String value of the eventlog or empty if it is not defined. Note that the eventlog
  // won't be defined for running applications
  def getEventLogPath: String = {
    eventLogInfo.map(_.eventLog).getOrElse(new Path("")).toString
  }

  // Update the endTime of the application and calculate the duration.
  // This is called while handling ApplicationEnd event
  def updateEndTime(newEndTime: Long): Unit = {
    appMetaData.foreach(_.setEndTime(newEndTime))
  }

  // Returns a boolean flag to indicate whether the endTime was estimated.
  def isAppDurationEstimated: Boolean = {
    appMetaData.map(_.isDurationEstimated).getOrElse(false)
  }

  // Returns the AppName
  def getAppName: String = {
    appMetaData.map(_.appName).getOrElse("")
  }

  // Returns optional endTime in ms.
  def getAppEndTime: Option[Long] = {
    appMetaData.flatMap(_.endTime)
  }

  // Returns optional wallClock duration of the Application
  def getAppDuration: Option[Long] = {
    appMetaData.flatMap(_.duration)
  }

  // Returns a boolean true/false. This is used to check whether processing an eventlog was
  // successful.
  def isAppMetaDefined: Boolean = appMetaData.isDefined

  /**
   * Sets an estimated endTime to the application based on the function passed as an argument.
   * First it checks if the endTime is already defined or not.
   * This method is a temporary refactor because both QualAppInfo and ProfAppInfo have different
   * ways of estimating the endTime.
   *
   * @param callBack function to estimate the endTime
   */
  def estimateAppEndTime(callBack: () => Option[Long]): Unit = {
    if (getAppEndTime.isEmpty) {
      val estimatedResult = callBack()
      estimatedResult.foreach(eT => appMetaData.foreach(_.setEndTime(eT, estimated = true)))
    }
  }

  def guestimateAppEndTimeCB(): () => Option[Long] = {
    () => None
  }

  // time in ms
  def calculateAppDuration(): Option[Long] = {
    if (appMetaData.isDefined) {
      val appMeta = appMetaData.get
      val startTime = appMeta.startTime
      if (startTime > 0) {
        estimateAppEndTime(guestimateAppEndTimeCB())
      }
      getAppDuration
    } else {
      None
    }
  }

  /**
   * Calculates total core seconds which is the sum over executor core seconds. Executor
   * core seconds is computed as executor duration (s) multiplied by num of cores.
   */
  def calculateTotalCoreSec(): Long = {
    var totalCoreSec: Double = 0
    executorIdToInfo.foreach { case(_, eInfo) =>
      val eStartTime = eInfo.addTime
      var eEndTime = eInfo.removeTime
      if (eEndTime == 0L) {
        getAppEndTime match {
          case Some(appEndTime) =>
            eEndTime = appEndTime
          case None =>
            logInfo("Unable to find either executor or app end time: " +
              "setting executor duration to 0")
            eEndTime = eStartTime
        }
      }
      totalCoreSec += (eEndTime - eStartTime).toDouble / 1000 * eInfo.totalCores
    }
    // round up for edge case when total core seconds is in range [0, 1)
    math.ceil(totalCoreSec).toLong
  }

  def getOrCreateExecutor(executorId: String, addTime: Long): ExecutorInfoClass = {
    executorIdToInfo.getOrElseUpdate(executorId, {
      new ExecutorInfoClass(executorId, addTime)
    })
  }

  // this is to keep track of the high water mark for number of executors
  // active at anyone instant in time
  def updateMaxExecutorsIfNeeded(): Unit = {
    val numActiveExecutors = executorIdToInfo.values.filter(_.isActive).size
    if (numActiveExecutors > maxNumExecutorsRunning) {
      maxNumExecutorsRunning = numActiveExecutors
    }
  }

  // this is to keep track of the high water mark for number of nodes
  // active at anyone instant in time
  def updateMaxNodesIfNeeded(): Unit = {
    // make this a set to make it dedup nodes
    val numActiveNodes = executorIdToInfo.values.filter(_.isActive).map(_.host).toSet.size
    logWarning("max nodes is: " + numActiveNodes + " app id: " + appId + " nodes: " +
      executorIdToInfo.values.filter(_.isActive).map(_.host).toSet.mkString(","))
    if (numActiveNodes > maxNumNodesRunning) {
      maxNumNodesRunning = numActiveNodes
    }
  }

  def getOrCreateStage(info: StageInfo): StageModel = {
    val stage = stageManager.addStageInfo(info)
    stage
  }

  def getAllStagesForJobsInSqlQuery(sqlID: Long): Seq[Int] = {
    val jobsIdsInSQLQuery = jobIdToSqlID.filter { case (_, sqlIdForJob) =>
      sqlIdForJob == sqlID
    }.keys.toSeq
    jobsIdsInSQLQuery.flatMap { jId =>
      jobIdToInfo.get(jId).map(_.stageIds)
    }.flatten
  }

  def cleanupAccumId(accId: Long): Unit = {
    accumManager.removeAccumInfo(accId)
    driverAccumMap.remove(accId)
  }

  def cleanupStages(stageIds: Set[Int]): Unit = {
    // stageIdToInfo can have multiple stage attempts, remove all of them
    stageManager.removeStages(stageIds)
  }

  def cleanupSQL(sqlID: Long): Unit = {
    sqlIDToDataSetOrRDDCase.remove(sqlID)
    sqlIDtoProblematic.remove(sqlID)
    sqlIdToInfo.remove(sqlID)
    sqlPlans.remove(sqlID)
    val dsToRemove = dataSourceInfo.filter(_.sqlID == sqlID)
    dsToRemove.foreach(dataSourceInfo -= _)

    val jobsInSql = jobIdToSqlID.filter { case (_, sqlIdForJob) =>
      sqlIdForJob == sqlID
    }.keys
    jobsInSql.foreach { jobId =>
      // must call cleanupStage first
      // clean when no other jobs need that stage
      // not sure about races here but lets check the jobs and assume we can clean
      // when none of them reference this stage
      val stagesNotInOtherJobs = jobIdToInfo.get(jobId).map { jInfo =>
        val stagesInJobToRemove = jInfo.stageIds.toSet
        // check to make sure no other jobs reference the same stage
        val allOtherJobs = jobIdToInfo - jobId
        val allOtherStageIds = allOtherJobs.values.flatMap(_.stageIds).toSet
        stagesInJobToRemove.filter(!allOtherStageIds.contains(_))
      }
      stagesNotInOtherJobs.foreach(cleanupStages(_))
      jobIdToSqlID.remove(_)
      jobIdToInfo.remove(_)
    }
  }

  def processEvent(event: SparkListenerEvent): Boolean

  private def openEventLogInternal(log: Path, fs: FileSystem): InputStream = {
    EventLogFileWriter.codecName(log) match {
      case c if c.isDefined && c.get.equals("gz") =>
        val in = fs.open(log)
        try {
          new GZIPInputStream(in)
        } catch {
          case e: Throwable =>
            in.close()
            throw e
        }
      case _ => EventLogFileReader.openEventLog(log, fs)
    }
  }

  /**
   * Internal function to process all the events
   */
  private def processEventsInternal(): Unit = {
    eventLogInfo match {
      case Some(eventLog) =>
        val eventLogPath = eventLog.eventLog
        logInfo("Start Parsing Event Log: " + eventLogPath.toString)

        // at this point all paths should be valid event logs or event log dirs
        val hconf = hadoopConf.getOrElse(RapidsToolsConfUtil.newHadoopConf)
        val fs = eventLogPath.getFileSystem(hconf)
        var totalNumEvents = 0L
        val readerOpt = eventLog match {
          case _: DatabricksEventLog =>
            Some(new DatabricksRollingEventLogFilesFileReader(fs, eventLogPath))
          case _ => EventLogFileReader(fs, eventLogPath)
        }

        if (readerOpt.isDefined) {
          val reader = readerOpt.get
          val runtimeGetFromJsonMethod = EventUtils.getEventFromJsonMethod
          reader.listEventLogFiles.foreach { file =>
            Utils.tryWithResource(openEventLogInternal(file.getPath, fs)) { in =>
              UTF8Source.fromInputStream(in).getLines().find { line =>
                // Using find as foreach with conditional to exit early if we are done.
                // Do NOT use a while loop as it is much much slower.
                totalNumEvents += 1
                runtimeGetFromJsonMethod.apply(line) match {
                  case Some(e) => processEvent(e)
                  case None => false
                }
              }
            }
          }
        } else {
          logError(s"Error getting reader for ${eventLogPath.getName}")
        }
        logInfo(s"Total number of events parsed: $totalNumEvents for ${eventLogPath.toString}")
      case None => logInfo("Streaming events to application")
    }
  }

  private val UDFRegex = ".*UDF.*"

  private val potentialIssuesRegexMap = Map(
    UDFRegex -> "UDF",
    ".*current_timestamp\\(.*\\).*" -> "TIMEZONE current_timestamp()",
    ".*to_timestamp\\(.*\\).*" -> "TIMEZONE to_timestamp()",
    ".*hour\\(.*\\).*" -> "TIMEZONE hour()",
    ".*minute\\(.*\\).*" -> "TIMEZONE minute()",
    ".*second\\(.*\\).*" -> "TIMEZONE second()"
  )

  def findPotentialIssues(desc: String): Set[String] =  {
    val potentialIssuesRegexs = potentialIssuesRegexMap
    val issues = potentialIssuesRegexs.filterKeys(desc.matches(_))
    issues.values.toSet
  }

  /**
   * Builds cluster information based on executor nodes.
   * If executor nodes exist, calculates the number of hosts and total cores,
   * and extracts executor and driver instance types (databricks only)
   */
  protected def buildClusterInfo: Unit = {}

  // The ReadSchema metadata is only in the eventlog for DataSource V1 readers
  def checkMetadataForReadSchema(
      sqlPlanInfoGraph: SqlPlanInfoGraphEntry): ArrayBuffer[DataSourceCase] = {
    // check if planInfo has ReadSchema
    val allMetaWithSchema = AppBase.getPlanMetaWithSchema(sqlPlanInfoGraph.planInfo)
    val allNodes = sqlPlanInfoGraph.sparkPlanGraph.allNodes
    val results = ArrayBuffer[DataSourceCase]()

    allMetaWithSchema.foreach { plan =>
      val meta = plan.metadata
      val readSchema = ReadParser.formatSchemaStr(meta.getOrElse("ReadSchema", ""))
      val scanNode = allNodes.filter(node => {
        // Get ReadSchema of each Node and sanitize it for comparison
        val trimmedNode = AppBase.trimSchema(ReadParser.parseReadNode(node).schema)
        readSchema.contains(trimmedNode)
      }).filter(ReadParser.isScanNode(_))

      // If the ReadSchema is empty or if it is PhotonScan, then we don't need to
      // add it to the dataSourceInfo
      // Processing Photon eventlogs issue: https://github.com/NVIDIA/spark-rapids-tools/issues/251
      if (scanNode.nonEmpty) {
        results += DataSourceCase(
          sqlPlanInfoGraph.sqlID,
          scanNode.head.id,
          ReadParser.extractTagFromV1ReadMeta("Format", meta),
          ReadParser.extractTagFromV1ReadMeta("Location", meta),
          ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_PUSHED_FILTERS, meta),
          readSchema,
          ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_DATA_FILTERS, meta),
          ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_PARTITION_FILTERS, meta)
        )
      }
    }
    // "scan hive" has no "ReadSchema" defined. So, we need to look explicitly for nodes
    // that are scan hive and add them one by one to the dataSource
    if (hiveEnabled) { // only scan for hive when the CatalogImplementation is using hive
      val allPlanWithHiveScan = AppBase.getPlanInfoWithHiveScan(sqlPlanInfoGraph.planInfo)
      allPlanWithHiveScan.foreach { hiveReadPlan =>
        val sqlGraph = ToolsPlanGraph(hiveReadPlan)
        val hiveScanNode = sqlGraph.allNodes.head
        val scanHiveMeta = HiveParseHelper.parseReadNode(hiveScanNode)
        results += DataSourceCase(
          sqlPlanInfoGraph.sqlID,
          hiveScanNode.id,
          scanHiveMeta.format,
          scanHiveMeta.location,
          scanHiveMeta.pushedFilters,
          scanHiveMeta.schema,
          scanHiveMeta.dataFilters,
          scanHiveMeta.partitionFilters
        )
      }
    }
    dataSourceInfo ++= results
    results
  }

  // This will find scans for DataSource V2, if the schema is very large it
  // will likely be incomplete and have ... at the end.
  def checkGraphNodeForReads(
      sqlID: Long, node: SparkPlanGraphNode): Option[DataSourceCase] = {
    if (ReadParser.isDataSourceV2Node(node)) {
      val res = ReadParser.parseReadNode(node)
      val dsCase = DataSourceCase(
        sqlID,
        node.id,
        res.format,
        res.location,
        res.pushedFilters,
        res.schema,
        res.dataFilters,
        res.partitionFilters)
      dataSourceInfo += dsCase
      Some(dsCase)
    } else {
      None
    }
  }

  protected def reportComplexTypes: (Seq[String], Seq[String]) = {
    if (dataSourceInfo.nonEmpty) {
      val schema = dataSourceInfo.map { ds => ds.schema }
      AppBase.parseReadSchemaForNestedTypes(schema)
    } else {
      (Seq(), Seq())
    }
  }

  protected def probNotDataset: HashMap[Long, LinkedHashSet[String]] = {
    sqlIDtoProblematic.filterNot { case (sqlID, _) => sqlIDToDataSetOrRDDCase.contains(sqlID) }
  }

  protected def getPotentialProblemsForDf: Seq[String] = {
    probNotDataset.values.flatten.toSet.toSeq
  }

  /**
   * Registers the attempt ID for the application and updates the tracker map if the attemptId is
   * greater than the existing attemptId.
   */
  def registerAttemptId(): Unit = {
    if (isAppMetaDefined) {
      val currentAttemptId = sparkProperties.getOrElse("spark.app.attempt.id", "1").toInt
      appMetaData.foreach(_.setAttemptId(currentAttemptId))
      AppSubscriber.subscribeAppAttempt(appId, currentAttemptId)
    }
  }

  protected def postCompletion(): Unit = {
    registerAttemptId()
    calculateAppDuration()
  }

  /**
   * Wrapper function to process all the events followed by any
   * post completion tasks.
   */
  def processEvents(): Unit = {
    processEventsInternal()
    postCompletion()
  }
}

object AppBase {

  def parseReadSchemaForNestedTypes(
      schema: ArrayBuffer[String]): (Seq[String], Seq[String]) = {
    val tempStringBuilder = new StringBuilder()
    val individualSchema: ArrayBuffer[String] = new ArrayBuffer()
    var angleBracketsCount = 0
    var parenthesesCount = 0
    val distinctSchema = schema.distinct.filter(_.nonEmpty).mkString(",")

    // Get the nested types i.e everything between < >
    for (char <- distinctSchema) {
      char match {
        case '<' => angleBracketsCount += 1
        case '>' => angleBracketsCount -= 1
        // If the schema has decimals, Example decimal(6,2) then we have to make sure it has both
        // opening and closing parentheses(unless the string is incomplete due to V2 reader).
        case '(' => parenthesesCount += 1
        case ')' => parenthesesCount -= 1
        case _ =>
      }
      if (angleBracketsCount == 0 && parenthesesCount == 0 && char.equals(',')) {
        individualSchema += tempStringBuilder.toString
        tempStringBuilder.setLength(0)
      } else {
        tempStringBuilder.append(char)
      }
    }
    if (tempStringBuilder.nonEmpty) {
      individualSchema += tempStringBuilder.toString
    }

    // If DataSource V2 is used, then Schema may be incomplete with ... appended at the end.
    // We determine complex types and nested complex types until ...
    val incompleteSchema = individualSchema.filter(x => x.contains("..."))
    val completeSchema = individualSchema.filterNot(x => x.contains("..."))

    // Check if it has types
    val incompleteTypes = incompleteSchema.map { x =>
      if (x.contains("...") && x.contains(":")) {
        val schemaTypes = x.split(":", 2)
        if (schemaTypes.size == 2) {
          val partialSchema = schemaTypes(1).split("\\.\\.\\.")
          if (partialSchema.size == 1) {
            partialSchema(0)
          } else {
            ""
          }
        } else {
          ""
        }
      } else {
        ""
      }
    }
    // Omit columnName and get only schemas
    val completeTypes = completeSchema.map { x =>
      val schemaTypes = x.split(":", 2)
      if (schemaTypes.size == 2) {
        schemaTypes(1)
      } else {
        ""
      }
    }
    val schemaTypes = completeTypes ++ incompleteTypes

    // Filter only complex types.
    // Example: array<string>, array<struct<string, string>>
    val complexTypes = schemaTypes.filter(x =>
      x.startsWith("array<") || x.startsWith("map<") || x.startsWith("struct<"))

    // Determine nested complex types from complex types
    // Example: array<struct<string, string>> is nested complex type.
    val nestedComplexTypes = complexTypes.filter(complexType => {
      val startIndex = complexType.indexOf('<')
      val closedBracket = complexType.lastIndexOf('>')
      // If String is incomplete due to dsv2, then '>' may not be present. In that case traverse
      // until length of the incomplete string
      val lastIndex = if (closedBracket == -1) {
        complexType.length - 1
      } else {
        closedBracket
      }
      val string = complexType.substring(startIndex, lastIndex + 1)
      string.contains("array<") || string.contains("struct<") || string.contains("map<")
    })

    (complexTypes.filter(_.nonEmpty), nestedComplexTypes.filter(_.nonEmpty))
  }

  private def trimSchema(str: String): String = {
    val index = str.lastIndexOf(",")
    if (index != -1 && str.contains("...")) {
      str.substring(0, index)
    } else {
      str
    }
  }

  private def getPlanMetaWithSchema(planInfo: SparkPlanInfo): Seq[SparkPlanInfo] = {
    // TODO: This method does not belong to AppBase. It should move to another member.
    val childRes = planInfo.children.flatMap(getPlanMetaWithSchema(_))
    if (planInfo.metadata != null && planInfo.metadata.contains("ReadSchema")) {
      childRes :+ planInfo
    } else {
      childRes
    }
  }

  // Finds all the nodes that scan a hive table
  private def getPlanInfoWithHiveScan(planInfo: SparkPlanInfo): Seq[SparkPlanInfo] = {
    // TODO: This method does not belong to AppBAse. It should move to another member.
    val childRes = planInfo.children.flatMap(getPlanInfoWithHiveScan(_))
    if (isHiveTableScanNode(planInfo.nodeName)) {
      childRes :+ planInfo
    } else {
      childRes
    }
  }

  def handleException(e: Exception, path: EventLogInfo): FailureApp = {
    val (status, message): (String, String) = e match {
      case incorrectStatusEx: IncorrectAppStatusException =>
        ("unknown", incorrectStatusEx.getMessage)
      case skippedEx: AppEventlogProcessException =>
        ("skipped", skippedEx.getMessage)
      case _: com.fasterxml.jackson.core.JsonParseException =>
        ("unknown", s"Error parsing JSON: ${path.eventLog.toString}")
      case _: IllegalArgumentException =>
        ("unknown", s"Error parsing file: ${path.eventLog.toString}")
      case ue: Exception =>
        // catch all exceptions and skip that file
        ("unknown", s"Got unexpected exception processing file:" +
          s"${path.eventLog.toString}. ${ue.getMessage} ")
    }

    FailureApp(status, s"${e.getClass.getSimpleName}: $message")
  }
}
