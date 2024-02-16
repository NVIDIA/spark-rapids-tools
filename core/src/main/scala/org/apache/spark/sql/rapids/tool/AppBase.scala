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

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.io.{Codec, Source}

import com.nvidia.spark.rapids.tool.{DatabricksEventLog, DatabricksRollingEventLogFilesFileReader, EventLogInfo}
import com.nvidia.spark.rapids.tool.planparser.{HiveParseHelper, ReadParser}
import com.nvidia.spark.rapids.tool.planparser.HiveParseHelper.isHiveTableScanNode
import com.nvidia.spark.rapids.tool.profiling.{DataSourceCase, DriverAccumCase, JobInfoClass, ProfileUtils, SQLExecutionInfoClass, StageInfoClass, TaskStageAccumCase}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.deploy.history.{EventLogFileReader, EventLogFileWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerJobStart, SparkListenerLogStart, StageInfo}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.qualification.MLFunctions
import org.apache.spark.sql.rapids.tool.util.{EventUtils, RapidsToolsConfUtil, ToolsPlanGraph}
import org.apache.spark.util.Utils
import org.apache.spark.util.Utils.REDACTION_REPLACEMENT_TEXT

// Handles updating and caching Spark Properties for a Spark application.
// Properties stored in this container can be accessed to make decision about certain analysis
// that depends on the context of the Spark properties.
trait CacheableProps {
  private val RETAINED_SYSTEM_PROPS = Set(
    "file.encoding", "java.version", "os.arch", "os.name",
    "os.version", "user.timezone")

  // Patterns to be used to redact sensitive values from Spark Properties.
  private val REDACTED_PROPERTIES = Set[String](
    // S3
    "spark.hadoop.fs.s3a.secret.key",
    "spark.hadoop.fs.s3a.access.key",
    "spark.hadoop.fs.s3a.session.token",
    "spark.hadoop.fs.s3a.encryption.key",
    "spark.hadoop.fs.s3a.bucket.nightly.access.key",
    "spark.hadoop.fs.s3a.bucket.nightly.secret.key",
    "spark.hadoop.fs.s3a.bucket.nightly.session.token",
    // ABFS
    "spark.hadoop.fs.azure.account.oauth2.client.secret",
    "spark.hadoop.fs.azure.account.oauth2.client.id",
    "spark.hadoop.fs.azure.account.oauth2.refresh.token",
    "spark.hadoop.fs.azure.account.key\\..*",
    "spark.hadoop.fs.azure.account.auth.type\\..*",
    // GCS
    "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
    "spark.hadoop.fs.gs.auth.client.id",
    "spark.hadoop.fs.gs.encryption.key",
    "spark.hadoop.fs.gs.auth.client.secret",
    "spark.hadoop.fs.gs.auth.refresh.token",
    "spark.hadoop.fs.gs.auth.impersonation.service.account.for.user\\..*",
    "spark.hadoop.fs.gs.auth.impersonation.service.account.for.group\\..*",
    "spark.hadoop.fs.gs.auth.impersonation.service.account",
    "spark.hadoop.fs.gs.proxy.username",
    // matches on any key that contains password in it.
    "(?i).*password.*"
  )

  // caches the spark-version from the eventlogs
  var sparkVersion: String = ""
  var gpuMode = false
  // A flag whether hive is enabled or not. Note that we assume that the
  // property is global to the entire application once it is set. a.k.a, it cannot be disabled
  // once it is was set to true.
  var hiveEnabled = false
  var sparkProperties = Map[String, String]()
  var classpathEntries = Map[String, String]()
  // set the fileEncoding to UTF-8 by default
  var systemProperties = Map[String, String]()

  private def processPropKeys(srcMap: Map[String, String]): Map[String, String] = {
    // Redact the sensitive values in the given map.
    val redactedKeys = REDACTED_PROPERTIES.collect {
      case rK if srcMap.keySet.exists(_.matches(rK)) => rK -> REDACTION_REPLACEMENT_TEXT
    }
    srcMap ++ redactedKeys
  }

  def handleEnvUpdateForCachedProps(event: SparkListenerEnvironmentUpdate): Unit = {
    sparkProperties ++= processPropKeys(event.environmentDetails("Spark Properties").toMap)
    classpathEntries ++= event.environmentDetails("Classpath Entries").toMap

    gpuMode ||=  ProfileUtils.isPluginEnabled(sparkProperties)
    hiveEnabled ||= HiveParseHelper.isHiveEnabled(sparkProperties)

    // Update the properties if system environments are set.
    // No need to capture all the properties in memory. We only capture important ones.
    systemProperties ++= event.environmentDetails("System Properties").toMap.filterKeys(
      RETAINED_SYSTEM_PROPS.contains(_))
  }

  def handleJobStartForCachedProps(event: SparkListenerJobStart): Unit = {
    // TODO: we need to improve this in order to support per-job-level
    hiveEnabled ||= HiveParseHelper.isHiveEnabled(event.properties.asScala)
  }

  def handleLogStartForCachedProps(event: SparkListenerLogStart): Unit = {
    sparkVersion = event.sparkVersion
  }

  def isGPUModeEnabledForJob(event: SparkListenerJobStart): Boolean = {
    gpuMode || ProfileUtils.isPluginEnabled(event.properties.asScala)
  }
}

abstract class AppBase(
    val eventLogInfo: Option[EventLogInfo],
    val hadoopConf: Option[Configuration]) extends Logging with CacheableProps {

  // Store map of executorId to executor info
  val executorIdToInfo = new HashMap[String, ExecutorInfoClass]()

  var appEndTime: Option[Long] = None
  // The data source information
  val dataSourceInfo: ArrayBuffer[DataSourceCase] = ArrayBuffer[DataSourceCase]()

  // jobId to job info
  val jobIdToInfo = new HashMap[Int, JobInfoClass]()
  val jobIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]

  // SQL containing any Dataset operation or RDD to DataSet/DataFrame operation
  val sqlIDToDataSetOrRDDCase: HashSet[Long] = HashSet[Long]()
  val sqlIDtoProblematic: HashMap[Long, Set[String]] = HashMap[Long, Set[String]]()
  // sqlId to sql info
  val sqlIdToInfo = new HashMap[Long, SQLExecutionInfoClass]()
  // sqlPlans stores HashMap (sqlID <-> SparkPlanInfo)
  var sqlPlans: HashMap[Long, SparkPlanInfo] = HashMap.empty[Long, SparkPlanInfo]

  // accum id to task stage accum info
  var taskStageAccumMap: HashMap[Long, ArrayBuffer[TaskStageAccumCase]] =
    HashMap[Long, ArrayBuffer[TaskStageAccumCase]]()

  val stageIdToInfo: HashMap[(Int, Int), StageInfoClass] = new HashMap[(Int, Int), StageInfoClass]()
  val accumulatorToStages: HashMap[Long, Set[Int]] = new HashMap[Long, Set[Int]]()

  var driverAccumMap: HashMap[Long, ArrayBuffer[DriverAccumCase]] =
    HashMap[Long, ArrayBuffer[DriverAccumCase]]()
  var mlEventLogType = ""
  var pysparkLogFlag = false

  def getOrCreateExecutor(executorId: String, addTime: Long): ExecutorInfoClass = {
    executorIdToInfo.getOrElseUpdate(executorId, {
      new ExecutorInfoClass(executorId, addTime)
    })
  }

  /**
   * Retrieves cluster information based on executor nodes.
   * If executor nodes exist, calculates the number of hosts and total cores,
   * and extracts executor and driver instance types (databricks only)
   *
   * @return
   */
  def getClusterInfo: Option[ClusterInfo] = {
    // Extracts instance types from properties (databricks only)
    val executorInstance = sparkProperties.get("spark.databricks.workerNodeTypeId")
    val driverInstance = sparkProperties.get("spark.databricks.driverNodeTypeId")

    val executorOnlyInfo = executorIdToInfo.filterKeys(_ != "driver")
    if (executorOnlyInfo.nonEmpty) {
      // #executor nodes = #unique hosts
      val numExecutorNodes = executorOnlyInfo.values.map(_.host).toSet.size
      val numCores = executorOnlyInfo.head._2.totalCores
      Some(ClusterInfo(numCores, numExecutorNodes, executorInstance, driverInstance))
    } else {
      None
    }
  }

  def getOrCreateStage(info: StageInfo): StageInfoClass = {
    val stage = stageIdToInfo.getOrElseUpdate((info.stageId, info.attemptNumber()),
      new StageInfoClass(info))
    stage
  }

  def checkMLOps(appId: Int, stageInfo: StageInfoClass): Option[MLFunctions] = {
    val stageInfoDetails = stageInfo.info.details
    val mlOps = if (stageInfoDetails.contains(MlOps.sparkml) ||
      stageInfoDetails.contains(MlOps.xgBoost)) {

      // Check if it's a pyspark eventlog and set the mleventlogtype to pyspark
      // Once it is set, do not change it to scala if other stageInfoDetails don't match.
      if (stageInfoDetails.contains(MlOps.pysparkLog)) {
        if (!pysparkLogFlag) {
          mlEventLogType = MlOpsEventLogType.pyspark
          pysparkLogFlag = true
        }
      } else {
        if (!pysparkLogFlag) {
          mlEventLogType = MlOpsEventLogType.scala
        }
      }

      // Consider stageInfo to have below string as an example
      //org.apache.spark.rdd.RDD.first(RDD.scala:1463)
      //org.apache.spark.mllib.feature.PCA.fit(PCA.scala:44)
      //org.apache.spark.ml.feature.PCA.fit(PCA.scala:93)
      val splitString = stageInfoDetails.split("\n")

      // filteredString = org.apache.spark.ml.feature.PCA.fit
      val filteredString = splitString.filter(
        string => string.contains(MlOps.sparkml) || string.contains(MlOps.xgBoost)).map(
        packageName => packageName.split("\\(").head
      )
      filteredString
    } else {
      Array.empty[String]
    }

    if (mlOps.nonEmpty) {
      Some(MLFunctions(Some(appId.toString), stageInfo.info.stageId, mlOps,
        stageInfo.duration.getOrElse(0)))
    } else {
      None
    }
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
    taskStageAccumMap.remove(accId)
    driverAccumMap.remove(accId)
    accumulatorToStages.remove(accId)
  }

  def cleanupStages(stageIds: Set[Int]): Unit = {
    // stageIdToInfo can have multiple stage attempts, remove all of them
    stageIds.foreach { stageId =>
      val toRemove = stageIdToInfo.keys.filter(_._1 == stageId)
      toRemove.foreach(stageIdToInfo.remove(_))
    }
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
      case c if (c.isDefined && c.get.equals("gz")) =>
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
   * Functions to process all the events
   */
  protected def processEvents(): Unit = {
    eventLogInfo match {
      case Some(eventLog) =>
        val eventLogPath = eventLog.eventLog
        logInfo("Parsing Event Log: " + eventLogPath.toString)

        // at this point all paths should be valid event logs or event log dirs
        val hconf = hadoopConf.getOrElse(RapidsToolsConfUtil.newHadoopConf)
        val fs = eventLogPath.getFileSystem(hconf)
        var totalNumEvents = 0
        val readerOpt = eventLog match {
          case _: DatabricksEventLog =>
            Some(new DatabricksRollingEventLogFilesFileReader(fs, eventLogPath))
          case _ => EventLogFileReader(fs, eventLogPath)
        }

        if (readerOpt.isDefined) {
          val reader = readerOpt.get
          val logFiles = reader.listEventLogFiles
          logFiles.foreach { file =>
            Utils.tryWithResource(openEventLogInternal(file.getPath, fs)) { in =>
              val lines = Source.fromInputStream(in)(Codec.UTF8).getLines().toIterator
              // Using find as foreach with conditional to exit early if we are done.
              // Do NOT use a while loop as it is much much slower.
              lines.find { line =>
                totalNumEvents += 1
                EventUtils.getEventFromJsonMethod(line) match {
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

  def containsUDF(desc: String): Boolean = {
    desc.matches(UDFRegex)
  }

  protected def findPotentialIssues(desc: String): Set[String] =  {
    val potentialIssuesRegexs = potentialIssuesRegexMap
    val issues = potentialIssuesRegexs.filterKeys(desc.matches(_))
    issues.values.toSet
  }

  def getPlanMetaWithSchema(planInfo: SparkPlanInfo): Seq[SparkPlanInfo] = {
    val childRes = planInfo.children.flatMap(getPlanMetaWithSchema(_))
    if (planInfo.metadata != null && planInfo.metadata.contains("ReadSchema")) {
      childRes :+ planInfo
    } else {
      childRes
    }
  }

  // Finds all the nodes that scan a hive table
  def getPlanInfoWithHiveScan(planInfo: SparkPlanInfo): Seq[SparkPlanInfo] = {
    val childRes = planInfo.children.flatMap(getPlanInfoWithHiveScan(_))
    if (isHiveTableScanNode(planInfo.nodeName)) {
      childRes :+ planInfo
    } else {
      childRes
    }
  }

  private def trimSchema(str: String): String = {
    val index = str.lastIndexOf(",")
    if (index != -1 && str.contains("...")) {
      str.substring(0, index)
    } else {
      str
    }
  }

  // The ReadSchema metadata is only in the eventlog for DataSource V1 readers
  protected def checkMetadataForReadSchema(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    // check if planInfo has ReadSchema
    val allMetaWithSchema = getPlanMetaWithSchema(planInfo)
    val planGraph = ToolsPlanGraph(planInfo)
    val allNodes = planGraph.allNodes

    allMetaWithSchema.foreach { plan =>
      val meta = plan.metadata
      val readSchema = ReadParser.formatSchemaStr(meta.getOrElse("ReadSchema", ""))
      val scanNode = allNodes.filter(node => {
        // Get ReadSchema of each Node and sanitize it for comparison
        val trimmedNode = trimSchema(ReadParser.parseReadNode(node).schema)
        readSchema.contains(trimmedNode)
      }).filter(ReadParser.isScanNode(_)).head

      dataSourceInfo += DataSourceCase(sqlID,
        scanNode.id,
        meta.getOrElse("Format", "unknown"),
        meta.getOrElse("Location", "unknown"),
        meta.getOrElse("PushedFilters", "unknown"),
        readSchema
      )
    }
    // "scan hive" has no "ReadSchema" defined. So, we need to look explicitly for nodes
    // that are scan hive and add them one by one to the dataSource
    if (hiveEnabled) { // only scan for hive when the CatalogImplementation is using hive
      val allPlanWithHiveScan = getPlanInfoWithHiveScan(planInfo)
      allPlanWithHiveScan.foreach { hiveReadPlan =>
        val sqlGraph = ToolsPlanGraph(hiveReadPlan)
        val hiveScanNode = sqlGraph.allNodes.head
        val scanHiveMeta = HiveParseHelper.parseReadNode(hiveScanNode)
        dataSourceInfo += DataSourceCase(sqlID,
          hiveScanNode.id,
          scanHiveMeta.format,
          scanHiveMeta.location,
          scanHiveMeta.filters,
          scanHiveMeta.schema
        )
      }
    }
  }

  // This will find scans for DataSource V2, if the schema is very large it
  // will likely be incomplete and have ... at the end.
  protected def checkGraphNodeForReads(sqlID: Long, node: SparkPlanGraphNode): Unit = {
    if (ReadParser.isDataSourceV2Node(node)) {
      val res = ReadParser.parseReadNode(node)

      dataSourceInfo += DataSourceCase(sqlID,
        node.id,
        res.format,
        res.location,
        res.filters,
        res.schema
      )
    }
  }

  protected def reportComplexTypes: (Seq[String], Seq[String]) = {
    if (dataSourceInfo.size != 0) {
      val schema = dataSourceInfo.map { ds => ds.schema }
      AppBase.parseReadSchemaForNestedTypes(schema)
    } else {
      (Seq(), Seq())
    }
  }

  protected def probNotDataset: HashMap[Long, Set[String]] = {
    sqlIDtoProblematic.filterNot { case (sqlID, _) => sqlIDToDataSetOrRDDCase.contains(sqlID) }
  }

  protected def getPotentialProblemsForDf: Seq[String] = {
    probNotDataset.values.flatten.toSet.toSeq
  }

  // This is to append potential issues such as UDF, decimal type determined from
  // SparkGraphPlan Node description and nested complex type determined from reading the
  // event logs. If there are any complex nested types, then `NESTED COMPLEX TYPE` is mentioned
  // in the `Potential Problems` section in the csv file. Section `Unsupported Nested Complex
  // Types` has information on the exact nested complex types which are not supported for a
  // particular application.
  protected def getAllPotentialProblems(
      dFPotentialProb: Seq[String], nestedComplex: Seq[String]): Seq[String] = {
    val nestedComplexType = if (nestedComplex.nonEmpty) Seq("NESTED COMPLEX TYPE") else Seq("")
    val result = if (dFPotentialProb.nonEmpty) {
      if (nestedComplex.nonEmpty) {
        dFPotentialProb ++ nestedComplexType
      } else {
        dFPotentialProb
      }
    } else {
      nestedComplexType
    }
    result
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
        tempStringBuilder.append(char);
      }
    }
    if (!tempStringBuilder.isEmpty) {
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
}
