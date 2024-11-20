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

package com.nvidia.spark.rapids.tool.qualification

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.collection.mutable.{Buffer, LinkedHashMap, ListBuffer}

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import com.nvidia.spark.rapids.tool.analysis.ExecInfoAnalyzer
import com.nvidia.spark.rapids.tool.planparser.{DatabricksParseHelper, ExecInfo, PlanInfo, UnsupportedExecSummary}
import com.nvidia.spark.rapids.tool.profiling.AppStatusResult
import com.nvidia.spark.rapids.tool.profiling.ProfileUtils.replaceDelimiter
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter.{CLUSTER_ID, CLUSTER_ID_STR_SIZE, JOB_ID, JOB_ID_STR_SIZE, RUN_NAME, RUN_NAME_STR_SIZE, TEXT_DELIMITER}
import org.apache.hadoop.conf.Configuration
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.qualification.{EstimatedPerSQLSummaryInfo, EstimatedSummaryInfo, QualificationSummaryInfo}
import org.apache.spark.sql.rapids.tool.util._

/**
 * This class handles the output files for qualification.
 * It can write both a raw csv file and then a text summary report.
 *
 * @param outputDir The directory to output the files to
 * @param reportReadSchema Whether to include the read data source schema in csv output
 * @param printStdout Indicates if the summary report should be printed to stdout as well
 * @param prettyPrintOrder The order in which to print the Text output
 * @param hadoopConf Optional Hadoop Configuration to use
 */
class QualOutputWriter(outputDir: String, reportReadSchema: Boolean,
    printStdout: Boolean, prettyPrintOrder: String,
    hadoopConf: Option[Configuration] = None) {

  implicit val formats: DefaultFormats.type = DefaultFormats

  def writeDetailedCSVReport(sums: Seq[QualificationSummaryInfo]): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}.csv", "CSV", hadoopConf)
    try {
      writeDetailedCSVReport(csvFileWriter, sums)
    } finally {
      csvFileWriter.close()
    }
  }

  protected def writeDetailedCSVReport(csvFileWriter: ToolTextFileWriter,
      sums: Seq[QualificationSummaryInfo]): Unit = {
    val headersAndSizes = QualOutputWriter.getDetailedHeaderStringsAndSizes(sums,
      reportReadSchema)
    csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes,
      QualOutputWriter.CSV_DELIMITER, prettyPrint = false))
    sums.foreach { sum =>
      csvFileWriter.write(QualOutputWriter.constructAppDetailedInfo(sum, headersAndSizes,
        QualOutputWriter.CSV_DELIMITER, prettyPrint = false, reportReadSchema = reportReadSchema))
    }
  }

  // write the text summary report
  def writeTextReport(sums: Seq[QualificationSummaryInfo], estSums: Seq[EstimatedSummaryInfo],
      numOutputRows: Int): Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}.log", "Summary Report", hadoopConf)
    try {
      writeTextReport(textFileWriter, sums, estSums, numOutputRows)
    } finally {
      textFileWriter.close()
    }
  }

  protected def writeTextReport(writer: ToolTextFileWriter,
      sums: Seq[QualificationSummaryInfo], estSum: Seq[EstimatedSummaryInfo],
      numOutputRows: Int): Unit = {
    val appIdMaxSize = QualOutputWriter.getAppIdSize(sums)
    val unSupExecMaxSize = QualOutputWriter.getunSupportedMaxSize(
      sums.map(_.unSupportedExecs.size),
      QualOutputWriter.UNSUPPORTED_EXECS_MAX_SIZE,
      QualOutputWriter.UNSUPPORTED_EXECS.size)
    val unSupExprMaxSize = QualOutputWriter.getunSupportedMaxSize(
      sums.map(_.unSupportedExprs.size),
      QualOutputWriter.UNSUPPORTED_EXPRS_MAX_SIZE,
      QualOutputWriter.UNSUPPORTED_EXPRS.size)
    val estimatedFrequencyMaxSize = QualOutputWriter.ESTIMATED_FREQUENCY_MAX_SIZE
    val appNameMaxSize = QualOutputWriter.getAppNameSize(sums)
    val hasClusterTags = sums.exists(_.clusterTags.nonEmpty)
    val (clusterIdMaxSize, jobIdMaxSize, runNameMaxSize) = if (hasClusterTags) {
      (QualOutputWriter.getMaxSizeForHeader(sums.map(_.allClusterTagsMap.getOrElse(
        CLUSTER_ID, "").size), QualOutputWriter.CLUSTER_ID),
        QualOutputWriter.getMaxSizeForHeader(sums.map(_.allClusterTagsMap.getOrElse(
          JOB_ID, "").size), QualOutputWriter.JOB_ID),
        QualOutputWriter.getMaxSizeForHeader(sums.map(_.allClusterTagsMap.getOrElse(
          RUN_NAME, "").size), QualOutputWriter.RUN_NAME))
    } else {
      (CLUSTER_ID_STR_SIZE, JOB_ID_STR_SIZE, RUN_NAME_STR_SIZE)
    }
    val headersAndSizes = QualOutputWriter.getSummaryHeaderStringsAndSizes(
      appNameMaxSize, appIdMaxSize, unSupExecMaxSize, unSupExprMaxSize, estimatedFrequencyMaxSize,
      hasClusterTags, clusterIdMaxSize, jobIdMaxSize, runNameMaxSize)
    val entireHeader = QualOutputWriter.constructOutputRowFromMap(headersAndSizes,
      TEXT_DELIMITER, prettyPrint = true)
    val sep = "=" * (entireHeader.size - 1)
    writer.write(s"$sep\n")
    writer.write(entireHeader)
    writer.write(s"$sep\n")
    // write to stdout as well
    if (printStdout) {
      print("APPLICATION SUMMARY:\n")
      print(s"$sep\n")
      print(entireHeader)
      print(s"$sep\n")
    }
    val finalSums = estSum.take(numOutputRows)
    finalSums.foreach { sumInfo =>
      val wStr = QualOutputWriter.constructAppSummaryInfo(sumInfo, headersAndSizes,
        appIdMaxSize, unSupExecMaxSize, unSupExprMaxSize, estimatedFrequencyMaxSize, hasClusterTags,
        clusterIdMaxSize, jobIdMaxSize, runNameMaxSize, TEXT_DELIMITER, true)
      writer.write(wStr)
      if (printStdout) print(wStr)
    }
    writer.write(s"$sep\n")
    if (printStdout) print(s"$sep\n")
  }

  def writeStageReport(sums: Seq[QualificationSummaryInfo], order: String) : Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_stages.csv",
      "Stage Exec Info", hadoopConf)
    try {
      val headersAndSizes = QualOutputWriter.getDetailedStagesHeaderStrings
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", false))
      sums.foreach { sumInfo =>
        val rows = QualOutputWriter.constructStagesInfo(sumInfo, headersAndSizes, ",", false)
        rows.foreach(csvFileWriter.write(_))
      }
    } finally {
      csvFileWriter.close()
    }
  }

  def writeUnsupportedOpsSummaryCSVReport(
      sums: Seq[QualificationSummaryInfo]): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_unsupportedOperators.csv",
      "Unsupported Operators DetailedStageDuration CSV Report", hadoopConf)
    try {
      val headersAndSizes = QualOutputWriter.getUnsupportedOperatorsHeaderStrings
      csvFileWriter.write(QualOutputWriter.constructOutputRowFromMap(headersAndSizes,
        QualOutputWriter.CSV_DELIMITER))
      sums.foreach { sum =>
        QualOutputWriter.constructUnsupportedDetailedStagesDurationInfo(csvFileWriter,
          sum, headersAndSizes,
          QualOutputWriter.CSV_DELIMITER, false)
      }
    } finally {
      csvFileWriter.close()
    }
  }

  def writeAllOpsSummaryCSVreport(
      sums: Seq[QualificationSummaryInfo]): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_allOperators.csv",
      "All Operators CSV Report", hadoopConf)
    try {
      val headersAndSizes = QualOutputWriter.getAllOperatorsHeaderStrings
      csvFileWriter.write(QualOutputWriter.constructOutputRowFromMap(headersAndSizes,
        QualOutputWriter.CSV_DELIMITER))
      sums.foreach { sum =>
        QualOutputWriter.constructAllOperatorsInfo(csvFileWriter, sum.planInfo, sum.appId,
          headersAndSizes, QualOutputWriter.CSV_DELIMITER, false)
      }
    } finally {
      csvFileWriter.close()
    }
  }

  def writePerSqlCSVReport(sums: Seq[QualificationSummaryInfo], maxSQLDescLength: Int): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_persql.csv",
      "Per SQL CSV Report", hadoopConf)
    try {
      val appNameSize = QualOutputWriter.getAppNameSize(sums)
      val appIdSize = QualOutputWriter.getAppIdSize(sums)
      val sqlDescSize =
        QualOutputWriter.getSqlDescSize(sums, maxSQLDescLength, QualOutputWriter.CSV_DELIMITER)
      val headersAndSizes =
        QualOutputWriter.getDetailedPerSqlHeaderStringsAndSizes(appNameSize, appIdSize, sqlDescSize)
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes,
        QualOutputWriter.CSV_DELIMITER, false))
      val appIdMaxSize = QualOutputWriter.getAppIdSize(sums)
      val sortedInfo = sortPerSqlInfo(sums)
      sortedInfo.foreach { sumInfo =>
        val row = QualOutputWriter.constructPerSqlSummaryInfo(sumInfo, headersAndSizes,
          appIdMaxSize, ",", false, maxSQLDescLength)
        csvFileWriter.write(row)
      }
    } finally {
      csvFileWriter.close()
    }
  }

  private def sortPerSqlInfo(
      sums: Seq[QualificationSummaryInfo]): Seq[EstimatedPerSQLSummaryInfo] = {
    val estSumPerSql = sums.flatMap(_.perSQLEstimatedInfo).flatten
    val sortedAsc = estSumPerSql.sortBy(sum => {
      (sum.info.gpuOpportunity, sum.info.appDur, sum.info.appId)
    })
    if (QualificationArgs.isOrderAsc(prettyPrintOrder)) {
      sortedAsc
    } else {
      sortedAsc.reverse
    }
  }
  private def writePerSqlTextSummary(writer: ToolTextFileWriter,
      sums: Seq[QualificationSummaryInfo],
      numOutputRows: Int, maxSQLDescLength: Int): Unit = {
    val appNameSize = QualOutputWriter.getAppNameSize(sums)
    val appIdSize = QualOutputWriter.getAppIdSize(sums)
    val sqlDescSize =
      QualOutputWriter.getSqlDescSize(sums, maxSQLDescLength, QualOutputWriter.TEXT_DELIMITER)
    val headersAndSizes =
      QualOutputWriter.getDetailedPerSqlHeaderStringsAndSizes(appNameSize, appIdSize, sqlDescSize)
    val entireHeader = QualOutputWriter.constructOutputRowFromMap(headersAndSizes,
      TEXT_DELIMITER, true)
    val sep = "=" * (entireHeader.size - 1)
    writer.write(s"$sep\n")
    writer.write(entireHeader)
    writer.write(s"$sep\n")
    // write to stdout as well
    if (printStdout) {
      print("PER SQL SUMMARY:\n")
      print(s"$sep\n")
      print(entireHeader)
      print(s"$sep\n")
    }
    val sortedInfo = sortPerSqlInfo(sums)
    val finalSums = sortedInfo.take(numOutputRows)
    finalSums.foreach { estInfo =>
      val wStr = QualOutputWriter.constructPerSqlSummaryInfo(estInfo, headersAndSizes,
        appIdSize, TEXT_DELIMITER, true, maxSQLDescLength, false)
      writer.write(wStr)
      if (printStdout) print(wStr)
    }
    writer.write(s"$sep\n")
    if (printStdout) print(s"$sep\n")
  }

  def writePerSqlTextReport(sums: Seq[QualificationSummaryInfo], numOutputRows: Int,
      maxSQLDescLength: Int): Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_persql.log",
      "Per SQL Summary Report", hadoopConf)
    try {
      writePerSqlTextSummary(textFileWriter, sums, numOutputRows, maxSQLDescLength)
    } finally {
      textFileWriter.close()
    }
  }

  def writeExecReport(sums: Seq[QualificationSummaryInfo], order: String) : Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_execs.csv",
      "Plan Exec Info", hadoopConf)
    try {
      val headersAndSizes = QualOutputWriter.getDetailedExecsHeaderStrings
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(
        QualOutputWriter.getDetailedExecsHeaderStrings, ",", false))
      sums.foreach { sumInfo =>
        val appRows = QualOutputWriter.constructExecsInfo(sumInfo, headersAndSizes, ",", false)
        appRows.foreach(csvFileWriter.write(_))
      }
    } finally {
      csvFileWriter.close()
    }
  }

  def writeClusterReport(sums: Seq[QualificationSummaryInfo]): Unit = {
    val jsonFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_cluster_information.json",
      "Cluster Information", hadoopConf)
    try {
      // Append new line at end of JSON string
      jsonFileWriter.write(Serialization.writePretty(sums.map(_.clusterSummary)) + "\n")
    } finally {
      jsonFileWriter.close()
    }
  }

  def writeClusterReportCsv(sums: Seq[QualificationSummaryInfo]): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_cluster_information.csv",
      "Cluster Information", hadoopConf)
    try {
      val headersAndSizes = QualOutputWriter.getClusterInfoHeaderStrings
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(
        headersAndSizes, ",", prettyPrint = false))
      sums.foreach { sumInfo =>
        val appRows = QualOutputWriter.constructClusterInfo(sumInfo, headersAndSizes, ",",
          prettyPrint = false)
        appRows.foreach(csvFileWriter.write)
      }
    } finally {
      csvFileWriter.close()
    }
  }

  def writeMlFuncsReports(sums: Seq[QualificationSummaryInfo], order: String): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_mlfunctions.csv",
      "", hadoopConf)
    try {
      val headersAndSizes = QualOutputWriter.getDetailedMlFuncsHeaderStringsAndSizes(sums)
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", false))
      sums.foreach { sumInfo =>
        val rows = QualOutputWriter.constructMlFuncsInfo(sumInfo, headersAndSizes, ",", false)
        rows.foreach(csvFileWriter.write(_))
      }
    } finally {
      csvFileWriter.close()
    }
  }

  def writeMlFuncsTotalDurationReports(sums: Seq[QualificationSummaryInfo]): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_mlfunctions_totalduration.csv",
      "", hadoopConf)
    try {
      // Filter only supported ML functions
      val supportedMlFuns = sums.filter(x => x.mlFunctionsStageDurations.nonEmpty)
      val headersAndSizes =
        QualOutputWriter.getDetailedMlFuncsTotalDurationHeaderStringsAndSizes(supportedMlFuns)
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", false))
      supportedMlFuns.foreach { sumInfo =>
        val rows = QualOutputWriter.constructMlFuncsTotalDurationInfo(
          sumInfo, headersAndSizes, ",", false, true)
        rows.foreach(csvFileWriter.write(_))
      }
    } finally {
      csvFileWriter.close()
    }
  }

  def writeStatusReport(statusReports: Seq[AppStatusResult], order: String): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_status.csv",
      "Status Report Info", hadoopConf)
    try {
      val headersAndSizes = QualOutputWriter
        .getDetailedStatusHeaderStringsAndSizes(statusReports)
      csvFileWriter.write(
        QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", prettyPrint = false))
      statusReports.foreach { statusReport =>
        val rows = QualOutputWriter.constructStatusReportInfo(
          statusReport, headersAndSizes, ",", prettyPrint = false)
        rows.foreach(csvFileWriter.write)
      }
    } finally {
      csvFileWriter.close()
    }
  }
}

case class FormattedQualificationSummaryInfo(
    appName: String,
    appId: String,
    sqlDataframeDuration: Long,
    sqlDataframeTaskDuration: Long,
    appDuration: Long,
    gpuOpportunity: Long,
    executorCpuTimePercent: Double,
    failedSQLIds: String,
    readFileFormatAndTypesNotSupported: String,
    readFileFormats: String,
    writeDataFormat: String,
    complexTypes: String,
    nestedComplexTypes: String,
    potentialProblems: String,
    longestSqlDuration: Long,
    sqlStageDurationsSum: Long,
    nonSqlTaskDurationAndOverhead: Long,
    unsupportedSQLTaskDuration: Long,
    supportedSQLTaskDuration: Long,
    endDurationEstimated: Boolean,
    unSupportedExecs: String,
    unSupportedExprs: String,
    clusterTags: Map[String, String],
    estimatedFrequency: Long,
    totalCoreSec: Long)

object QualOutputWriter {
  val NON_SQL_TASK_DURATION_STR = "NonSQL Task Duration"
  val SQL_ID_STR = "SQL ID"
  val ROOT_SQL_ID_STR = "Root SQL ID"
  val SQL_DESC_STR = "SQL Description"
  val STAGE_ID_STR = "Stage ID"
  val APP_ID_STR = "App ID"
  val APP_NAME_STR = "App Name"
  val APP_DUR_STR = "App Duration"
  val SQL_DUR_STR = "SQL DF Duration"
  val TASK_DUR_STR = "SQL Dataframe Task Duration"
  val STAGE_DUR_STR = "Stage Task Duration"
  val STAGE_WALLCLOCK_DUR_STR = "Stage Duration"
  val SQL_STAGE_DUR_SUM_STR = "SQL Stage Durations Sum"
  val POT_PROBLEM_STR = "Potential Problems"
  val EXEC_CPU_PERCENT_STR = "Executor CPU Time Percent"
  val APP_DUR_ESTIMATED_STR = "App Duration Estimated"
  val SQL_IDS_FAILURES_STR = "SQL Ids with Failures"
  val READ_FILE_FORMAT_TYPES_STR = "Unsupported Read File Formats and Types"
  val WRITE_DATA_FORMAT_STR = "Unsupported Write Data Format"
  val COMPLEX_TYPES_STR = "Complex Types"
  val NESTED_TYPES_STR = "Nested Complex Types"
  val READ_SCHEMA_STR = "Read Schema"
  val NONSQL_DUR_STR = "NONSQL Task Duration Plus Overhead"
  val UNSUPPORTED_TASK_DURATION_STR = "Unsupported Task Duration"
  val SUPPORTED_SQL_TASK_DURATION_STR = "Supported SQL DF Task Duration"
  val LONGEST_SQL_DURATION_STR = "Longest SQL Duration"
  val EXEC_STR = "Exec Name"
  val EXPR_STR = "Expression Name"
  val EXEC_DURATION = "Exec Duration"
  val EXEC_NODEID = "SQL Node Id"
  val EXEC_IS_SUPPORTED = "Exec Is Supported"
  val EXEC_STAGES = "Exec Stages"
  val EXEC_SHOULD_REMOVE = "Exec Should Remove"
  val EXEC_SHOULD_IGNORE = "Exec Should Ignore"
  val EXEC_CHILDREN = "Exec Children"
  val EXEC_CHILDREN_NODE_IDS = "Exec Children Node Ids"
  val GPU_OPPORTUNITY_STR = "GPU Opportunity"
  val STAGE_ESTIMATED_STR = "Stage Estimated"
  val NUM_TRANSITIONS = "Number of transitions from or to GPU"
  val UNSUPPORTED_EXECS = "Unsupported Execs"
  val UNSUPPORTED_EXPRS = "Unsupported Expressions"
  val UNSUPPORTED_OPERATOR = "Unsupported Operator"
  val COUNT = "Count"
  val IS_SUPPORTED = "Supported"
  val OPERATOR_TYPE = "Operator Type"
  val OPERATOR_NAME = "Operator Name"
  val STAGES = "Stages"
  val CLUSTER_TAGS = "Cluster Tags"
  val CLUSTER_ID = DatabricksParseHelper.SUB_PROP_CLUSTER_ID
  val JOB_ID = DatabricksParseHelper.SUB_PROP_JOB_ID
  val UNSUPPORTED_TYPE = "Unsupported Type"
  val EXEC_ID = "ExecId"
  val DETAILS = "Details"
  private val EXEC_ACTION = "Action"
  val RUN_NAME = DatabricksParseHelper.SUB_PROP_RUN_NAME
  val ESTIMATED_FREQUENCY = "Estimated Job Frequency (monthly)"
  val ML_FUNCTIONS = "ML Functions"
  val ML_FUNCTION_NAME = "ML Function Name"
  val ML_TOTAL_STAGE_DURATION = "Total Duration"
  val ML_STAGE_IDS = "Stage Ids"
  val STATUS_REPORT_PATH_STR = "Event Log"
  val STATUS_REPORT_STATUS_STR = "Status"
  val STATUS_REPORT_APP_ID = "AppID"
  val STATUS_REPORT_DESC_STR = "Description"
  val VENDOR = "Vendor"
  val RECOMMENDED_VENDOR = "Recommended Vendor"
  val DRIVER_HOST = "Driver Host"
  val CLUSTER_ID_STR = "Cluster Id" // Different from ClusterId used for Databricks Tags
  val CLUSTER_NAME = "Cluster Name"
  val RECOMMENDED_NUM_GPUS = "Recommended Num GPUs Per Node"
  val RECOMMENDED_GPU_DEVICE = "Recommended GPU Device"
  val NUM_EXECS_PER_NODE = "Num Executors Per Node"
  val NUM_EXECS = "Num Executors"
  val EXECUTOR_HEAP_MEMORY = "Executor Heap Memory"
  val DYN_ALLOC_ENABLED = "Dynamic Allocation Enabled"
  val DYN_ALLOC_MAX = "Dynamic Allocation Max Executors"
  val DYN_ALLOC_MIN = "Dynamic Allocation Min Executors"
  val DYN_ALLOC_INIT = "Dynamic Allocation Initial Executors"
  val RECOMMENDED_DYN_ALLOC_ENABLED = "Recommended Dynamic Allocation Enabled"
  val RECOMMENDED_DYN_ALLOC_MAX = "Recommended Dynamic Allocation Max Executors"
  val RECOMMENDED_DYN_ALLOC_MIN = "Recommended Dynamic Allocation Min Executors"
  val RECOMMENDED_DYN_ALLOC_INIT = "Recommended Dynamic Allocation Initial Executors"
  val RECOMMENDED_NUM_EXECS = "Recommended Num Executors"
  val NUM_WORKER_NODES = "Num Worker Nodes"
  val RECOMMENDED_NUM_WORKER_NODES = "Recommended Num Worker Nodes"
  val CORES_PER_EXEC = "Cores Per Executor"
  val RECOMMENDED_CORES_PER_EXEC = "Recommended Cores Per Executor"
  val WORKER_NODE_TYPE = "Worker Node Type"
  val RECOMMENDED_WORKER_NODE_TYPE = "Recommended Worker Node Type"
  val DRIVER_NODE_TYPE = "Driver Node Type"
  val TOTAL_CORE_SEC = "Total Core Seconds"
  // Default frequency for jobs with a single instance is 30 times every month (30 days)
  val DEFAULT_JOB_FREQUENCY = 30L
  val APP_DUR_STR_SIZE: Int = APP_DUR_STR.size
  val SQL_DUR_STR_SIZE: Int = SQL_DUR_STR.size
  val LONGEST_SQL_DURATION_STR_SIZE: Int = LONGEST_SQL_DURATION_STR.size
  val GPU_OPPORTUNITY_STR_SIZE: Int = GPU_OPPORTUNITY_STR.size
  val UNSUPPORTED_EXECS_MAX_SIZE: Int = 25
  val UNSUPPORTED_EXPRS_MAX_SIZE: Int = 25
  val ESTIMATED_FREQUENCY_MAX_SIZE: Int = 33
  val CLUSTER_ID_STR_SIZE: Int = CLUSTER_ID.size
  val JOB_ID_STR_SIZE: Int = JOB_ID.size
  val RUN_NAME_STR_SIZE: Int = RUN_NAME.size
  val CSV_DELIMITER = ","
  val TEXT_DELIMITER = "|"

  // a file extension will be added to this later
  val LOGFILE_NAME = "rapids_4_spark_qualification_output"

  private def getReformatCSVFunc(reformatCSV: Boolean): String => String = {
    if (reformatCSV) str => StringUtils.reformatCSVString(str) else str => stringIfEmpty(str)
  }

  def getAppIdSize(sums: Seq[QualificationSummaryInfo]): Int = {
    val sizes = sums.map(_.appId.size)
    getMaxSizeForHeader(sizes, QualOutputWriter.APP_ID_STR)
  }

  def getAppNameSize(sums: Seq[QualificationSummaryInfo]): Int = {
    getMaxSizeForHeader(sums.map(_.appName.size), APP_NAME_STR)
  }

  def getunSupportedMaxSize(unSupExecs: Seq[Int], maxStringSize: Int, headerSize: Int): Int = {
    val unSupportedExecsSize = unSupExecs.size
    val unSupportedExecsMax = if (unSupExecs.nonEmpty) {
      unSupExecs.max
    } else {
      0
    }
    // return maxString size if the unsupportedString exceeds maxStringSize
    if (unSupportedExecsSize > 0 && unSupportedExecsMax > maxStringSize) {
      maxStringSize
    } else if (unSupportedExecsSize > 0 && unSupportedExecsMax < maxStringSize
      && unSupportedExecsMax >= headerSize) {
      unSupportedExecsMax
    } else {
      headerSize
    }
  }

  def getSqlDescSize(sums: Seq[QualificationSummaryInfo], maxSQLDescLength: Int,
      delimiter: String): Int = {
    val sizes = sums.flatMap(_.perSQLEstimatedInfo).flatten.map { info =>
      formatSQLDescription(info.sqlDesc, maxSQLDescLength, delimiter).size
    }
    val maxSizeOfDesc = getMaxSizeForHeader(sizes, QualOutputWriter.SQL_DESC_STR)
    Math.min(maxSQLDescLength, maxSizeOfDesc)
  }

  def getMaxSizeForHeader(sizes: Seq[Int], headerTxtStr: String): Int = {
    if (sizes.nonEmpty && sizes.max > headerTxtStr.size) {
      sizes.max
    } else {
      headerTxtStr.size
    }
  }

  def getMaxSizeForHeader(sizes: Seq[Long], headerTxtStr: String): Long = {
    if (sizes.size > 0 && sizes.max > headerTxtStr.size) {
      sizes.max
    } else {
      headerTxtStr.size
    }
  }

  // ordered hashmap contains each header string and the size to use
  def constructOutputRowFromMap(
      strAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean = false): String = {
    constructOutputRow(strAndSizes.toBuffer, delimiter, prettyPrint)
  }

  private def constructOutputRow(
      strAndSizes: Buffer[(String, Int)],
      delimiter: String,
      prettyPrint: Boolean): String = {
    val entireHeader = new StringBuffer
    if (prettyPrint) {
      entireHeader.append(delimiter)
    }
    val lastEntry = strAndSizes.last
    strAndSizes.dropRight(1).foreach { case (str, strSize) =>
      if (prettyPrint) {
        val updatedString = stringLengthExceedsMax(str, strSize, delimiter)
        entireHeader.append(updatedString)
      } else {
        entireHeader.append(s"${str}${delimiter}")
      }
    }
    // for the last element we don't want to print the delimiter at the end unless
    // pretty printing
    if (prettyPrint) {
      val updatedString = stringLengthExceedsMax(lastEntry._1, lastEntry._2, delimiter)
      entireHeader.append(updatedString)
    } else {
      entireHeader.append(s"${lastEntry._1}")
    }
    entireHeader.append("\n")
    entireHeader.toString
  }

  private def stringIfEmpty(str: String): String = {
    if (str.isEmpty) "\"\"" else str
  }

  private def stringLengthExceedsMax(str: String, strSize: Int, delimiter: String): String = {
    val prettyPrintValue = if (str.size > strSize) {
      val newStrSize = strSize - 3 // suffixing ... at the end
      s"%${newStrSize}.${newStrSize}s...${delimiter}".format(str)
    } else {
      s"%${strSize}.${strSize}s${delimiter}".format(str)
    }
    prettyPrintValue
  }

  private def getUnsupportedOperatorsHeaderStrings: LinkedHashMap[String, Int] = {
    val detailedHeaderAndFields = LinkedHashMap[String, Int](
      APP_ID_STR -> APP_ID_STR.size,
      SQL_ID_STR -> SQL_ID_STR.size,
      STAGE_ID_STR -> STAGE_ID_STR.size,
      EXEC_ID -> EXEC_ID.size,
      UNSUPPORTED_TYPE -> UNSUPPORTED_TYPE.size,
      UNSUPPORTED_OPERATOR -> UNSUPPORTED_OPERATOR.size,
      DETAILS -> DETAILS.size,
      STAGE_WALLCLOCK_DUR_STR -> STAGE_WALLCLOCK_DUR_STR.size,
      APP_DUR_STR -> APP_DUR_STR.size,
      EXEC_ACTION -> EXEC_ACTION.size
    )
    detailedHeaderAndFields
  }

  private def getAllOperatorsHeaderStrings: LinkedHashMap[String, Int] = {
    val detailedHeaderAndFields = LinkedHashMap[String, Int](
      APP_ID_STR -> APP_ID_STR.size,
      SQL_ID_STR -> SQL_ID_STR.size,
      OPERATOR_TYPE -> OPERATOR_TYPE.size,
      OPERATOR_NAME -> OPERATOR_NAME.size,
      COUNT -> COUNT.size,
      IS_SUPPORTED -> IS_SUPPORTED.size,
      STAGES -> STAGES.size
    )
    detailedHeaderAndFields
  }

  def getDetailedHeaderStringsAndSizes(appInfos: Seq[QualificationSummaryInfo],
      reportReadSchema: Boolean): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_NAME_STR -> getMaxSizeForHeader(appInfos.map(_.appName.size), APP_NAME_STR),
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      SQL_DUR_STR -> SQL_DUR_STR_SIZE,
      TASK_DUR_STR -> TASK_DUR_STR.size,
      APP_DUR_STR -> APP_DUR_STR_SIZE,
      GPU_OPPORTUNITY_STR -> GPU_OPPORTUNITY_STR_SIZE,
      EXEC_CPU_PERCENT_STR -> EXEC_CPU_PERCENT_STR.size,
      SQL_IDS_FAILURES_STR -> getMaxSizeForHeader(appInfos.map(_.failedSQLIds.size),
        SQL_IDS_FAILURES_STR),
      READ_FILE_FORMAT_TYPES_STR ->
        getMaxSizeForHeader(appInfos.map(_.readFileFormats.map(_.length).sum),
          READ_FILE_FORMAT_TYPES_STR),
      WRITE_DATA_FORMAT_STR ->
        getMaxSizeForHeader(appInfos.map(_.writeDataFormat.map(_.length).sum),
          WRITE_DATA_FORMAT_STR),
      COMPLEX_TYPES_STR ->
        getMaxSizeForHeader(appInfos.map(_.complexTypes.size), COMPLEX_TYPES_STR),
      NESTED_TYPES_STR -> getMaxSizeForHeader(appInfos.map(_.nestedComplexTypes.size),
        NESTED_TYPES_STR),
      POT_PROBLEM_STR ->
        getMaxSizeForHeader(appInfos.map(_.potentialProblems.size), POT_PROBLEM_STR),
      LONGEST_SQL_DURATION_STR -> LONGEST_SQL_DURATION_STR_SIZE,
      SQL_STAGE_DUR_SUM_STR -> SQL_STAGE_DUR_SUM_STR.size,
      NONSQL_DUR_STR -> NONSQL_DUR_STR.size,
      UNSUPPORTED_TASK_DURATION_STR -> UNSUPPORTED_TASK_DURATION_STR.size,
      SUPPORTED_SQL_TASK_DURATION_STR -> SUPPORTED_SQL_TASK_DURATION_STR.size,
      APP_DUR_ESTIMATED_STR -> APP_DUR_ESTIMATED_STR.size,
      UNSUPPORTED_EXECS -> UNSUPPORTED_EXECS.size,
      UNSUPPORTED_EXPRS -> UNSUPPORTED_EXPRS.size,
      ESTIMATED_FREQUENCY -> ESTIMATED_FREQUENCY.size,
      TOTAL_CORE_SEC -> TOTAL_CORE_SEC.size
    )
    if (appInfos.exists(_.clusterTags.nonEmpty)) {
      detailedHeadersAndFields += (CLUSTER_TAGS -> getMaxSizeForHeader(
        appInfos.map(_.clusterTags.length), CLUSTER_TAGS))
    }
    if (reportReadSchema) {
      detailedHeadersAndFields +=
        (READ_SCHEMA_STR ->
          getMaxSizeForHeader(appInfos.map(_.readFileFormats.map(_.length).sum), READ_SCHEMA_STR))
    }
    detailedHeadersAndFields
  }

  private[qualification] def getSummaryHeaderStringsAndSizes(
      appNameMaxSize: Int,
      appIdMaxSize: Int,
      unSupExecMaxSize: Int = UNSUPPORTED_EXECS_MAX_SIZE,
      unSupExprMaxSize: Int = UNSUPPORTED_EXPRS_MAX_SIZE,
      estimatedFrequencyMaxSize: Int = ESTIMATED_FREQUENCY_MAX_SIZE,
      hasClusterTags: Boolean = false,
      clusterIdMaxSize: Int = CLUSTER_ID_STR_SIZE,
      jobIdMaxSize: Int = JOB_ID_STR_SIZE,
      runNameMaxSize: Int = RUN_NAME_STR_SIZE): LinkedHashMap[String, Int] = {
    val data = LinkedHashMap[String, Int](
      APP_NAME_STR -> appNameMaxSize,
      APP_ID_STR -> appIdMaxSize,
      APP_DUR_STR -> APP_DUR_STR_SIZE,
      SQL_DUR_STR -> SQL_DUR_STR_SIZE,
      GPU_OPPORTUNITY_STR -> GPU_OPPORTUNITY_STR_SIZE,
      UNSUPPORTED_EXECS -> unSupExecMaxSize,
      UNSUPPORTED_EXPRS -> unSupExprMaxSize,
      ESTIMATED_FREQUENCY -> estimatedFrequencyMaxSize
    )
    if (hasClusterTags) {
      data += (CLUSTER_ID -> clusterIdMaxSize)
      data += (JOB_ID -> jobIdMaxSize)
      data += (RUN_NAME -> runNameMaxSize)
    }
    data
  }

  def constructAppSummaryInfo(
      sumInfo: EstimatedSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      appIdMaxSize: Int,
      unSupExecMaxSize: Int,
      unSupExprMaxSize: Int,
      estimatedFrequencyMaxSize: Int,
      hasClusterTags: Boolean,
      clusterIdMaxSize: Int,
      jobIdMaxSize: Int,
      runNameMaxSize: Int,
      delimiter: String,
      prettyPrint: Boolean): String = {
    val data = ListBuffer[(String, Int)](
      sumInfo.estimatedInfo.appName -> headersAndSizes(APP_NAME_STR),
      sumInfo.estimatedInfo.appId -> appIdMaxSize,
      sumInfo.estimatedInfo.appDur.toString -> APP_DUR_STR_SIZE,
      sumInfo.estimatedInfo.sqlDfDuration.toString -> SQL_DUR_STR_SIZE,
      sumInfo.estimatedInfo.gpuOpportunity.toString -> GPU_OPPORTUNITY_STR_SIZE,
      sumInfo.estimatedInfo.unsupportedExecs -> unSupExecMaxSize,
      sumInfo.estimatedInfo.unsupportedExprs -> unSupExprMaxSize,
      sumInfo.estimatedFrequency.toString -> estimatedFrequencyMaxSize
    )
    if (hasClusterTags) {
      data += (sumInfo.estimatedInfo.allTagsMap.getOrElse(CLUSTER_ID, "") -> clusterIdMaxSize)
      data += (sumInfo.estimatedInfo.allTagsMap.getOrElse(JOB_ID, "") -> jobIdMaxSize)
      data += (sumInfo.estimatedInfo.allTagsMap.getOrElse(RUN_NAME, "") -> runNameMaxSize)
    }
    constructOutputRow(data, delimiter, prettyPrint)
  }

  def constructDetailedHeader(
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String,
      prettyPrint: Boolean): String = {
    QualOutputWriter.constructOutputRowFromMap(headersAndSizes, delimiter, prettyPrint)
  }

  def getDetailedPerSqlHeaderStringsAndSizes(
      appMaxNameSize: Int,
      appMaxIdSize: Int,
      sqlDescLength: Int): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_NAME_STR -> appMaxNameSize,
      APP_ID_STR -> appMaxIdSize,
      ROOT_SQL_ID_STR -> ROOT_SQL_ID_STR.size,
      SQL_ID_STR -> SQL_ID_STR.size,
      SQL_DESC_STR -> sqlDescLength,
      SQL_DUR_STR -> SQL_DUR_STR_SIZE,
      GPU_OPPORTUNITY_STR -> GPU_OPPORTUNITY_STR_SIZE
    )
    detailedHeadersAndFields
  }

  private def formatSQLDescription(sqlDesc: String, maxSQLDescLength: Int,
      delimiter: String): String = {
    val escapedMetaStr =
      StringUtils.renderStr(sqlDesc, doEscapeMetaCharacters = true, maxLength = maxSQLDescLength)
    // should be a one for one replacement so length wouldn't be affected by this
    replaceDelimiter(escapedMetaStr, delimiter)
  }

  def constructPerSqlSummaryInfo(
      sumInfo: EstimatedPerSQLSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      appIdMaxSize: Int,
      delimiter: String,
      prettyPrint: Boolean,
      maxSQLDescLength: Int,
      reformatCSV: Boolean = true): String = {
    val reformatCSVFunc : String => String =
      if (reformatCSV) str => StringUtils.reformatCSVString(str) else str => str
    val data = ListBuffer[(String, Int)](
      reformatCSVFunc(sumInfo.info.appName) -> headersAndSizes(APP_NAME_STR),
      reformatCSVFunc(sumInfo.info.appId) -> appIdMaxSize,
      reformatCSVFunc(sumInfo.rootExecutionID.getOrElse("").toString)-> ROOT_SQL_ID_STR.size,
      sumInfo.sqlID.toString -> SQL_ID_STR.size,
      reformatCSVFunc(formatSQLDescription(sumInfo.sqlDesc, maxSQLDescLength, delimiter)) ->
        headersAndSizes(SQL_DESC_STR),
      sumInfo.info.sqlDfDuration.toString -> SQL_DUR_STR_SIZE,
      sumInfo.info.gpuOpportunity.toString -> GPU_OPPORTUNITY_STR_SIZE
    )
    constructOutputRow(data, delimiter, prettyPrint)
  }

  private def getDetailedExecsHeaderStrings: LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_ID_STR -> APP_ID_STR.size,
      SQL_ID_STR -> SQL_ID_STR.size,
      EXEC_STR -> EXEC_STR.size,
      EXPR_STR -> EXPR_STR.size,
      EXEC_DURATION -> EXEC_DURATION.size,
      EXEC_NODEID -> EXEC_NODEID.size,
      EXEC_IS_SUPPORTED -> EXEC_IS_SUPPORTED.size,
      EXEC_STAGES -> EXEC_STAGES.size,
      EXEC_CHILDREN -> EXEC_CHILDREN.size,
      EXEC_CHILDREN_NODE_IDS -> EXEC_CHILDREN_NODE_IDS.size,
      EXEC_SHOULD_REMOVE -> EXEC_SHOULD_REMOVE.size,
      EXEC_SHOULD_IGNORE -> EXEC_SHOULD_IGNORE.size,
      EXEC_ACTION -> EXEC_ACTION.size)
    detailedHeadersAndFields
  }

  private def getClusterInfoHeaderStrings: mutable.LinkedHashMap[String, Int] = {
    val headersAndFields = Seq(
      APP_ID_STR, APP_NAME_STR, STATUS_REPORT_PATH_STR, VENDOR, DRIVER_HOST, CLUSTER_ID_STR,
      CLUSTER_NAME, WORKER_NODE_TYPE, DRIVER_NODE_TYPE, NUM_WORKER_NODES, NUM_EXECS_PER_NODE,
      NUM_EXECS, EXECUTOR_HEAP_MEMORY, DYN_ALLOC_ENABLED, DYN_ALLOC_MAX, DYN_ALLOC_MIN,
      DYN_ALLOC_INIT, CORES_PER_EXEC, RECOMMENDED_WORKER_NODE_TYPE, RECOMMENDED_NUM_EXECS,
      RECOMMENDED_NUM_WORKER_NODES, RECOMMENDED_CORES_PER_EXEC, RECOMMENDED_GPU_DEVICE,
      RECOMMENDED_NUM_GPUS, RECOMMENDED_VENDOR, RECOMMENDED_DYN_ALLOC_ENABLED,
      RECOMMENDED_DYN_ALLOC_MAX, RECOMMENDED_DYN_ALLOC_MIN, RECOMMENDED_DYN_ALLOC_INIT).map {
      key => (key, key.length)
    }
    mutable.LinkedHashMap(headersAndFields: _*)
  }

  def constructClusterInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean,
      reformatCSV: Boolean = true): Seq[String] = {
    val reformatCSVFunc = getReformatCSVFunc(reformatCSV)
    val clusterInfo = sumInfo.clusterSummary.clusterInfo
    val recClusterInfo = sumInfo.clusterSummary.recommendedClusterInfo

    // Wrapper function around reformatCSVFunc() to handle optional fields and
    // reduce redundancy
    def refactorCSVFuncWithOption(field: Option[String], headerConst: String): (String, Int) =
      reformatCSVFunc(field.getOrElse("")) -> headersAndSizes(headerConst)

    val data = ListBuffer(
      refactorCSVFuncWithOption(Some(sumInfo.clusterSummary.appId), APP_ID_STR),
      refactorCSVFuncWithOption(Some(sumInfo.clusterSummary.appName), APP_NAME_STR),
      refactorCSVFuncWithOption(sumInfo.clusterSummary.eventLogPath, STATUS_REPORT_PATH_STR),
      refactorCSVFuncWithOption(clusterInfo.map(_.vendor), VENDOR),
      refactorCSVFuncWithOption(clusterInfo.flatMap(_.driverHost), DRIVER_HOST),
      refactorCSVFuncWithOption(clusterInfo.flatMap(_.clusterId), CLUSTER_ID_STR),
      refactorCSVFuncWithOption(clusterInfo.flatMap(_.clusterName), CLUSTER_NAME),
      refactorCSVFuncWithOption(clusterInfo.flatMap(_.workerNodeType), WORKER_NODE_TYPE),
      refactorCSVFuncWithOption(clusterInfo.flatMap(_.driverNodeType), DRIVER_NODE_TYPE),
      refactorCSVFuncWithOption(clusterInfo.map(_.numWorkerNodes.toString), NUM_WORKER_NODES),
      refactorCSVFuncWithOption(clusterInfo.map(_.numExecsPerNode.toString), NUM_EXECS_PER_NODE),
      refactorCSVFuncWithOption(clusterInfo.map(_.numExecutors.toString), NUM_EXECS),
      refactorCSVFuncWithOption(clusterInfo.map(_.executorHeapMemory.toString),
        EXECUTOR_HEAP_MEMORY),
      refactorCSVFuncWithOption(clusterInfo.map(_.dynamicAllocationEnabled.toString),
        DYN_ALLOC_ENABLED),
      refactorCSVFuncWithOption(clusterInfo.map(_.dynamicAllocationMaxExecutors), DYN_ALLOC_MAX),
      refactorCSVFuncWithOption(clusterInfo.map(_.dynamicAllocationMinExecutors), DYN_ALLOC_MIN),
      refactorCSVFuncWithOption(clusterInfo.map(_.dynamicAllocationInitialExecutors),
        DYN_ALLOC_INIT),
      refactorCSVFuncWithOption(clusterInfo.map(_.coresPerExecutor.toString), CORES_PER_EXEC),
      refactorCSVFuncWithOption(recClusterInfo.flatMap(_.workerNodeType),
        RECOMMENDED_WORKER_NODE_TYPE),
      refactorCSVFuncWithOption(recClusterInfo.map(_.numExecutors.toString),
        RECOMMENDED_NUM_EXECS),
      refactorCSVFuncWithOption(recClusterInfo.map(_.numWorkerNodes.toString),
        RECOMMENDED_NUM_WORKER_NODES),
      refactorCSVFuncWithOption(recClusterInfo.map(_.coresPerExecutor.toString),
        RECOMMENDED_CORES_PER_EXEC),
      refactorCSVFuncWithOption(recClusterInfo.map(_.gpuDevice), RECOMMENDED_GPU_DEVICE),
      refactorCSVFuncWithOption(recClusterInfo.map(_.numGpus.toString), RECOMMENDED_NUM_GPUS),
      refactorCSVFuncWithOption(recClusterInfo.map(_.vendor), RECOMMENDED_VENDOR),
      refactorCSVFuncWithOption(recClusterInfo.map(_.dynamicAllocationEnabled.toString),
        RECOMMENDED_DYN_ALLOC_ENABLED),
      refactorCSVFuncWithOption(recClusterInfo.map(_.dynamicAllocationMaxExecutors),
        RECOMMENDED_DYN_ALLOC_MAX),
      refactorCSVFuncWithOption(recClusterInfo.map(_.dynamicAllocationMinExecutors),
        RECOMMENDED_DYN_ALLOC_MIN),
      refactorCSVFuncWithOption(recClusterInfo.map(_.dynamicAllocationInitialExecutors),
        RECOMMENDED_DYN_ALLOC_INIT)
    )
    constructOutputRow(data, delimiter, prettyPrint) :: Nil
  }

  def constructMlFuncsInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean,
      reformatCSV: Boolean = true): Seq[String] = {
    val reformatCSVFunc = getReformatCSVFunc(reformatCSV)
    val appId = sumInfo.appId
    sumInfo.mlFunctions.get.map { info =>
      val data = ListBuffer[(String, Int)](
        reformatCSVFunc(appId) -> headersAndSizes(APP_ID_STR),
        info.stageId.toString -> headersAndSizes(STAGE_ID_STR),
        reformatCSVFunc(ToolUtils.renderTextField(info.mlOps, ";", delimiter)) ->
          headersAndSizes(ML_FUNCTIONS),
        info.duration.toString -> headersAndSizes(STAGE_DUR_STR))
      constructOutputRow(data, delimiter, prettyPrint)
    }
  }

  def constructMlFuncsTotalDurationInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean,
      reformatCSV: Boolean = true): Seq[String] = {
    val reformatCSVFunc = getReformatCSVFunc(reformatCSV)
    val appId = sumInfo.appId
    sumInfo.mlFunctionsStageDurations.get.map { info =>
      val data = ListBuffer[(String, Int)](
        reformatCSVFunc(appId) -> headersAndSizes(APP_ID_STR),
        reformatCSVFunc(ToolUtils.renderTextField(info.stageIds, ";", delimiter)) ->
          headersAndSizes(ML_STAGE_IDS),
        reformatCSVFunc(info.mlFuncName) -> headersAndSizes(ML_FUNCTION_NAME),
        info.duration.toString -> headersAndSizes(ML_TOTAL_STAGE_DURATION))
      constructOutputRow(data, delimiter, prettyPrint)
    }
  }

  def flattenedExecs(execs: Seq[ExecInfo]): Seq[ExecInfo] = {
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

  def getDetailedMlFuncsHeaderStringsAndSizes(
      appInfos: Seq[QualificationSummaryInfo]): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      STAGE_ID_STR -> STAGE_ID_STR.size,
      ML_FUNCTIONS -> getMaxSizeForHeader(
        appInfos.map(_.mlFunctions.get.map(
          mlFuns => mlFuns.mlOps.map(funcName => funcName.length).sum).sum), ML_FUNCTIONS),
      STAGE_DUR_STR -> STAGE_DUR_STR.size)
    detailedHeadersAndFields
  }

  def getDetailedMlFuncsTotalDurationHeaderStringsAndSizes(
      appInfos: Seq[QualificationSummaryInfo]): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      ML_STAGE_IDS -> getMaxSizeForHeader(
        appInfos.map(_.mlFunctionsStageDurations.get.map(
          mlFuncs => mlFuncs.stageIds).size), ML_STAGE_IDS),
      ML_FUNCTION_NAME -> getMaxSizeForHeader(
        appInfos.map(_.mlFunctionsStageDurations.get.map(
          mlFuncs => mlFuncs.mlFuncName.length).sum), ML_FUNCTION_NAME),
      ML_TOTAL_STAGE_DURATION -> ML_TOTAL_STAGE_DURATION.size)
    detailedHeadersAndFields
  }

  private def constructExecInfoBuffer(
      info: ExecInfo,
      appId: String,
      delimiter: String,
      prettyPrint: Boolean,
      headersAndSizes: LinkedHashMap[String, Int],
      reformatCSV: Boolean = true): String = {
    val reformatCSVFunc = getReformatCSVFunc(reformatCSV)
    val data = ListBuffer[(String, Int)](
      reformatCSVFunc(appId) -> headersAndSizes(APP_ID_STR),
      info.sqlID.toString -> headersAndSizes(SQL_ID_STR),
      reformatCSVFunc(info.exec) -> headersAndSizes(EXEC_STR),
      reformatCSVFunc(info.expr) -> headersAndSizes(EXEC_STR),
      info.duration.getOrElse(0).toString -> headersAndSizes(EXEC_DURATION),
      info.nodeId.toString -> headersAndSizes(EXEC_NODEID),
      info.isSupported.toString -> headersAndSizes(EXEC_IS_SUPPORTED),
      reformatCSVFunc(info.stages.mkString(":")) -> headersAndSizes(EXEC_STAGES),
      reformatCSVFunc(info.children.getOrElse(Seq.empty).map(_.exec).mkString(":")) ->
        headersAndSizes(EXEC_CHILDREN),
      reformatCSVFunc(info.children.getOrElse(Seq.empty).map(_.nodeId).mkString(":")) ->
        headersAndSizes(EXEC_CHILDREN_NODE_IDS),
      info.shouldRemove.toString -> headersAndSizes(EXEC_SHOULD_REMOVE),
      info.shouldIgnore.toString -> headersAndSizes(EXEC_SHOULD_IGNORE),
      reformatCSVFunc(info.getOpAction.toString) -> headersAndSizes(EXEC_ACTION)
    )
    constructOutputRow(data, delimiter, prettyPrint)
  }

  private def getDetailedStagesHeaderStrings: LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_ID_STR -> APP_ID_STR.size,
      STAGE_ID_STR -> STAGE_ID_STR.size,
      STAGE_DUR_STR -> STAGE_DUR_STR.size,
      UNSUPPORTED_TASK_DURATION_STR -> UNSUPPORTED_TASK_DURATION_STR.size,
      STAGE_ESTIMATED_STR -> STAGE_ESTIMATED_STR.size,
      NUM_TRANSITIONS -> NUM_TRANSITIONS.size
    )
    detailedHeadersAndFields
  }

  def constructStagesInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean,
      reformatCSV: Boolean = true): Seq[String] = {
    val reformatCSVFunc: String => String =
      if (reformatCSV) str => StringUtils.reformatCSVString(str) else str => stringIfEmpty(str)
    sumInfo.stageInfo.map { info =>
      val data = ListBuffer[(String, Int)](
        reformatCSVFunc(sumInfo.appId) -> headersAndSizes(APP_ID_STR),
        info.stageId.toString -> headersAndSizes(STAGE_ID_STR),
        info.stageTaskTime.toString -> headersAndSizes(STAGE_DUR_STR),
        info.unsupportedTaskDur.toString -> headersAndSizes(UNSUPPORTED_TASK_DURATION_STR),
        info.estimated.toString -> headersAndSizes(STAGE_ESTIMATED_STR),
        info.numTransitions.toString -> headersAndSizes(NUM_TRANSITIONS))
      constructOutputRow(data, delimiter, prettyPrint)
    }
  }

  private def getUnsupportedExecsPerStage(
      sumInfo: QualificationSummaryInfo,
      stageID: Int): Set[ExecInfo] = {
    sumInfo.planInfo.map(_.execInfo).collect {
      case execInfos =>
        val allExecs = flattenedExecs(execInfos)
        allExecs.filter(exec => !exec.isSupported && exec.stages.contains(stageID))
    }.flatten.toSet
  }

  private def getUnsupportedExecsWithNoStage(
      sumInfo: QualificationSummaryInfo): Set[ExecInfo] = {
    sumInfo.planInfo.map(_.execInfo).collect {
      case execInfos =>
        val allExecs = flattenedExecs(execInfos)
        allExecs.filter(exec => exec.stages.isEmpty && !exec.isSupported)
    }.flatten.toSet
  }

  private def constructAllOperatorsInfo(
      csvWriter: ToolTextFileWriter,
      planInfo: Seq[PlanInfo],
      appId: String,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String,
      prettyPrint: Boolean): Unit = {

    val execInfos = planInfo.flatMap(_.execInfo)
    val analyzer = ExecInfoAnalyzer(execInfos)
    analyzer.analyze()
    val allOperators = analyzer.getResults

    allOperators.foreach { sqlResult =>
      val sqlID = sqlResult.sqlID.toString
      sqlResult.operators.foreach { operator =>
        val data = ListBuffer[(String, Int)](
          StringUtils.reformatCSVString(appId) -> headersAndSizes(APP_ID_STR),
          sqlID -> headersAndSizes(SQL_ID_STR),
          operator.opType.toString -> headersAndSizes(OPERATOR_TYPE),
          operator.execRef.value -> headersAndSizes(OPERATOR_NAME),
          operator.count.toString -> headersAndSizes(COUNT),
          operator.isSupported.toString -> headersAndSizes(IS_SUPPORTED),
          operator.stages.mkString(":") -> headersAndSizes(STAGES)
        )
        csvWriter.write(constructOutputRow(data, delimiter, prettyPrint))

        // Write expression data
        operator.expressions.foreach { expr =>
          val exprData = ListBuffer[(String, Int)](
            StringUtils.reformatCSVString(appId) -> headersAndSizes(APP_ID_STR),
            sqlID -> headersAndSizes(SQL_ID_STR),
            expr.opType.toString -> headersAndSizes(OPERATOR_TYPE),
            expr.exprRef.value -> headersAndSizes(OPERATOR_NAME),
            expr.count.toString -> headersAndSizes(COUNT),
            expr.isSupported.toString -> headersAndSizes(IS_SUPPORTED),
            expr.stages.mkString(":") -> headersAndSizes(STAGES)
          )
          csvWriter.write(constructOutputRow(exprData, delimiter, prettyPrint))
        }
      }
    }
  }

  private def constructUnsupportedDetailedStagesDurationInfo(
      csvWriter: ToolTextFileWriter,
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String,
      prettyPrint: Boolean,
      reformatCSV: Boolean = true): Unit = {
    val reformatCSVFunc = getReformatCSVFunc(reformatCSV)
    val appId = sumInfo.appId
    val appDuration = sumInfo.estimatedInfo.appDur
    val dummyStageID = -1
    val dummyStageDur = 0
    val execIdGenerator = new AtomicLong(0)

    def constructDetailedUnsupportedRow(unSupExecInfo: UnsupportedExecSummary,
        stageId: Int, stageAppDuration: Long): String = {
      val data = ListBuffer[(String, Int)](
        reformatCSVFunc(appId) -> headersAndSizes(APP_ID_STR),
        unSupExecInfo.sqlId.toString -> headersAndSizes(SQL_ID_STR),
        stageId.toString -> headersAndSizes(STAGE_ID_STR),
        reformatCSVFunc(unSupExecInfo.execId.toString) -> headersAndSizes(EXEC_ID),
        reformatCSVFunc(unSupExecInfo.finalOpType) -> headersAndSizes(UNSUPPORTED_TYPE),
        reformatCSVFunc(unSupExecInfo.unsupportedOperator) -> headersAndSizes(UNSUPPORTED_OPERATOR),
        reformatCSVFunc(unSupExecInfo.details) -> headersAndSizes(DETAILS),
        stageAppDuration.toString -> headersAndSizes(STAGE_WALLCLOCK_DUR_STR),
        appDuration.toString -> headersAndSizes(APP_DUR_STR),
        reformatCSVFunc(unSupExecInfo.opAction.toString) -> headersAndSizes(EXEC_ACTION)
      )
      constructOutputRow(data, delimiter, prettyPrint)
    }

    def getUnsupportedRows(execI: ExecInfo, stageId: Int, stageDur: Long): String = {
      val results = execI.getUnsupportedExecSummaryRecord(execIdGenerator.getAndIncrement())
      results.map { unsupportedExecSummary =>
        constructDetailedUnsupportedRow(unsupportedExecSummary, stageId, stageDur)
      }.mkString
    }

    csvWriter.write(sumInfo.origPlanStageInfo.flatMap { sInfo =>
      getUnsupportedExecsPerStage(sumInfo, sInfo.stageId).map { execInfo =>
          getUnsupportedRows(execInfo, sInfo.stageId, sInfo.stageWallclockDuration)
      }
    }.mkString)

    // write down the execs that are not attached to any stage
    csvWriter.write(getUnsupportedExecsWithNoStage(sumInfo).map { eInfo =>
      getUnsupportedRows(eInfo, dummyStageID, dummyStageDur)
    }.mkString)
  }

  def getAllExecsFromPlan(plans: Seq[PlanInfo]): Set[ExecInfo] = {
    val topExecInfo = plans.flatMap(_.execInfo)
    topExecInfo.flatMap { e =>
      e.children.getOrElse(Seq.empty) :+ e
    }.toSet
  }

  private def constructExecsInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String,
      prettyPrint: Boolean): Set[String] = {
    // No need to visit the execInfo children because the result returned from
    // "getAllExecsFromPlan" is already flattened
    getAllExecsFromPlan(sumInfo.planInfo).collect { case info =>
      constructExecInfoBuffer(info, sumInfo.appId, delimiter, prettyPrint, headersAndSizes)
    }
  }

  def createFormattedQualSummaryInfo(
      appInfo: QualificationSummaryInfo,
      delimiter: String = TEXT_DELIMITER) : FormattedQualificationSummaryInfo = {
    FormattedQualificationSummaryInfo(
      appInfo.appName,
      appInfo.appId,
      appInfo.estimatedInfo.sqlDfDuration,
      appInfo.sqlDataframeTaskDuration,
      appInfo.estimatedInfo.appDur,
      appInfo.estimatedInfo.gpuOpportunity,
      ToolUtils.truncateDoubleToTwoDecimal(appInfo.executorCpuTimePercent),
      ToolUtils.renderTextField(appInfo.failedSQLIds, ",", delimiter),
      ToolUtils.renderTextField(appInfo.readFileFormatAndTypesNotSupported, ";", delimiter),
      ToolUtils.renderTextField(appInfo.readFileFormats, ":", delimiter),
      ToolUtils.renderTextField(appInfo.writeDataFormat, ";", delimiter).toUpperCase,
      ToolUtils.formatComplexTypes(appInfo.complexTypes, delimiter),
      ToolUtils.formatComplexTypes(appInfo.nestedComplexTypes, delimiter),
      ToolUtils.formatPotentialProblems(appInfo.potentialProblems, delimiter),
      appInfo.longestSqlDuration,
      appInfo.stageInfo.map(_.stageWallclockDuration).sum,
      appInfo.nonSqlTaskDurationAndOverhead,
      appInfo.unsupportedSQLTaskDuration,
      appInfo.supportedSQLTaskDuration,
      appInfo.endDurationEstimated,
      appInfo.unSupportedExecs,
      appInfo.unSupportedExprs,
      appInfo.allClusterTagsMap,
      appInfo.estimatedFrequency.getOrElse(DEFAULT_JOB_FREQUENCY),
      appInfo.totalCoreSec
    )
  }

  private def constructDetailedAppInfoCSVRow(
      appInfo: FormattedQualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      reportReadSchema: Boolean,
      reformatCSV: Boolean = true): ListBuffer[(String, Int)] = {
    val reformatCSVFunc = getReformatCSVFunc(reformatCSV)
    val data = ListBuffer[(String, Int)](
      reformatCSVFunc(appInfo.appName) -> headersAndSizes(APP_NAME_STR),
      reformatCSVFunc(appInfo.appId) -> headersAndSizes(APP_ID_STR),
      appInfo.sqlDataframeDuration.toString -> headersAndSizes(SQL_DUR_STR),
      appInfo.sqlDataframeTaskDuration.toString -> headersAndSizes(TASK_DUR_STR),
      appInfo.appDuration.toString -> headersAndSizes(APP_DUR_STR),
      appInfo.gpuOpportunity.toString -> GPU_OPPORTUNITY_STR_SIZE,
      appInfo.executorCpuTimePercent.toString -> headersAndSizes(EXEC_CPU_PERCENT_STR),
      reformatCSVFunc(appInfo.failedSQLIds) -> headersAndSizes(SQL_IDS_FAILURES_STR),
      reformatCSVFunc(appInfo.readFileFormatAndTypesNotSupported) ->
        headersAndSizes(READ_FILE_FORMAT_TYPES_STR),
      reformatCSVFunc(appInfo.writeDataFormat) -> headersAndSizes(WRITE_DATA_FORMAT_STR),
      reformatCSVFunc(appInfo.complexTypes) -> headersAndSizes(COMPLEX_TYPES_STR),
      reformatCSVFunc(appInfo.nestedComplexTypes) -> headersAndSizes(NESTED_TYPES_STR),
      reformatCSVFunc(appInfo.potentialProblems) -> headersAndSizes(POT_PROBLEM_STR),
      appInfo.longestSqlDuration.toString -> headersAndSizes(LONGEST_SQL_DURATION_STR),
      appInfo.sqlStageDurationsSum.toString ->
        headersAndSizes(SQL_STAGE_DUR_SUM_STR),
      appInfo.nonSqlTaskDurationAndOverhead.toString -> headersAndSizes(NONSQL_DUR_STR),
      appInfo.unsupportedSQLTaskDuration.toString -> headersAndSizes(UNSUPPORTED_TASK_DURATION_STR),
      appInfo.supportedSQLTaskDuration.toString -> headersAndSizes(SUPPORTED_SQL_TASK_DURATION_STR),
      appInfo.endDurationEstimated.toString -> headersAndSizes(APP_DUR_ESTIMATED_STR),
      reformatCSVFunc(appInfo.unSupportedExecs) -> headersAndSizes(UNSUPPORTED_EXECS),
      reformatCSVFunc(appInfo.unSupportedExprs) -> headersAndSizes(UNSUPPORTED_EXPRS),
      appInfo.estimatedFrequency.toString -> headersAndSizes(ESTIMATED_FREQUENCY),
      appInfo.totalCoreSec.toString -> headersAndSizes(TOTAL_CORE_SEC)
    )

    if (appInfo.clusterTags.nonEmpty) {
      data += reformatCSVFunc(appInfo.clusterTags.mkString(";")) -> headersAndSizes(CLUSTER_TAGS)
    }
    if (reportReadSchema) {
      data += reformatCSVFunc(appInfo.readFileFormats) -> headersAndSizes(READ_SCHEMA_STR)
    }
    data
  }

  def constructAppDetailedInfo(
      summaryAppInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String,
      prettyPrint: Boolean,
      reportReadSchema: Boolean): String = {
    val formattedAppInfo = createFormattedQualSummaryInfo(summaryAppInfo, delimiter)
    val data = constructDetailedAppInfoCSVRow(formattedAppInfo, headersAndSizes, reportReadSchema)
    constructOutputRow(data, delimiter, prettyPrint)
  }

  private def getDetailedStatusHeaderStringsAndSizes(
      statusInfos: Seq[AppStatusResult]): LinkedHashMap[String, Int] = {
    val descLengthList = statusInfos.map { statusInfo =>
      statusInfo.appId.length + statusInfo.message.length + 1
    }
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      STATUS_REPORT_PATH_STR ->
        getMaxSizeForHeader(statusInfos.map(_.path.length), STATUS_REPORT_PATH_STR),
      STATUS_REPORT_STATUS_STR ->
        getMaxSizeForHeader(statusInfos.map(_.status.length), STATUS_REPORT_STATUS_STR),
      STATUS_REPORT_APP_ID ->
        getMaxSizeForHeader(statusInfos.map(_.appId.length), STATUS_REPORT_APP_ID),
      STATUS_REPORT_DESC_STR ->
        getMaxSizeForHeader(descLengthList, STATUS_REPORT_DESC_STR)
    )
    detailedHeadersAndFields
  }

  private def constructStatusReportInfo(
      statusInfo: AppStatusResult,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String,
      prettyPrint: Boolean,
      reformatCSV: Boolean = true): Seq[String] = {
    val reformatCSVFunc = getReformatCSVFunc(reformatCSV)
    val data = ListBuffer[(String, Int)](
      reformatCSVFunc(statusInfo.path) -> headersAndSizes(STATUS_REPORT_PATH_STR),
      reformatCSVFunc(statusInfo.status) -> headersAndSizes(STATUS_REPORT_STATUS_STR),
      reformatCSVFunc(statusInfo.appId) -> headersAndSizes(STATUS_REPORT_APP_ID),
      reformatCSVFunc(statusInfo.message) -> headersAndSizes(STATUS_REPORT_DESC_STR))
    Seq(constructOutputRow(data, delimiter, prettyPrint))
  }
}
