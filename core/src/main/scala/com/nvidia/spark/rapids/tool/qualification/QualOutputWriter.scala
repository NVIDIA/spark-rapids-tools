/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable.{ArrayBuffer, Buffer, LinkedHashMap, ListBuffer}

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import com.nvidia.spark.rapids.tool.planparser.{ExecInfo, PlanInfo}
import com.nvidia.spark.rapids.tool.profiling.ProfileUtils.replaceDelimiter
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter.{CLUSTER_ID, CLUSTER_ID_STR_SIZE, JOB_ID, JOB_ID_STR_SIZE, RUN_NAME, RUN_NAME_STR_SIZE, TEXT_DELIMITER}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.qualification.{EstimatedPerSQLSummaryInfo, EstimatedSummaryInfo, QualificationAppInfo, QualificationSummaryInfo, StatusSummaryInfo}
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
      QualOutputWriter.CSV_DELIMITER, false))
    sums.foreach { sum =>
      csvFileWriter.write(QualOutputWriter.constructAppDetailedInfo(sum, headersAndSizes,
        QualOutputWriter.CSV_DELIMITER, false, reportReadSchema))
    }
  }

  // write the text summary report
  def writeTextReport(sums: Seq[QualificationSummaryInfo], estSums: Seq[EstimatedSummaryInfo],
      numOutputRows: Int) : Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir, s"${QualOutputWriter.LOGFILE_NAME}.log",
      "Summary Report", hadoopConf)
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
      TEXT_DELIMITER, true)
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
      val headersAndSizes = QualOutputWriter
        .getDetailedStagesHeaderStringsAndSizes(sums)
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", false))
      sums.foreach { sumInfo =>
        val rows = QualOutputWriter.constructStagesInfo(sumInfo, headersAndSizes, ",", false)
        rows.foreach(csvFileWriter.write(_))
      }
    } finally {
      csvFileWriter.close()
    }
  }

  def writeUnsupportedOperatorsCSVReport(sums: Seq[QualificationSummaryInfo],
      order: String): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_unsupportedOperators.csv",
      "Unsupported Operators CSV Report", hadoopConf)
    val headersAndSizes = QualOutputWriter.getUnsupportedOperatorsHeaderStringsAndSizes(sums)
    csvFileWriter.write(QualOutputWriter.constructOutputRowFromMap(headersAndSizes,
      QualOutputWriter.CSV_DELIMITER, false))
    sums.foreach { sum =>
      val rows = QualOutputWriter.constructUnsupportedOperatorsInfo(sum, headersAndSizes,
        QualOutputWriter.CSV_DELIMITER, false)
      rows.foreach(row => csvFileWriter.write(row))
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
      (sum.info.recommendation, sum.info.estimatedGpuSpeedup,
        sum.info.estimatedGpuTimeSaved, sum.info.appDur, sum.info.appId)
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
      maxSQLDescLength: Int) : Unit = {
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
      val plans = sums.flatMap(_.planInfo)
      val allExecs = QualOutputWriter.getAllExecsFromPlan(plans)
      val headersAndSizes = QualOutputWriter
        .getDetailedExecsHeaderStringsAndSizes(sums, allExecs.toSeq)
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", false))
      sums.foreach { sumInfo =>
        val rows = QualOutputWriter.constructExecsInfo(sumInfo, headersAndSizes, ",", false)
        rows.foreach(csvFileWriter.write(_))
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

  def writeStatusReport(statusReports: Seq[StatusSummaryInfo], order: String): Unit = {
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
    recommendation: String,
    estimatedGpuSpeedup: Double,
    estimatedGpuDur: Double,
    estimatedGpuTimeSaved: Double,
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
    nonSqlTaskDurationAndOverhead: Long,
    unsupportedSQLTaskDuration: Long,
    supportedSQLTaskDuration: Long,
    taskSpeedupFactor: Double,
    endDurationEstimated: Boolean,
    unSupportedExecs: String,
    unSupportedExprs: String,
    clusterTags: Map[String, String],
    estimatedFrequency: Long)

object QualOutputWriter {
  val NON_SQL_TASK_DURATION_STR = "NonSQL Task Duration"
  val SQL_ID_STR = "SQL ID"
  val SQL_DESC_STR = "SQL Description"
  val STAGE_ID_STR = "Stage ID"
  val APP_ID_STR = "App ID"
  val APP_NAME_STR = "App Name"
  val APP_DUR_STR = "App Duration"
  val SQL_DUR_STR = "SQL DF Duration"
  val TASK_DUR_STR = "SQL Dataframe Task Duration"
  val STAGE_DUR_STR = "Stage Task Duration"
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
  val SPEEDUP_FACTOR_STR = "Task Speedup Factor"
  val AVERAGE_SPEEDUP_STR = "Average Speedup Factor"
  val SPEEDUP_BUCKET_STR = "Recommendation"
  val LONGEST_SQL_DURATION_STR = "Longest SQL Duration"
  val EXEC_STR = "Exec Name"
  val EXPR_STR = "Expression Name"
  val EXEC_DURATION = "Exec Duration"
  val EXEC_NODEID = "SQL Node Id"
  val EXEC_IS_SUPPORTED = "Exec Is Supported"
  val EXEC_STAGES = "Exec Stages"
  val EXEC_SHOULD_REMOVE = "Exec Should Remove"
  val EXEC_CHILDREN = "Exec Children"
  val EXEC_CHILDREN_NODE_IDS = "Exec Children Node Ids"
  val GPU_OPPORTUNITY_STR = "GPU Opportunity"
  val ESTIMATED_GPU_DURATION = "Estimated GPU Duration"
  val ESTIMATED_GPU_SPEEDUP = "Estimated GPU Speedup"
  val ESTIMATED_GPU_TIMESAVED = "Estimated GPU Time Saved"
  val STAGE_ESTIMATED_STR = "Stage Estimated"
  val NUM_TRANSITIONS = "Number of transitions from or to GPU"
  val UNSUPPORTED_EXECS = "Unsupported Execs"
  val UNSUPPORTED_EXPRS = "Unsupported Expressions"
  val CLUSTER_TAGS = "Cluster Tags"
  val CLUSTER_ID = "ClusterId"
  val JOB_ID = "JobId"
  val UNSUPPORTED_TYPE = "Unsupported Type"
  val DETAILS = "Details"
  val NOTES = "Notes"
  val RUN_NAME = "RunName"
  val ESTIMATED_FREQUENCY = "Estimated Job Frequency (monthly)"
  val ML_FUNCTIONS = "ML Functions"
  val ML_FUNCTION_NAME = "ML Function Name"
  val ML_TOTAL_STAGE_DURATION = "Total Duration"
  val ML_STAGE_IDS = "Stage Ids"
  val STATUS_REPORT_PATH_STR = "Event Log"
  val STATUS_REPORT_STATUS_STR = "Status"
  val STATUS_REPORT_DESC_STR = "Description"
  // Default frequency for jobs with a single instance is 30 times every month (30 days)
  val DEFAULT_JOB_FREQUENCY = 30L

  val APP_DUR_STR_SIZE: Int = APP_DUR_STR.size
  val SQL_DUR_STR_SIZE: Int = SQL_DUR_STR.size
  val NON_SQL_TASK_DURATION_SIZE: Int = NON_SQL_TASK_DURATION_STR.size
  val SPEEDUP_BUCKET_STR_SIZE: Int = QualificationAppInfo.STRONGLY_RECOMMENDED.size
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
    if (sizes.size > 0 && sizes.max > headerTxtStr.size) {
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
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean = false): String = {
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

  private def stringIfempty(str: String): String = {
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

  def getUnsupportedOperatorsHeaderStringsAndSizes(
      appInfos: Seq[QualificationSummaryInfo]): LinkedHashMap[String, Int] = {
    val detailedHeaderAndFields = LinkedHashMap[String, Int](
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      UNSUPPORTED_TYPE -> UNSUPPORTED_TYPE.size,
      DETAILS -> DETAILS.size,
      NOTES -> NOTES.size
    )
    detailedHeaderAndFields
  }


  def getDetailedHeaderStringsAndSizes(appInfos: Seq[QualificationSummaryInfo],
      reportReadSchema: Boolean): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_NAME_STR -> getMaxSizeForHeader(appInfos.map(_.appName.size), APP_NAME_STR),
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      SPEEDUP_BUCKET_STR -> SPEEDUP_BUCKET_STR_SIZE,
      ESTIMATED_GPU_SPEEDUP -> ESTIMATED_GPU_SPEEDUP.size,
      ESTIMATED_GPU_DURATION -> ESTIMATED_GPU_DURATION.size,
      ESTIMATED_GPU_TIMESAVED -> ESTIMATED_GPU_TIMESAVED.size,
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
      NONSQL_DUR_STR -> NONSQL_DUR_STR.size,
      UNSUPPORTED_TASK_DURATION_STR -> UNSUPPORTED_TASK_DURATION_STR.size,
      SUPPORTED_SQL_TASK_DURATION_STR -> SUPPORTED_SQL_TASK_DURATION_STR.size,
      SPEEDUP_FACTOR_STR -> SPEEDUP_FACTOR_STR.size,
      APP_DUR_ESTIMATED_STR -> APP_DUR_ESTIMATED_STR.size,
      UNSUPPORTED_EXECS -> UNSUPPORTED_EXECS.size,
      UNSUPPORTED_EXPRS -> UNSUPPORTED_EXPRS.size,
      ESTIMATED_FREQUENCY -> ESTIMATED_FREQUENCY.size
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
      ESTIMATED_GPU_DURATION -> ESTIMATED_GPU_DURATION.size,
      ESTIMATED_GPU_SPEEDUP -> ESTIMATED_GPU_SPEEDUP.size,
      ESTIMATED_GPU_TIMESAVED -> ESTIMATED_GPU_TIMESAVED.size,
      SPEEDUP_BUCKET_STR -> SPEEDUP_BUCKET_STR_SIZE,
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
      ToolUtils.formatDoublePrecision(sumInfo.estimatedInfo.estimatedGpuDur) ->
        ESTIMATED_GPU_DURATION.size,
      ToolUtils.formatDoublePrecision(sumInfo.estimatedInfo.estimatedGpuSpeedup) ->
        ESTIMATED_GPU_SPEEDUP.size,
      ToolUtils.formatDoublePrecision(sumInfo.estimatedInfo.estimatedGpuTimeSaved) ->
        ESTIMATED_GPU_TIMESAVED.size,
      sumInfo.estimatedInfo.recommendation -> SPEEDUP_BUCKET_STR_SIZE,
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

  private def getChildrenSize(execInfos: Seq[ExecInfo]): Seq[Int] = {
    execInfos.map(_.children.getOrElse(Seq.empty).mkString(",").size)
  }

  private def getChildrenNodeIdsSize(execInfos: Seq[ExecInfo]): Seq[Int] = {
    execInfos.map(_.children.getOrElse(Seq.empty).map(_.nodeId).mkString(",").size)
  }
  def getDetailedPerSqlHeaderStringsAndSizes(
      appMaxNameSize: Int,
      appMaxIdSize: Int,
      sqlDescLength: Int): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_NAME_STR -> appMaxNameSize,
      APP_ID_STR -> appMaxIdSize,
      SQL_ID_STR -> SQL_ID_STR.size,
      SQL_DESC_STR -> sqlDescLength,
      SQL_DUR_STR -> SQL_DUR_STR_SIZE,
      GPU_OPPORTUNITY_STR -> GPU_OPPORTUNITY_STR_SIZE,
      ESTIMATED_GPU_DURATION -> ESTIMATED_GPU_DURATION.size,
      ESTIMATED_GPU_SPEEDUP -> ESTIMATED_GPU_SPEEDUP.size,
      ESTIMATED_GPU_TIMESAVED -> ESTIMATED_GPU_TIMESAVED.size,
      SPEEDUP_BUCKET_STR -> SPEEDUP_BUCKET_STR_SIZE
    )
    detailedHeadersAndFields
  }

  private def formatSQLDescription(sqlDesc: String, maxSQLDescLength: Int,
      delimiter: String): String = {
    val escapedMetaStr = ToolUtils.escapeMetaCharacters(sqlDesc).trim()
    val sqlDescTruncated = escapedMetaStr.substring(0,
      Math.min(maxSQLDescLength, escapedMetaStr.length))
    // should be a one for one replacement so length wouldn't be affected by this
    replaceDelimiter(sqlDescTruncated, delimiter)
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
      sumInfo.sqlID.toString -> SQL_ID_STR.size,
      reformatCSVFunc(formatSQLDescription(sumInfo.sqlDesc, maxSQLDescLength, delimiter)) ->
        headersAndSizes(SQL_DESC_STR),
      sumInfo.info.sqlDfDuration.toString -> SQL_DUR_STR_SIZE,
      sumInfo.info.gpuOpportunity.toString -> GPU_OPPORTUNITY_STR_SIZE,
      ToolUtils.formatDoublePrecision(sumInfo.info.estimatedGpuDur) -> ESTIMATED_GPU_DURATION.size,
      ToolUtils.formatDoublePrecision(sumInfo.info.estimatedGpuSpeedup) ->
        ESTIMATED_GPU_SPEEDUP.size,
      ToolUtils.formatDoublePrecision(sumInfo.info.estimatedGpuTimeSaved) ->
        ESTIMATED_GPU_TIMESAVED.size,
      reformatCSVFunc(sumInfo.info.recommendation) -> SPEEDUP_BUCKET_STR_SIZE
    )
    constructOutputRow(data, delimiter, prettyPrint)
  }

  def getDetailedExecsHeaderStringsAndSizes(appInfos: Seq[QualificationSummaryInfo],
      execInfos: Seq[ExecInfo]): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      SQL_ID_STR -> SQL_ID_STR.size,
      EXEC_STR -> getMaxSizeForHeader(execInfos.map(_.exec.size), EXEC_STR),
      EXPR_STR -> getMaxSizeForHeader(execInfos.map(_.expr.size), EXPR_STR),
      SPEEDUP_FACTOR_STR -> SPEEDUP_FACTOR_STR.size,
      EXEC_DURATION -> EXEC_DURATION.size,
      EXEC_NODEID -> EXEC_NODEID.size,
      EXEC_IS_SUPPORTED -> EXEC_IS_SUPPORTED.size,
      EXEC_STAGES -> getMaxSizeForHeader(execInfos.map(_.stages.mkString(",").size), EXEC_STAGES),
      EXEC_CHILDREN -> getMaxSizeForHeader(getChildrenSize(execInfos), EXEC_CHILDREN),
      EXEC_CHILDREN_NODE_IDS -> getMaxSizeForHeader(getChildrenNodeIdsSize(execInfos),
        EXEC_CHILDREN_NODE_IDS),
      EXEC_SHOULD_REMOVE -> EXEC_SHOULD_REMOVE.size
    )
    detailedHeadersAndFields
  }

  def constructMlFuncsInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean,
      reformatCSV: Boolean = true): Seq[String] = {
    val reformatCSVFunc : String => String =
      if (reformatCSV) str => StringUtils.reformatCSVString(str) else str => stringIfempty(str)
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
    val reformatCSVFunc : String => String =
      if (reformatCSV) str => StringUtils.reformatCSVString(str) else str => stringIfempty(str)
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
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean,
      headersAndSizes: LinkedHashMap[String, Int],
      reformatCSV: Boolean = true): String = {
    val reformatCSVFunc : String => String =
      if (reformatCSV) str => StringUtils.reformatCSVString(str) else str => stringIfempty(str)
    val data = ListBuffer[(String, Int)](
      reformatCSVFunc(appId) -> headersAndSizes(APP_ID_STR),
      info.sqlID.toString -> headersAndSizes(SQL_ID_STR),
      reformatCSVFunc(info.exec) -> headersAndSizes(EXEC_STR),
      reformatCSVFunc(info.expr) -> headersAndSizes(EXEC_STR),
      ToolUtils.formatDoublePrecision(info.speedupFactor) -> headersAndSizes(SPEEDUP_FACTOR_STR),
      info.duration.getOrElse(0).toString -> headersAndSizes(EXEC_DURATION),
      info.nodeId.toString -> headersAndSizes(EXEC_NODEID),
      info.isSupported.toString -> headersAndSizes(EXEC_IS_SUPPORTED),
      reformatCSVFunc(info.stages.mkString(":")) -> headersAndSizes(EXEC_STAGES),
      reformatCSVFunc(info.children.getOrElse(Seq.empty).map(_.exec).mkString(":")) ->
        headersAndSizes(EXEC_CHILDREN),
      reformatCSVFunc(info.children.getOrElse(Seq.empty).map(_.nodeId).mkString(":")) ->
        headersAndSizes(EXEC_CHILDREN_NODE_IDS),
      info.shouldRemove.toString -> headersAndSizes(EXEC_SHOULD_REMOVE))
    constructOutputRow(data, delimiter, prettyPrint)
  }

  def getDetailedStagesHeaderStringsAndSizes(
      appInfos: Seq[QualificationSummaryInfo]): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      STAGE_ID_STR -> STAGE_ID_STR.size,
      AVERAGE_SPEEDUP_STR -> AVERAGE_SPEEDUP_STR.size,
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
    val reformatCSVFunc : String => String =
      if (reformatCSV) str => StringUtils.reformatCSVString(str) else str => stringIfempty(str)
    val appId = sumInfo.appId
    sumInfo.stageInfo.map { info =>
      val data = ListBuffer[(String, Int)](
        reformatCSVFunc(appId) -> headersAndSizes(APP_ID_STR),
        info.stageId.toString -> headersAndSizes(STAGE_ID_STR),
        ToolUtils.formatDoublePrecision(info.averageSpeedup) ->
          headersAndSizes(AVERAGE_SPEEDUP_STR),
        info.stageTaskTime.toString -> headersAndSizes(STAGE_DUR_STR),
        info.unsupportedTaskDur.toString -> headersAndSizes(UNSUPPORTED_TASK_DURATION_STR),
        info.estimated.toString -> headersAndSizes(STAGE_ESTIMATED_STR),
        info.numTransitions.toString -> headersAndSizes(NUM_TRANSITIONS))
      constructOutputRow(data, delimiter, prettyPrint)
    }
  }

  def constructUnsupportedOperatorsInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean,
      reformatCSV: Boolean = true): Seq[String] = {
    val reformatCSVFunc: String => String =
      if (reformatCSV) str => StringUtils.reformatCSVString(str) else str => stringIfempty(str)
    val appId = sumInfo.appId
    val readFormat = sumInfo.readFileFormatAndTypesNotSupported
    val writeFormat = sumInfo.writeDataFormat
    val unsupportedExecs = sumInfo.unSupportedExecs
    val unsupportedExprs = sumInfo.unSupportedExprs
    val unsupportedExecExprsMap = sumInfo.unsupportedExecstoExprsMap
    val unsupportedOperatorsOutputRows = new ArrayBuffer[String]()

    if (readFormat.nonEmpty) {
      val unsupportedReadFormatRows = readFormat.map { format =>
        val readFormatAndType = format.split("\\[")
        val readFormat = readFormatAndType(0)
        val readType = if (readFormatAndType.size > 1) {
          s"Types not supported - ${readFormatAndType(1).replace("]", "")}"
        } else {
          ""
        }
        val data = ListBuffer(
          reformatCSVFunc(appId) -> headersAndSizes(APP_ID_STR),
          reformatCSVFunc("Read")-> headersAndSizes(UNSUPPORTED_TYPE),
          reformatCSVFunc(readFormat) -> headersAndSizes(DETAILS),
          reformatCSVFunc(readType) -> headersAndSizes(NOTES)
        )
        constructOutputRow(data, delimiter, prettyPrint)
      }
      unsupportedOperatorsOutputRows ++= unsupportedReadFormatRows
    }
    if (unsupportedExecs.nonEmpty) {
      val unsupportedExecRows = unsupportedExecs.split(";").map { exec =>
        val data = ListBuffer(
          reformatCSVFunc(appId) -> headersAndSizes(APP_ID_STR),
          reformatCSVFunc("Exec") -> headersAndSizes(UNSUPPORTED_TYPE),
          reformatCSVFunc(exec) -> headersAndSizes(DETAILS),
          reformatCSVFunc("") -> headersAndSizes(NOTES)
        )
        constructOutputRow(data, delimiter, prettyPrint)
      }
      unsupportedOperatorsOutputRows ++= unsupportedExecRows
    }
    if (unsupportedExecExprsMap.nonEmpty) {
      val unsupportedExecExprMapRows = unsupportedExecExprsMap.map { case (exec, exprs) =>
        val data = ListBuffer(
          reformatCSVFunc(appId) -> headersAndSizes(APP_ID_STR),
          reformatCSVFunc("Exec") -> headersAndSizes(UNSUPPORTED_TYPE),
          reformatCSVFunc(exec) -> headersAndSizes(DETAILS),
          reformatCSVFunc(s"$exec Exec is not supported as expressions are " +
            s"not supported -  `$exprs`") -> headersAndSizes(NOTES)
        )
        constructOutputRow(data, delimiter, prettyPrint)
      }.toArray
      unsupportedOperatorsOutputRows ++= unsupportedExecExprMapRows
    }
    if (unsupportedExprs.nonEmpty) {
      val unsupportedExprRows = unsupportedExprs.split(";").map { expr =>
        val data = ListBuffer(
          reformatCSVFunc(appId) -> headersAndSizes(APP_ID_STR),
          reformatCSVFunc("Expression") -> headersAndSizes(UNSUPPORTED_TYPE),
          reformatCSVFunc(expr) -> headersAndSizes(DETAILS),
          reformatCSVFunc("") -> headersAndSizes(NOTES)
        )
        constructOutputRow(data, delimiter, prettyPrint)
      }
      unsupportedOperatorsOutputRows ++= unsupportedExprRows
    }
    if (writeFormat.nonEmpty) {
      val unsupportedwriteFormatRows = writeFormat.map { format =>
        val data = ListBuffer(
          reformatCSVFunc(appId) -> headersAndSizes(APP_ID_STR),
          reformatCSVFunc("Write") -> headersAndSizes(UNSUPPORTED_TYPE),
          reformatCSVFunc(format) -> headersAndSizes(DETAILS),
          reformatCSVFunc("") -> headersAndSizes(NOTES)
        )
        constructOutputRow(data, delimiter, prettyPrint)
      }
      unsupportedOperatorsOutputRows ++= unsupportedwriteFormatRows
    }
    unsupportedOperatorsOutputRows
  }

  def getAllExecsFromPlan(plans: Seq[PlanInfo]): Set[ExecInfo] = {
    val topExecInfo = plans.flatMap(_.execInfo)
    topExecInfo.flatMap { e =>
      e.children.getOrElse(Seq.empty) :+ e
    }.toSet
  }

  def constructExecsInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean): Set[String] = {
    val allExecs = getAllExecsFromPlan(sumInfo.planInfo)
    val appId = sumInfo.appId
    allExecs.flatMap { info =>
      val children = info.children
        .map(_.map(constructExecInfoBuffer(
          _, appId, delimiter, prettyPrint, headersAndSizes)))
        .getOrElse(Seq.empty)
      children :+ constructExecInfoBuffer(
        info, appId, delimiter, prettyPrint, headersAndSizes)
    }
  }

  def createFormattedQualSummaryInfo(
      appInfo: QualificationSummaryInfo,
      delimiter: String = TEXT_DELIMITER) : FormattedQualificationSummaryInfo = {
    FormattedQualificationSummaryInfo(
      appInfo.appName,
      appInfo.appId,
      appInfo.estimatedInfo.recommendation,
      ToolUtils.truncateDoubleToTwoDecimal(appInfo.estimatedInfo.estimatedGpuSpeedup),
      ToolUtils.truncateDoubleToTwoDecimal(appInfo.estimatedInfo.estimatedGpuDur),
      ToolUtils.truncateDoubleToTwoDecimal(appInfo.estimatedInfo.estimatedGpuTimeSaved),
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
      appInfo.nonSqlTaskDurationAndOverhead,
      appInfo.unsupportedSQLTaskDuration,
      appInfo.supportedSQLTaskDuration,
      ToolUtils.truncateDoubleToTwoDecimal(appInfo.taskSpeedupFactor),
      appInfo.endDurationEstimated,
      appInfo.unSupportedExecs,
      appInfo.unSupportedExprs,
      appInfo.allClusterTagsMap,
      appInfo.estimatedFrequency.getOrElse(DEFAULT_JOB_FREQUENCY)
    )
  }

  private def constructDetailedAppInfoCSVRow(
      appInfo: FormattedQualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      reportReadSchema: Boolean = false,
      reformatCSV: Boolean = true): ListBuffer[(String, Int)] = {
    val reformatCSVFunc : String => String =
      if (reformatCSV) str => StringUtils.reformatCSVString(str) else str => stringIfempty(str)
    val data = ListBuffer[(String, Int)](
      reformatCSVFunc(appInfo.appName) -> headersAndSizes(APP_NAME_STR),
      reformatCSVFunc(appInfo.appId) -> headersAndSizes(APP_ID_STR),
      reformatCSVFunc(appInfo.recommendation) -> headersAndSizes(SPEEDUP_BUCKET_STR),
      appInfo.estimatedGpuSpeedup.toString -> ESTIMATED_GPU_SPEEDUP.size,
      appInfo.estimatedGpuDur.toString -> ESTIMATED_GPU_DURATION.size,
      appInfo.estimatedGpuTimeSaved.toString -> ESTIMATED_GPU_TIMESAVED.size,
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
      appInfo.nonSqlTaskDurationAndOverhead.toString -> headersAndSizes(NONSQL_DUR_STR),
      appInfo.unsupportedSQLTaskDuration.toString -> headersAndSizes(UNSUPPORTED_TASK_DURATION_STR),
      appInfo.supportedSQLTaskDuration.toString -> headersAndSizes(SUPPORTED_SQL_TASK_DURATION_STR),
      appInfo.taskSpeedupFactor.toString -> headersAndSizes(SPEEDUP_FACTOR_STR),
      appInfo.endDurationEstimated.toString -> headersAndSizes(APP_DUR_ESTIMATED_STR),
      reformatCSVFunc(appInfo.unSupportedExecs) -> headersAndSizes(UNSUPPORTED_EXECS),
      reformatCSVFunc(appInfo.unSupportedExprs) -> headersAndSizes(UNSUPPORTED_EXPRS),
      appInfo.estimatedFrequency.toString -> headersAndSizes(ESTIMATED_FREQUENCY)
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
      statusInfos: Seq[StatusSummaryInfo]): LinkedHashMap[String, Int] = {
    val descLengthList = statusInfos.map { statusInfo =>
      statusInfo.appId.length + statusInfo.message.length + 1
    }
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      STATUS_REPORT_PATH_STR ->
        getMaxSizeForHeader(statusInfos.map(_.path.length), STATUS_REPORT_PATH_STR),
      STATUS_REPORT_STATUS_STR ->
        getMaxSizeForHeader(statusInfos.map(_.status.length), STATUS_REPORT_STATUS_STR),
      STATUS_REPORT_DESC_STR ->
        getMaxSizeForHeader(descLengthList, STATUS_REPORT_DESC_STR)
    )
    detailedHeadersAndFields
  }

  private def constructStatusReportInfo(
      statusInfo: StatusSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean,
      reformatCSV: Boolean = true): Seq[String] = {
    val reformatCSVFunc: String => String =
      if (reformatCSV) str => StringUtils.reformatCSVString(str) else str => stringIfempty(str)
    val descriptionStr = statusInfo.appId match {
      case "" => statusInfo.message
      case appId => if (statusInfo.message.isEmpty) appId else s"$appId,${statusInfo.message}"
    }
    val data = ListBuffer[(String, Int)](
      reformatCSVFunc(statusInfo.path) -> headersAndSizes(STATUS_REPORT_PATH_STR),
      reformatCSVFunc(statusInfo.status) -> headersAndSizes(STATUS_REPORT_STATUS_STR),
      reformatCSVFunc(descriptionStr) -> headersAndSizes(STATUS_REPORT_DESC_STR))
    Seq(constructOutputRow(data, delimiter, prettyPrint))
  }
}
