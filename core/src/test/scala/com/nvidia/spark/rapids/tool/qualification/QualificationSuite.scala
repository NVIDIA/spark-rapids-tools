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

import java.io.{File, PrintWriter}
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.nvidia.spark.rapids.BaseTestSuite
import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, PlatformNames, StatusReportCounts, ToolTestUtils}
import com.nvidia.spark.rapids.tool.planparser.DatabricksParseHelper
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, TrampolineUtil}
import org.apache.spark.sql.functions.{desc, hex, to_json, udf}
import org.apache.spark.sql.rapids.tool.{AppBase, AppFilterImpl, ClusterSummary, ExistingClusterInfo, ToolUtils}
import org.apache.spark.sql.rapids.tool.qualification.{QualificationSummaryInfo, RunningQualificationEventProcessor}
import org.apache.spark.sql.rapids.tool.util.{FSUtils, RapidsToolsConfUtil, UTF8Source}
import org.apache.spark.sql.types._

// drop the fields that won't go to DataFrame without encoders
case class TestQualificationSummary(
    appName: String,
    appId: String,
    estimatedGpuDur: Double,
    estimatedGpuTimeSaved: Double,
    sqlDataframeDuration: Long,
    sqlDataframeTaskDuration: Long,
    appDuration: Long,
    gpuOpportunity: Long,
    executorCpuTimePercent: Double,
    failedSQLIds: String,
    readFileFormatAndTypesNotSupported: String,
    writeDataFormat: String,
    complexTypes: String,
    nestedComplexTypes: String,
    potentialProblems: String,
    longestSqlDuration: Long,
    totalStageWallClockDuration: Long,
    nonSqlTaskDurationAndOverhead: Long,
    unsupportedSQLTaskDuration: Long,
    supportedSQLTaskDuration: Long,
    endDurationEstimated: Boolean,
    unsupportedExecs: String,
    unsupportedExprs: String,
    estimatedFrequency: Long,
    toalCoreSecs: Long)

class QualificationSuite extends BaseTestSuite {

  private val expRoot = ToolTestUtils.getTestResourceFile("QualificationExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")

  private val csvDetailedFields = Seq(
    (QualOutputWriter.APP_NAME_STR, StringType),
    (QualOutputWriter.APP_ID_STR, StringType),
    (QualOutputWriter.ESTIMATED_GPU_DURATION, DoubleType),
    (QualOutputWriter.ESTIMATED_GPU_TIMESAVED, DoubleType),
    (QualOutputWriter.SQL_DUR_STR, LongType),
    (QualOutputWriter.TASK_DUR_STR, LongType),
    (QualOutputWriter.APP_DUR_STR, LongType),
    (QualOutputWriter.GPU_OPPORTUNITY_STR, LongType),
    (QualOutputWriter.EXEC_CPU_PERCENT_STR, DoubleType),
    (QualOutputWriter.SQL_IDS_FAILURES_STR, StringType),
    (QualOutputWriter.READ_FILE_FORMAT_TYPES_STR, StringType),
    (QualOutputWriter.WRITE_DATA_FORMAT_STR, StringType),
    (QualOutputWriter.COMPLEX_TYPES_STR, StringType),
    (QualOutputWriter.NESTED_TYPES_STR, StringType),
    (QualOutputWriter.POT_PROBLEM_STR, StringType),
    (QualOutputWriter.LONGEST_SQL_DURATION_STR, LongType),
    (QualOutputWriter.SQL_STAGE_DUR_SUM_STR, LongType),
    (QualOutputWriter.NONSQL_DUR_STR, LongType),
    (QualOutputWriter.UNSUPPORTED_TASK_DURATION_STR, LongType),
    (QualOutputWriter.SUPPORTED_SQL_TASK_DURATION_STR, LongType),
    (QualOutputWriter.APP_DUR_ESTIMATED_STR, BooleanType),
    (QualOutputWriter.UNSUPPORTED_EXECS, StringType),
    (QualOutputWriter.UNSUPPORTED_EXPRS, StringType),
    (QualOutputWriter.ESTIMATED_FREQUENCY, LongType),
    (QualOutputWriter.TOTAL_CORE_SEC, LongType))

  private val csvPerSQLFields = Seq(
    (QualOutputWriter.APP_NAME_STR, StringType),
    (QualOutputWriter.APP_ID_STR, StringType),
    (QualOutputWriter.ROOT_SQL_ID_STR, StringType),
    (QualOutputWriter.SQL_ID_STR, StringType),
    (QualOutputWriter.SQL_DESC_STR, StringType),
    (QualOutputWriter.SQL_DUR_STR, LongType),
    (QualOutputWriter.GPU_OPPORTUNITY_STR, LongType),
    (QualOutputWriter.ESTIMATED_GPU_DURATION, DoubleType),
    (QualOutputWriter.ESTIMATED_GPU_TIMESAVED, DoubleType))

  val schema = new StructType(csvDetailedFields.map(f => StructField(f._1, f._2, true)).toArray)
  val perSQLSchema = new StructType(csvPerSQLFields.map(f => StructField(f._1, f._2, true)).toArray)

  def csvDetailedHeader(ind: Int) = csvDetailedFields(ind)._1

  def readExpectedFile(expected: File, escape: String = "\\"): DataFrame = {
    ToolTestUtils.readExpectationCSV(sparkSession, expected.getPath(), Some(schema), escape)
  }

  def readPerSqlFile(expected: File, escape: String = "\\"): DataFrame = {
    ToolTestUtils.readExpectationCSV(sparkSession, expected.getPath(), Some(perSQLSchema), escape)
  }

  def readPerSqlTextFile(expected: File): Dataset[String] = {
    sparkSession.read.textFile(expected.getPath())
  }

  private def createSummaryForDF(
      appSums: Seq[QualificationSummaryInfo]): Seq[TestQualificationSummary] = {
    appSums.map { appInfoRec =>
      val sum = QualOutputWriter.createFormattedQualSummaryInfo(appInfoRec, ",")
      TestQualificationSummary(sum.appName, sum.appId, sum.estimatedGpuDur,
        sum.estimatedGpuTimeSaved, sum.sqlDataframeDuration,
        sum.sqlDataframeTaskDuration, sum.appDuration,
        sum.gpuOpportunity, sum.executorCpuTimePercent, sum.failedSQLIds,
        sum.readFileFormatAndTypesNotSupported, sum.writeDataFormat,
        sum.complexTypes, sum.nestedComplexTypes, sum.potentialProblems, sum.longestSqlDuration,
        sum.sqlStageDurationsSum, sum.nonSqlTaskDurationAndOverhead,
        sum.unsupportedSQLTaskDuration, sum.supportedSQLTaskDuration,
        sum.endDurationEstimated, sum.unSupportedExecs, sum.unSupportedExprs,
        sum.estimatedFrequency, sum.totalCoreSec)
    }
  }

  private def runQualificationTest(eventLogs: Array[String], expectFileName: String = "",
      shouldReturnEmpty: Boolean = false, expectPerSqlFileName: Option[String] = None,
      expectedStatus: Option[StatusReportCounts] = None): Unit = {
    TrampolineUtil.withTempDir { outpath =>
      val qualOutputPrefix = "rapids_4_spark_qualification_output"
      val outputArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath())

      val allArgs = if (expectPerSqlFileName.isDefined) {
        outputArgs ++ Array("--per-sql")
      } else {
        outputArgs
      }

      val appArgs = new QualificationArgs(allArgs ++ eventLogs)
      val (exit, appSum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)
      val spark2 = sparkSession
      import spark2.implicits._
      val summaryDF = createSummaryForDF(appSum).toDF
      val dfQual = sparkSession.createDataFrame(summaryDF.rdd, schema)

      // Default expectation for the status counts - All applications are successful.
      val expectedStatusCounts =
        expectedStatus.getOrElse(StatusReportCounts(appSum.length, 0, 0, 0))
      // Compare the expected status counts with the actual status counts from the application
      ToolTestUtils.compareStatusReport(sparkSession, expectedStatusCounts,
        s"${outpath.getAbsolutePath}/$qualOutputPrefix/${qualOutputPrefix}_status.csv")

      if (shouldReturnEmpty) {
        assert(appSum.head.estimatedInfo.sqlDfDuration == 0.0)
      } else if (expectFileName.nonEmpty) {
        val resultExpectation = new File(expRoot, expectFileName)
        val dfExpect = readExpectedFile(resultExpectation)
        assert(!dfQual.isEmpty)
        ToolTestUtils.compareDataFrames(dfQual, dfExpect)
        if (expectPerSqlFileName.isDefined) {
          val resultExpectation = new File(expRoot, expectPerSqlFileName.get)
          val dfPerSqlExpect = readPerSqlFile(resultExpectation)
          val actualExpectation = s"$outpath/$qualOutputPrefix/${qualOutputPrefix}_persql.csv"
          val dfPerSqlActual = readPerSqlFile(new File(actualExpectation))
          ToolTestUtils.compareDataFrames(dfPerSqlActual, dfPerSqlExpect)
        }
      }
    }
  }

  test("RunningQualificationEventProcessor per sql") {
    TrampolineUtil.withTempDir { qualOutDir =>
      TrampolineUtil.withTempPath { outParquetFile =>
        TrampolineUtil.withTempPath { outJsonFile =>
          // note don't close the application here so we test running output
          ToolTestUtils.runAndCollect("running per sql") { spark =>
            val sparkConf = spark.sparkContext.getConf
            sparkConf.set("spark.rapids.qualification.output.numSQLQueriesPerFile", "2")
            sparkConf.set("spark.rapids.qualification.output.maxNumFiles", "3")
            sparkConf.set("spark.rapids.qualification.outputDir", qualOutDir.getPath)
            val listener = new RunningQualificationEventProcessor(sparkConf)
            spark.sparkContext.addSparkListener(listener)
            import spark.implicits._
            val testData = Seq((1, 2), (3, 4)).toDF("a", "b")
            testData.write.json(outJsonFile.getCanonicalPath)
            testData.write.parquet(outParquetFile.getCanonicalPath)
            val df = spark.read.parquet(outParquetFile.getCanonicalPath)
            val df2 = spark.read.json(outJsonFile.getCanonicalPath)
            // generate a bunch of SQL queries to test the file rolling, should run
            // 10 sql queries total with above and below
            for (i <- 1 to 7) {
              df.join(df2.select($"a" as "a2"), $"a" === $"a2").count()
            }
            val df3 = df.join(df2.select($"a" as "a2"), $"a" === $"a2")
            df3
          }
          // the code above that runs the Spark query stops the Sparksession
          // so create a new one to read in the csv file
          createSparkSession()
          val outputDir = qualOutDir.getPath + "/"
          val csvOutput0 = outputDir + QualOutputWriter.LOGFILE_NAME + "_persql_0.csv"
          val txtOutput0 = outputDir + QualOutputWriter.LOGFILE_NAME + "_persql_0.log"
          // check that there are 6 files since configured for 3 and have 1 csv and 1 log
          // file each
          val outputDirPath = new Path(outputDir)
          val fs = FileSystem.get(outputDirPath.toUri, RapidsToolsConfUtil.newHadoopConf())
          val allFiles = fs.listStatus(outputDirPath)
          assert(allFiles.size == 6)
          val dfPerSqlActual = readPerSqlFile(new File(csvOutput0))
          assert(dfPerSqlActual.columns.size == 9)
          val rows = dfPerSqlActual.collect()
          assert(rows.size == 2)
          val firstRow = rows(1)
          // , should be replaced with ;
          assert(firstRow(4).toString.contains("at QualificationSuite.scala"))

          // this reads everything into single column
          val dfPerSqlActualTxt = readPerSqlTextFile(new File(txtOutput0))
          assert(dfPerSqlActualTxt.columns.size == 1)
          val rowsTxt = dfPerSqlActualTxt.collect()
          // have to account for headers
          assert(rowsTxt.size == 6)
          val firstValueRow = rowsTxt(3).toString
          assert(firstValueRow.contains("QualificationSuite.scala"))
        }
      }
    }
  }

  test("test order asc") {
    val logFiles = Array(
      s"$logDir/dataset_eventlog",
      s"$logDir/dsAndDf_eventlog.zstd",
      s"$logDir/udf_dataset_eventlog",
      s"$logDir/udf_func_eventlog"
    )
    TrampolineUtil.withTempDir { outpath =>
      val qualOutputPrefix = "rapids_4_spark_qualification_output"
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath(),
        "--order",
        "asc")

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, appSum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)
      assert(appSum.size == 4)
      assert(appSum.head.appId.equals("local-1622043423018"))

      // Default expectation for the status counts - All applications are successful.
      val expectedStatusCount = StatusReportCounts(appSum.length, 0, 0, 0)
      // Compare the expected status counts with the actual status counts from the application
      ToolTestUtils.compareStatusReport(sparkSession, expectedStatusCount,
        s"${outpath.getAbsolutePath}/$qualOutputPrefix/${qualOutputPrefix}_status.csv")

      val filename = s"$outpath/$qualOutputPrefix/$qualOutputPrefix.log"
      val inputSource = UTF8Source.fromFile(filename)
      try {
        val lines = inputSource.getLines.toArray
        // 4 lines of header and footer
        assert(lines.size == (4 + 4))
        // skip the 3 header lines
        val firstRow = lines(3)
        assert(firstRow.contains("local-1623281204390"))
      } finally {
        inputSource.close()
      }
    }
  }

  test("test order desc") {
    val logFiles = Array(
      s"$logDir/dataset_eventlog",
      s"$logDir/dsAndDf_eventlog.zstd",
      s"$logDir/udf_dataset_eventlog",
      s"$logDir/udf_func_eventlog"
    )
    TrampolineUtil.withTempDir { outpath =>
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath(),
        "--order",
        "desc",
        "--per-sql")

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, appSum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)
      assert(appSum.size == 4)
      assert(appSum.head.appId.equals("local-1622043423018"))

      val filename = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output.log"
      val inputSource = UTF8Source.fromFile(filename)
      try {
        val lines = inputSource.getLines.toArray
        // 4 lines of header and footer
        assert(lines.size == (4 + 4))
        // skip the 3 header lines
        val firstRow = lines(3)
        assert(firstRow.contains("local-1622043423018"))
      } finally {
        inputSource.close()
      }
      val persqlFileName = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output_persql.log"
      val persqlInputSource = UTF8Source.fromFile(persqlFileName)
      try {
        val lines = persqlInputSource.getLines.toArray
        // 4 lines of header and footer
        assert(lines.size == (4 + 17))
        // skip the 3 header lines
        val firstRow = lines(3)
        // this should be app
        assert(firstRow.contains("local-1622043423018"))
        assert(firstRow.contains("count at QualificationInfoUtils.scala:94"))
      } finally {
        persqlInputSource.close()
      }
    }
  }

  test("test limit desc") {
    val logFiles = Array(
      s"$logDir/dataset_eventlog",
      s"$logDir/dsAndDf_eventlog.zstd",
      s"$logDir/udf_dataset_eventlog",
      s"$logDir/udf_func_eventlog"
    )
    TrampolineUtil.withTempDir { outpath =>
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath(),
        "--order",
        "desc",
        "-n",
        "2",
        "--per-sql")

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, _) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)

      val filename = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output.log"
      val inputSource = UTF8Source.fromFile(filename)
      try {
        val lines = inputSource.getLines
        // 4 lines of header and footer, limit is 2
        assert(lines.size == (4 + 2))
      } finally {
        inputSource.close()
      }
      val persqlFileName = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output_persql.log"
      val persqlInputSource = UTF8Source.fromFile(persqlFileName)
      try {
        val lines = persqlInputSource.getLines
        // 4 lines of header and footer, limit is 2
        assert(lines.size == (4 + 2))
      } finally {
        persqlInputSource.close()
      }
    }
  }

  test("test datasource read format included") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val logFiles = Array(s"$profileLogDir/eventlog_dsv1.zstd")
    TrampolineUtil.withTempDir { outpath =>
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath(),
        "--report-read-schema")

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, sum@_) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)

      val filename = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output.csv"
      val inputSource = UTF8Source.fromFile(filename)
      try {
        val lines = inputSource.getLines.toSeq
        // 1 for header, 1 for values
        assert(lines.size == 2)
        assert(lines.head.contains("Read Schema"))
        assert(lines(1).contains("loan399"))
      } finally {
        inputSource.close()
      }
    }
  }

  test("test skip gpu event logs") {
    val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
    val logFiles = Array(s"$qualLogDir/gpu_eventlog")
    TrampolineUtil.withTempDir { outpath =>
      val qualOutputPrefix = "rapids_4_spark_qualification_output"
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath())

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, appSum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)
      assert(appSum.size == 0)

      // Application should fail. Status counts: 0 SUCCESS, 0 FAILURE, 1 SKIPPED, 0 UNKNOWN
      val expectedStatusCounts = StatusReportCounts(0, 0, 1, 0)
      // Compare the expected status counts with the actual status counts from the application
      ToolTestUtils.compareStatusReport(sparkSession, expectedStatusCounts,
        s"${outpath.getAbsolutePath}/$qualOutputPrefix/${qualOutputPrefix}_status.csv")

      val filename = s"$outpath/$qualOutputPrefix/$qualOutputPrefix.csv"
      val inputSource = UTF8Source.fromFile(filename)
      try {
        val lines = inputSource.getLines.toSeq
        // 1 for header, Event log not parsed since it is from GPU run.
        assert(lines.size == 1)
      } finally {
        inputSource.close()
      }
    }
  }

  test("skip malformed json eventlog") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val badEventLog = s"$profileLogDir/malformed_json_eventlog.zstd"
    val logFiles = Array(s"$logDir/nds_q86_test", badEventLog)
    // Status counts: 1 SUCCESS, 0 FAILURE, 0 SKIPPED, 1 UNKNOWN
    val expectedStatus = Some(StatusReportCounts(1, 0, 0, 1))
    runQualificationTest(logFiles, "nds_q86_test_expectation.csv", expectedStatus = expectedStatus)
  }

  test("incomplete json file does not cause entire app to fail") {
    // The purpose of this test is to make sure that the app is not skipped when the JSON parser
    // encounters an unexpected EOF.
    // There are two cases to evaluate:
    // 1- An eventlog that has an end-to-end application but for some reason the EOF is incorrect
    // 2- An eventlog of an unfinished app (missing SparkListenerApplicationEnd)

    TrampolineUtil.withTempDir { eventLogDir =>
      // generate the original eventlog
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "WholeStageFilterProject") { spark =>
        import spark.implicits._
        val df = spark.sparkContext.makeRDD(1 to 100, 3).toDF
        val df2 = spark.sparkContext.makeRDD(1 to 100, 3).toDF
        df.select($"value" as "a")
          .join(df2.select($"value" as "b"), $"a" === $"b")
          .filter("(((b < 100) AND (a > 50)) OR (a = 0))")
          .sort($"b")
      }
      // create the following files:
      // 1- inprogress eventlog that does not contain "SparkListenerApplicationEnd" (unfinished)
      // 2- inprogress eventlog with a terminated app (incomplete)
      // 3- inprogress eventlog with broken line (half line)
      val unfinishedLog = new File(s"$eventLogDir/unfinished.inprogress")
      val incompleteLog = new File(s"$eventLogDir/eventlog.inprogress")
      val brokenEvLog = new File(s"$eventLogDir/brokenevent.inprogress")
      val pwList = Array(new PrintWriter(unfinishedLog), new PrintWriter(incompleteLog),
        new PrintWriter(brokenEvLog))
      val bufferedSource = UTF8Source.fromFile(eventLog)
      try {
        val allEventLines = bufferedSource.getLines.toList
        // the following val will contain the last two lines of the eventlog
        //59 = "{"Event":"SparkListenerTaskEnd",
        //60 = "{"Event":"SparkListenerStageCompleted"
        //61 = "{"Event":"SparkListenerJobEnd","Job ID":5,"Completion Time":1718401564645,"
        //62 = "{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd","
        //63 = "{"Event":"SparkListenerApplicationEnd","Timestamp":1718401564663}"
        val tailLines = allEventLines.takeRight(5)
        val selectedLines: List[String] = allEventLines.dropRight(5)
        selectedLines.foreach { line =>
          pwList.foreach(pw => pw.println(line))
        }
        for (i <- 0 to tailLines.length - 1) {
          if (i == 0) {
            // add truncatedTaskEvent to the brokenEventlog
            pwList(2).println(tailLines(i).substring(0, 59))
          }
          // Write all the lines to the unfinishedLog and incompleteLog.
          // We do not want to ApplicationEnd in the incompleteLog
          val startListInd = if (i == tailLines.length - 1) {
            1 // index of unfinished
          } else {
            0 // index of incomplete
          }
          for (lIndex <- startListInd to 1) {
            pwList(lIndex).println(tailLines(i))
          }
        }
        // For the first two eventlogs, add a random incomplete line
        pwList.dropRight(1).foreach(pw =>
          pw.print("{\"Event\":\"SparkListenerEnvironmentUpdate\"," +
            "\"JVM Information\":{\"Java Home:")
        )
      } finally {
        bufferedSource.close()
        pwList.foreach(pw => pw.close())
      }
      // All the eventlogs should be parsed successfully
      // Status counts: 3 SUCCESS, 0 FAILURE, 0 UNKNOWN
      val logFiles = Array(eventLog,
        incompleteLog.getAbsolutePath,
        unfinishedLog.getAbsolutePath,
        brokenEvLog.getAbsolutePath)
      // test Qualification
      val outpath = new File(s"$eventLogDir/output_folder")
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath())
      // test qualification one file at a time to avoid merging results as a single app
      for (logFile <- logFiles) {
        val appArgs = new QualificationArgs(allArgs ++ Array(logFile))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == 1)
      }
      // test Profiler
      val apps = ToolTestUtils.processProfileApps(logFiles, sparkSession)
      assert(apps.size == pwList.length + 1)
    }
  }

  test("spark2 eventlog") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val log = s"$profileLogDir/spark2-eventlog.zstd"
    runQualificationTest(Array(log), "spark2_expectation.csv")
  }

  test("test appName filter") {
    val appName = "Spark shell"
    val appArgs = new QualificationArgs(Array(
      "--application-name",
      appName,
      s"$logDir/rdd_only_eventlog",
      s"$logDir/empty_eventlog",
      s"$logDir/udf_func_eventlog"
    ))

    val (eventLogInfo, _) = EventLogPathProcessor.processAllPaths(
      appArgs.filterCriteria.toOption, appArgs.matchEventLogs.toOption, appArgs.eventlog(),
      RapidsToolsConfUtil.newHadoopConf())

    val appFilter = new AppFilterImpl(1000, RapidsToolsConfUtil.newHadoopConf(),
      Some(84000), 2)
    val result = appFilter.filterEventLogs(eventLogInfo, appArgs)
    assert(eventLogInfo.length == 3)
    assert(result.length == 2) // 2 out of 3 have "Spark shell" as appName.
  }

  test("test appName filter - Negation") {
    val appName = "~Spark shell"
    val appArgs = new QualificationArgs(Array(
      "--application-name",
      appName,
      s"$logDir/rdd_only_eventlog",
      s"$logDir/empty_eventlog",
      s"$logDir/udf_func_eventlog"
    ))

    val (eventLogInfo, _) = EventLogPathProcessor.processAllPaths(
      appArgs.filterCriteria.toOption, appArgs.matchEventLogs.toOption, appArgs.eventlog(),
      RapidsToolsConfUtil.newHadoopConf())

    val appFilter = new AppFilterImpl(1000, RapidsToolsConfUtil.newHadoopConf(),
      Some(84000), 2)
    val result = appFilter.filterEventLogs(eventLogInfo, appArgs)
    assert(eventLogInfo.length == 3)
    assert(result.length == 1) // 1 out of 3 does not has "Spark shell" as appName.
  }

  test("test udf event logs") {
    val logFiles = Array(
      s"$logDir/dataset_eventlog",
      s"$logDir/dsAndDf_eventlog.zstd",
      s"$logDir/udf_dataset_eventlog",
      s"$logDir/udf_func_eventlog"
    )
    runQualificationTest(logFiles, "qual_test_simple_expectation.csv",
      expectPerSqlFileName = Some("qual_test_simple_expectation_persql.csv"))
  }

  test("test missing sql end") {
    val logFiles = Array(s"$logDir/join_missing_sql_end")
    runQualificationTest(logFiles, "qual_test_missing_sql_end_expectation.csv")
  }

  test("test eventlog with no jobs") {
    val logFiles = Array(s"$logDir/empty_eventlog")
    runQualificationTest(logFiles, shouldReturnEmpty=true)
  }

  test("test eventlog with rdd only jobs") {
    val logFiles = Array(s"$logDir/rdd_only_eventlog")
    runQualificationTest(logFiles, shouldReturnEmpty=true)
  }

  test("test truncated log file 1") {
    val logFiles = Array(s"$logDir/truncated_eventlog")
    runQualificationTest(logFiles, "truncated_1_end_expectation.csv")
  }

  test("test nds q86 test") {
    val logFiles = Array(s"$logDir/nds_q86_test")
    runQualificationTest(logFiles, "nds_q86_test_expectation.csv",
      expectPerSqlFileName = Some("nds_q86_test_expectation_persql.csv"))
  }

  // event log rolling creates files under a directory
  test("test event log rolling") {
    val logFiles = Array(s"$logDir/eventlog_v2_local-1623876083964")
    runQualificationTest(logFiles, "directory_test_expectation.csv")
  }

  // these test files were setup to simulator the directory structure
  // when running on Databricks and are not really from there
  test("test db event log rolling") {
    val logFiles = Array(s"$logDir/db_sim_eventlog")
    runQualificationTest(logFiles, "db_sim_test_expectation.csv")
  }

  runConditionalTest("test nds q86 with failure test",
    shouldSkipFailedLogsForSpark) {
    val logFiles = Array(s"$logDir/nds_q86_fail_test")
    runQualificationTest(logFiles, "nds_q86_fail_test_expectation.csv",
      expectPerSqlFileName = Some("nds_q86_fail_test_expectation_persql.csv"))
  }

  test("test event log write format") {
    val logFiles = Array(s"$logDir/writeformat_eventlog")
    runQualificationTest(logFiles, "write_format_expectation.csv")
  }

  test("test event log nested types in ReadSchema") {
    val logFiles = Array(s"$logDir/nested_type_eventlog")
    runQualificationTest(logFiles, "nested_type_expectation.csv")
  }

  // this tests parseReadSchema by passing different schemas as strings. Schemas
  // with complex types, complex nested types, decimals and simple types
  test("test different types in ReadSchema") {
    val testSchemas: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer(
      ArrayBuffer(""),
      ArrayBuffer("firstName:string,lastName:string", "", "address:string"),
      ArrayBuffer("properties:map<string,string>"),
      ArrayBuffer("name:array<string>"),
      ArrayBuffer("name:string,booksInterested:array<struct<name:string,price:decimal(8,2)," +
          "author:string,pages:int>>,authbook:array<map<name:string,author:string>>, " +
          "pages:array<array<struct<name:string,pages:int>>>,name:string,subject:string"),
      ArrayBuffer("name:struct<fn:string,mn:array<string>,ln:string>," +
          "add:struct<cur:struct<st:string,city:string>," +
          "previous:struct<st:map<string,string>,city:string>>," +
          "next:struct<fn:string,ln:string>"),
      ArrayBuffer("name:map<id:int,map<fn:string,ln:string>>, " +
          "address:map<id:int,struct<st:string,city:string>>," +
          "orders:map<id:int,order:array<map<oname:string,oid:int>>>," +
          "status:map<name:string,active:string>")
    )

    var index = 0
    val expectedResult = List(
      ("", ""),
      ("", ""),
      ("map<string,string>", ""),
      ("array<string>", ""),
      ("array<struct<name:string,price:decimal(8,2),author:string,pages:int>>;" +
          "array<map<name:string,author:string>>;array<array<struct<name:string,pages:int>>>",
          "array<struct<name:string,price:decimal(8,2),author:string,pages:int>>;" +
              "array<map<name:string,author:string>>;array<array<struct<name:string,pages:int>>>"),
      ("struct<fn:string,mn:array<string>,ln:string>;" +
          "struct<cur:struct<st:string,city:string>,previous:struct<st:map<string,string>," +
          "city:string>>;struct<fn:string,ln:string>",
          "struct<fn:string,mn:array<string>,ln:string>;" +
              "struct<cur:struct<st:string,city:string>,previous:struct<st:map<string,string>," +
              "city:string>>"),
      ("map<id:int,map<fn:string,ln:string>>;map<id:int,struct<st:string,city:string>>;" +
          "map<id:int,order:array<map<oname:string,oid:int>>>;map<name:string,active:string>",
          "map<id:int,map<fn:string,ln:string>>;map<id:int,struct<st:string,city:string>>;" +
              "map<id:int,order:array<map<oname:string,oid:int>>>"))

    val result = testSchemas.map(x => AppBase.parseReadSchemaForNestedTypes(x))
    result.foreach { actualResult =>
      assert(ToolUtils.formatComplexTypes(actualResult._1).equals(expectedResult(index)._1))
      assert(ToolUtils.formatComplexTypes(actualResult._2).equals(expectedResult(index)._2))
      index += 1
    }
  }

  test("test jdbc problematic") {
    val logFiles = Array(s"$logDir/jdbc_eventlog.zstd")
    runQualificationTest(logFiles, "jdbc_expectation.csv")
  }

  private def createDecFile(spark: SparkSession, dir: String): Unit = {
    import spark.implicits._
    val dfGen = Seq("1.32").toDF("value")
      .selectExpr("CAST(value AS DECIMAL(4, 2)) AS value")
    dfGen.write.parquet(dir)
  }

  private def createIntFile(spark:SparkSession, dir:String): Unit = {
    import spark.implicits._
    val t1 = Seq((1, 2), (3, 4), (1, 6)).toDF("a", "b")
    t1.write.parquet(dir)
  }

  test("test generate udf same") {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val tmpParquet = s"$outpath/decparquet"
        createDecFile(sparkSession, tmpParquet)

        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "dot") { spark =>
          val plusOne = udf((x: Int) => x + 1)
          import spark.implicits._
          spark.udf.register("plusOne", plusOne)
          val df = spark.read.parquet(tmpParquet)
          val df2 = df.withColumn("mult", $"value" * $"value")
          val df4 = df2.withColumn("udfcol", plusOne($"value"))
          df4
        }

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath())
        val appArgs = new QualificationArgs(allArgs ++ Array(eventLog))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == 1)
        val probApp = appSum.head
        assert(probApp.potentialProblems.contains("UDF"))
        assert(probApp.unsupportedSQLTaskDuration > 0) // only UDF is unsupported in the query.
      }
    }
  }

  test("test sparkML") {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val tmpParquet = s"$outpath/mlOpsParquet"
        createDecFile(sparkSession, tmpParquet)
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "dot") { spark =>
          val data = Array(
            Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
            Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
            Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
          )
          val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
          new PCA()
            .setInputCol("features")
            .setOutputCol("pcaFeatures")
            .setK(3)
            .fit(df)
          df
        }

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath(),
          "--ml-functions",
          "true")
        val appArgs = new QualificationArgs(allArgs ++ Array(eventLog))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == 1)
        val mlOpsRes = appSum.head
        assert(mlOpsRes.mlFunctions.nonEmpty)
        // Spark3.2.+ generates a plan with 6 stages. StageID 3 and 4 are both
        // "isEmpty at RowMatrix.scala:441"
        val expStageCount = if (ToolUtils.isSpark320OrLater()) 6 else 5
        assert(mlOpsRes.mlFunctions.get.map(x=> x.stageId).size == expStageCount)
        assert(mlOpsRes.mlFunctions.get.head.mlOps.mkString.contains(
          "org.apache.spark.ml.feature.PCA.fit"))
        assert(mlOpsRes.mlFunctionsStageDurations.get.head.mlFuncName.equals("PCA"))
        // estimated GPU time is for ML function, there are no Spark Dataframe/SQL functions.
        assert(mlOpsRes.estimatedInfo.estimatedGpuTimeSaved > 0)
      }
    }
  }

  test("test xgboost") {
    val logFiles = Array(
      s"$logDir/xgboost_eventlog.zstd"
    )
    TrampolineUtil.withTempDir { outpath =>
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath(),
        "--ml-functions",
        "true")

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, appSum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)
      assert(appSum.size == 1)
      val xgBoostRes = appSum.head
      assert(xgBoostRes.mlFunctions.nonEmpty)
      assert(xgBoostRes.mlFunctionsStageDurations.nonEmpty)
      assert(xgBoostRes.mlFunctions.get.head.mlOps.mkString.contains(
        "ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier.train"))
      assert(xgBoostRes.mlFunctionsStageDurations.get.head.mlFuncName.equals("XGBoost"))
      assert(xgBoostRes.mlFunctionsStageDurations.get.head.duration == 46444)
    }
  }


  test("test with stage reuse") {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "dot") { spark =>
          import spark.implicits._
          val df = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
          val df2 = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
          val j1 = df.select( $"value" as "a")
            .join(df2.select($"value" as "b"), $"a" === $"b").cache()
          j1.count()
          j1.union(j1).count()
          // count above is important thing, here we just make up small df to return
          spark.sparkContext.makeRDD(1 to 2).toDF
        }

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath())
        val appArgs = new QualificationArgs(allArgs ++ Array(eventLog))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == 1)
        // note this would have failed an assert with total task time to small if we
        // didn't dedup stages
      }
    }
  }

  runConditionalTest("test generate udf different sql ops",
    checkUDFDetectionSupportForSpark) {
    TrampolineUtil.withTempDir { outpath =>

      TrampolineUtil.withTempDir { eventLogDir =>
        val tmpParquet = s"$outpath/decparquet"
        val grpParquet = s"$outpath/grpParquet"
        createDecFile(sparkSession, tmpParquet)
        createIntFile(sparkSession, grpParquet)
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "dot") { spark =>
          val plusOne = udf((x: Int) => x + 1)
          import spark.implicits._
          spark.udf.register("plusOne", plusOne)
          val df = spark.read.parquet(tmpParquet)
          val df2 = df.withColumn("mult", $"value" * $"value")
          // first run sql op with decimal only
          df2.collect()
          // run a separate sql op using just udf
          spark.sql("SELECT plusOne(5)").collect()
          // Then run another sql op that doesn't use with decimal or udf
          val t2 = spark.read.parquet(grpParquet)
          val res = t2.groupBy("a").max("b").orderBy(desc("a"))
          res
        }

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath())
        val appArgs = new QualificationArgs(allArgs ++ Array(eventLog))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == 1)
        val probApp = appSum.head
        assert(probApp.potentialProblems.contains("UDF"))
        assert(probApp.unsupportedSQLTaskDuration > 0) // only UDF is unsupported in the query.
      }
    }
  }

  test("test clusterTags when redacted") {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val tagConfs =
          Map(DatabricksParseHelper.PROP_ALL_TAGS_KEY -> "*********(redacted)",
            DatabricksParseHelper.PROP_TAG_CLUSTER_ID_KEY -> "0617-131246-dray530",
            DatabricksParseHelper.PROP_TAG_CLUSTER_NAME_KEY -> "job-215-run-34243234")
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "clustertagsRedacted",
          Some(tagConfs)) {
          spark =>
            import spark.implicits._

            val df1 = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
            df1.sample(0.1)
        }
        val expectedClusterId = "0617-131246-dray530"
        val expectedJobId = "215"

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath())
        val appArgs = new QualificationArgs(allArgs ++ Array(eventLog))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == 1)
        val allTags = appSum.flatMap(_.allClusterTagsMap).toMap
        assert(allTags("ClusterId") == expectedClusterId)
        assert(allTags("JobId") == expectedJobId)
        assert(allTags.get("RunName") == None)
      }
    }
  }

  test("test clusterTags configs") {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { eventLogDir =>

        val allTagsConfVal =
          """[{"key":"Vendor",
            |"value":"Databricks"},{"key":"Creator","value":"abc@company.com"},
            |{"key":"ClusterName","value":"job-215-run-1"},{"key":"ClusterId",
            |"value":"0617-131246-dray530"},{"key":"JobId","value":"215"},
            |{"key":"RunName","value":"test73longer"},{"key":"DatabricksEnvironment",
            |"value":"workerenv-7026851462233806"}]""".stripMargin
        val tagConfs =
          Map(DatabricksParseHelper.PROP_ALL_TAGS_KEY -> allTagsConfVal)
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "clustertags",
          Some(tagConfs)) { spark =>
          import spark.implicits._

          val df1 = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
          df1.sample(0.1)
        }
        val expectedClusterId = "0617-131246-dray530"
        val expectedJobId = "215"
        val expectedRunName = "test73longer"

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath())
        val appArgs = new QualificationArgs(allArgs ++ Array(eventLog))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == 1)
        val allTags = appSum.flatMap(_.allClusterTagsMap).toMap
        assert(allTags("ClusterId") == expectedClusterId)
        assert(allTags("JobId") == expectedJobId)
        assert(allTags("RunName") == expectedRunName)
      }
    }
  }

  test("test read datasource v1") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val logFiles = Array(s"$profileLogDir/eventlog_dsv1.zstd")
    runQualificationTest(logFiles, "read_dsv1_expectation.csv")
  }

  test("test read datasource v2") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val logFiles = Array(s"$profileLogDir/eventlog_dsv2.zstd")
    runQualificationTest(logFiles, "read_dsv2_expectation.csv")
  }

  test("test dsv1 complex") {
    val logFiles = Array(s"$logDir/complex_dec_eventlog.zstd")
    runQualificationTest(logFiles, "complex_dec_expectation.csv")
  }

  test("test dsv2 nested complex") {
    val logFiles = Array(s"$logDir/eventlog_nested_dsv2")
    runQualificationTest(logFiles, "nested_dsv2_expectation.csv")
  }

  test("sql metric agg") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val listener = new ToolTestListener
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        spark.sparkContext.addSparkListener(listener)
        import spark.implicits._
        val testData = Seq((1, 2), (3, 4)).toDF("a", "b")
        spark.sparkContext.setJobDescription("testing, csv delimiter, replacement")
        testData.createOrReplaceTempView("t1")
        testData.createOrReplaceTempView("t2")
        spark.sql("SELECT a, MAX(b) FROM (SELECT t1.a, t2.b " +
          "FROM t1 JOIN t2 ON t1.a = t2.a) AS t " +
          "GROUP BY a ORDER BY a")
      }
      assert(listener.completedStages.length == 5)

      // run the qualification tool
      TrampolineUtil.withTempDir { outpath =>
        val appArgs = new QualificationArgs(Array(
          "--per-sql",
          "--output-directory",
          outpath.getAbsolutePath,
          eventLog))

        val (exit, sumInfo) =
          QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        // the code above that runs the Spark query stops the Sparksession
        // so create a new one to read in the csv file
        createSparkSession()

        // validate that the SQL description in the csv file escapes commas properly
        val persqlResults = s"$outpath/rapids_4_spark_qualification_output/" +
          s"rapids_4_spark_qualification_output_persql.csv"
        val dfPerSqlActual = readPerSqlFile(new File(persqlResults))
        // the number of columns actually won't be wrong if sql description is malformatted
        // because spark seems to drop extra column so need more checking
        assert(dfPerSqlActual.columns.size == 9)
        val rows = dfPerSqlActual.collect()
        assert(rows.size == 3)
        val firstRow = rows(1)
        // , should not be replaced with ; or any other delim
        assert(firstRow(4) == "testing, csv delimiter, replacement")

        // parse results from listener
        val executorCpuTime = listener.executorCpuTime
        val executorRunTime = listener.completedStages
          .map(_.stageInfo.taskMetrics.executorRunTime).sum

        val listenerCpuTimePercent =
          ToolUtils.calculateDurationPercent(executorCpuTime, executorRunTime)

        // compare metrics from event log with metrics from listener
        assert(sumInfo.head.executorCpuTimePercent === listenerCpuTimePercent)
      }
    }
  }

  test("running qualification print unsupported Execs and Exprs") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val qualApp = new RunningQualificationApp()
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "streaming") { spark =>
        val listener = qualApp.getEventListener
        spark.sparkContext.addSparkListener(listener)
        import spark.implicits._
        val df1 = spark.sparkContext.parallelize(List(10, 20, 30, 40)).toDF
        df1.filter(hex($"value") === "A") // hex is not supported in GPU yet.
      }
      //stdout output tests
      val sumOut = qualApp.getSummary()
      val detailedOut = qualApp.getDetailed()
      assert(sumOut.nonEmpty)
      assert(sumOut.startsWith("|") && sumOut.endsWith("|\n"))
      assert(detailedOut.nonEmpty)
      assert(detailedOut.startsWith("|") && detailedOut.endsWith("|\n"))
      val stdOut = sumOut.split("\n")
      val stdOutValues = stdOut(1).split("\\|")
      // index of unsupportedExecs
      val stdOutunsupportedExecs = stdOutValues(stdOutValues.length - 3)
      // index of unsupportedExprs
      val stdOutunsupportedExprs = stdOutValues(stdOutValues.length - 2)
      val expectedstdOutExecs = "Scan;Filter;SerializeF..."
      assert(stdOutunsupportedExecs == expectedstdOutExecs)
      // Exec value is Scan;Filter;SerializeFromObject and UNSUPPORTED_EXECS_MAX_SIZE is 25
      val expectedStdOutExecsMaxLength = 25
      // Expr value is hex and length of expr header is 23 (Unsupported Expressions)
      val expectedStdOutExprsMaxLength = 23
      assert(stdOutunsupportedExecs.size == expectedStdOutExecsMaxLength)
      assert(stdOutunsupportedExprs.size == expectedStdOutExprsMaxLength)

      // run the qualification tool
      TrampolineUtil.withTempDir { outpath =>
        val appArgs = new QualificationArgs(Array(
          "--output-directory",
          outpath.getAbsolutePath,
          eventLog))

        val (exit, sumInfo@_) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)

        // the code above that runs the Spark query stops the Sparksession
        // so create a new one to read in the csv file
        createSparkSession()

        //csv output tests
        val outputResults = s"$outpath/rapids_4_spark_qualification_output/" +
          s"rapids_4_spark_qualification_output.csv"
        val outputActual = readExpectedFile(new File(outputResults), "\"")
        val rows = outputActual.collect()
        assert(rows.size == 1)

        val expectedExecs = "Scan;Filter;SerializeFromObject" // Unsupported Execs
        val expectedExprs = "hex" //Unsupported Exprs
        val unsupportedExecs =
          outputActual.select(QualOutputWriter.UNSUPPORTED_EXECS).first.getString(0)
        val unsupportedExprs =
          outputActual.select(QualOutputWriter.UNSUPPORTED_EXPRS).first.getString(0)
        assert(expectedExecs == unsupportedExecs)
        assert(expectedExprs == unsupportedExprs)
      }
    }
  }

  test("running qualification app join") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val qualApp = new RunningQualificationApp()
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "streaming") { spark =>
        val listener = qualApp.getEventListener
        spark.sparkContext.addSparkListener(listener)
        import spark.implicits._
        val testData = Seq((1, 2), (3, 4)).toDF("a", "b")
        testData.createOrReplaceTempView("t1")
        testData.createOrReplaceTempView("t2")
        spark.sql("SELECT a, MAX(b) FROM (SELECT t1.a, t2.b " +
          "FROM t1 JOIN t2 ON t1.a = t2.a) AS t " +
          "GROUP BY a ORDER BY a")
      }

      val sumOut = qualApp.getSummary()
      val detailedOut = qualApp.getDetailed()
      assert(sumOut.nonEmpty)
      assert(sumOut.startsWith("|") && sumOut.endsWith("|\n"))
      assert(detailedOut.nonEmpty)
      assert(detailedOut.startsWith("|") && detailedOut.endsWith("|\n"))

      // run the qualification tool
      TrampolineUtil.withTempDir { outpath =>
        val appArgs = new QualificationArgs(Array(
          "--output-directory",
          outpath.getAbsolutePath,
          eventLog))

        val (exit, sumInfo@_) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)

        // the code above that runs the Spark query stops the Sparksession
        // so create a new one to read in the csv file
        createSparkSession()

        // validate that the SQL description in the csv file escapes commas properly
        val outputResults = s"$outpath/rapids_4_spark_qualification_output/" +
          s"rapids_4_spark_qualification_output.csv"
        val outputActual = readExpectedFile(new File(outputResults), "\"")
        val rowsDetailed = outputActual.collect()
        assert(rowsDetailed.size == 1)
        val headersDetailed = outputActual.columns
        val valuesDetailed = rowsDetailed(0)
        assert(headersDetailed.size == QualOutputWriter
          .getDetailedHeaderStringsAndSizes(Seq(qualApp.aggregateStats.get), false).keys.size)
        assert(headersDetailed.size == csvDetailedFields.size)
        assert(valuesDetailed.size == csvDetailedFields.size)
        // check all headers exists
        for (ind <- 0 until csvDetailedFields.size) {
          assert(csvDetailedHeader(ind).equals(headersDetailed(ind)))
        }

        // check numeric fields skipping "Estimated Speed-up" on purpose
        val appDur = outputActual.select(QualOutputWriter.APP_DUR_STR).first.getLong(0)
        for (ind <- 4 until csvDetailedFields.size) {
          val (header, dt) = csvDetailedFields(ind)
          val fetched: Option[Double] = dt match {
            case DoubleType => Some(outputActual.select(header).first.getDouble(0))
            case LongType => Some(outputActual.select(header).first.getLong(0).doubleValue)
            case _ => None
          }
          if (fetched.isDefined) {
            val numValue = fetched.get
            if (header == "Unsupported Task Duration") {
              // unsupported task duration can be 0
              assert(numValue >= 0)
            } else if (header == "Executor CPU Time Percent") {
              // cpu percentage 0-100
              assert(numValue >= 0.0 && numValue <= 100.0)
            } else if (header == QualOutputWriter.GPU_OPPORTUNITY_STR ||
                        header == QualOutputWriter.SQL_DUR_STR) {
              // "SQL DF Duration" and "GPU Opportunity" cannot be larger than App Duration
              assert(numValue >= 0 && numValue <= appDur)
            } else {
              assert(numValue > 0)
            }
          }
        }
      }
    }
  }

  test("custom reasons for operators disabled by default") {
    TrampolineUtil.withTempDir { outParquetFile =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "unsup") { spark =>
          import spark.implicits._
          val data = Seq((1, ("Person1", 30)), (2, ("Person2", 25))).toDF("id", "person")
          data.write.parquet(s"$outParquetFile/person_info")
          val df = spark.read.parquet(s"$outParquetFile/person_info")
          df.withColumn("person_json", to_json($"person"))
        }

        TrampolineUtil.withTempDir { outpath =>
          val appArgs = new QualificationArgs(Array(
            "--output-directory",
            outpath.getAbsolutePath,
            eventLog))

          val (exit, _) = QualificationMain.mainInternal(appArgs)
          assert(exit == 0)
          createSparkSession()
          val filename = s"$outpath/rapids_4_spark_qualification_output/" +
              s"rapids_4_spark_qualification_output_unsupportedOperators.csv"

          val inputSource = UTF8Source.fromFile(filename)
          try {
            val lines = inputSource.getLines.toArray
            val expr = ".*to_json.*"
            val matches = lines.filter(_.matches(expr))
            assert(matches.length == 1)
            //get line number containing to_json
            val lineNum = lines.indexOf(matches(0))
            // check if lineNum has the expected value "This is disabled by default"
            assert(lines(lineNum).contains("This is disabled by default"))
          } finally {
            inputSource.close()
          }
        }
      }
    }
  }

  test("running qualification app files with per sql") {
    TrampolineUtil.withTempPath { outParquetFile =>
      TrampolineUtil.withTempPath { outJsonFile =>

        val qualApp = new RunningQualificationApp()
        ToolTestUtils.runAndCollect("streaming") { spark =>
          val listener = qualApp.getEventListener
          spark.sparkContext.addSparkListener(listener)
          import spark.implicits._
          val testData = Seq((1, 2), (3, 4)).toDF("a", "b")
          testData.write.json(outJsonFile.getCanonicalPath)
          testData.write.parquet(outParquetFile.getCanonicalPath)
          val df = spark.read.parquet(outParquetFile.getCanonicalPath)
          val df2 = spark.read.json(outJsonFile.getCanonicalPath)
          df.join(df2.select($"a" as "a2"), $"a" === $"a2")
        }
        // just basic testing that line exists and has right separator
        val csvHeader = qualApp.getPerSqlCSVHeader
        assert(csvHeader.contains("App Name,App ID,Root SQL ID,SQL ID,SQL Description," +
            "SQL DF Duration,GPU Opportunity,Estimated GPU Duration," +
            "Estimated GPU Time Saved"))
        val txtHeader = qualApp.getPerSqlTextHeader
        assert(txtHeader.contains("|                              App Name|             App ID|" +
            "Root SQL ID|SQL ID|                                                              " +
            "                       SQL Description|SQL DF Duration|GPU Opportunity|" +
            "Estimated GPU Duration|Estimated GPU Time Saved|"))
        val randHeader = qualApp.getPerSqlHeader(";", true, 20)
        assert(randHeader.contains(";                              App Name;             App ID;" +
            "Root SQL ID;SQL ID;     SQL Description;SQL DF Duration;GPU Opportunity;" +
            "Estimated GPU Duration;Estimated GPU Time Saved"))
        val allSQLIds = qualApp.getAvailableSqlIDs
        val numSQLIds = allSQLIds.size
        assert(numSQLIds > 0)
        val sqlIdToLookup = allSQLIds.head
        val (csvOut, txtOut) = qualApp.getPerSqlTextAndCSVSummary(sqlIdToLookup)
        assert(csvOut.contains("Profiling Tool Unit Tests") && csvOut.contains(","),
          s"CSV output was: $csvOut")
        assert(txtOut.contains("Profiling Tool Unit Tests") && txtOut.contains("|"),
          s"TXT output was: $txtOut")
        val sqlOut = qualApp.getPerSQLSummary(sqlIdToLookup, ":", true, 5)
        assert(sqlOut.contains("Tool Unit Tests:"), s"SQL output was: $sqlOut")

        // test different delimiter
        val sumOut = qualApp.getSummary(":", false)
        val rowsSumOut = sumOut.split("\n")
        assert(rowsSumOut.size == 2)
        val headers = rowsSumOut(0).split(":")
        val values = rowsSumOut(1).split(":")
        val appInfo = qualApp.aggregateStats()
        assert(appInfo.nonEmpty)
        assert(headers.size ==
          QualOutputWriter.getSummaryHeaderStringsAndSizes(30, 30).keys.size)
        assert(values.size == headers.size)
        // 3 should be the SQL DF Duration
        assert(headers(3).contains("SQL DF"))
        assert(values(3).toInt > 0)
        val detailedOut = qualApp.getDetailed(":", prettyPrint = false, reportReadSchema = true)
        val rowsDetailedOut = detailedOut.split("\n")
        assert(rowsDetailedOut.size == 2)
        val headersDetailed = rowsDetailedOut(0).split(":")
        val valuesDetailed = rowsDetailedOut(1).split(":")
        // Check Read Schema contains json and parquet
        val readSchemaIndex = headersDetailed.length - 1
        assert(headersDetailed(readSchemaIndex).contains("Read Schema"))
        assert(valuesDetailed(readSchemaIndex).contains("json") &&
            valuesDetailed(readSchemaIndex).contains("parquet"))
        qualApp.cleanupSQL(sqlIdToLookup)
        assert(qualApp.getAvailableSqlIDs.size == numSQLIds - 1)
      }
    }
  }

  test("test potential problems timestamp") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "timezone") { spark =>
        import spark.implicits._
        val testData = Seq((1, 1662519019), (2, 1662519020)).toDF("id", "timestamp")
        spark.sparkContext.setJobDescription("timestamp functions as potential problems")
        testData.createOrReplaceTempView("t1")
        spark.sql("SELECT id, hour(current_timestamp()), second(to_timestamp(timestamp)) FROM t1")
      }

      // run the qualification tool
      TrampolineUtil.withTempDir { outpath =>
        val appArgs = new QualificationArgs(Array(
          "--output-directory",
          outpath.getAbsolutePath,
          eventLog))

        val (exit, sumInfo@_) =
          QualificationMain.mainInternal(appArgs)
        assert(exit == 0)

        // the code above that runs the Spark query stops the Sparksession
        // so create a new one to read in the csv file
        createSparkSession()

        // validate that the SQL description in the csv file escapes commas properly
        val outputResults = s"$outpath/rapids_4_spark_qualification_output/" +
          s"rapids_4_spark_qualification_output.csv"
        val outputActual = readExpectedFile(new File(outputResults))
        assert(outputActual.collect().size == 1)
        assert(outputActual.select("Potential Problems").first.getString(0) ==
          "TIMEZONE hour():TIMEZONE current_timestamp():TIMEZONE to_timestamp():TIMEZONE second()")
      }
    }
  }

  test("test SMJ corner case not supported on left outer join") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "smjLeftJoin") { spark =>
        import spark.implicits._
        val data = Seq(("A", 20, "M", "2024-01-01"),
          ("B", 25, "M", "2024-12-12"), ("C", 30, "F", "2022-03-04"))
            .toDF("name", "id", "gender", "dob_str")
        data.createOrReplaceTempView("tableA")
        spark.conf.set("spark.rapids.sql.hasExtendedYearValues", "false")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1")
        spark.sql("SELECT COUNT(*) FROM tableA a LEFT JOIN tableA b ON " +
            "lower(a.id) in ('1','2') AND a.name = b.name")
      }
      val reader = FSUtils.readFileContentAsUTF8(eventLog)
      assert(reader.contains("SortMergeJoin"))

      // run the qualification tool
      TrampolineUtil.withTempDir { outpath =>
        val appArgs = new QualificationArgs(Array(
          "--output-directory",
          outpath.getAbsolutePath,
          eventLog))

        val (exit, _) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)

        // the code above that runs the Spark query stops the Sparksession
        // so create a new one to read in the csv file
        createSparkSession()

        val unsupportedOpsCSV = s"$outpath/rapids_4_spark_qualification_output/" +
            s"rapids_4_spark_qualification_output_unsupportedOperators.csv"
        val inputSource = UTF8Source.fromFile(unsupportedOpsCSV)
        try {
          val unsupportedRows = inputSource.getLines.toSeq
          // verify that SortMergeJoin is in unsupported operators
          // if there is lower and IN operator in the left join condition
          assert(unsupportedRows.exists(_.contains("SortMergeJoin")))
        } finally {
          inputSource.close()
        }
      }
    }
  }

  test("test existence join as supported join type") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "existenceJoin") { spark =>
        import spark.implicits._
        val df1 = Seq(("A", 20, 90), ("B", 25, 91), ("C", 30, 94)).toDF("name", "age", "score")
        val df2 = Seq(("A", 15, 90), ("B", 25, 92), ("C", 30, 94)).toDF("name", "age", "score")
        df1.createOrReplaceTempView("tableA")
        df2.createOrReplaceTempView("tableB")
        spark.sql("SELECT * from tableA as l where l.age > 24 or exists" +
          " (SELECT  * from tableB as r where l.age=r.age and l.score <= r.score)")
      }
      // validate that the eventlog contains ExistenceJoin and BroadcastHashJoin
      val reader = FSUtils.readFileContentAsUTF8(eventLog)
      assert(reader.contains("ExistenceJoin"))
      assert(reader.contains("BroadcastHashJoin"))

      // run the qualification tool
      TrampolineUtil.withTempDir { outpath =>
        val appArgs = new QualificationArgs(Array(
          "--output-directory",
          outpath.getAbsolutePath,
          eventLog))

        val (exit, _) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)

        // the code above that runs the Spark query stops the Sparksession
        // so create a new one to read in the csv file
        createSparkSession()

        // validate that the SQL description in the csv file escapes commas properly
        val outputResults = s"$outpath/rapids_4_spark_qualification_output/" +
          s"rapids_4_spark_qualification_output.csv"
        // validate that ExistenceJoin is supported since BroadcastHashJoin is not in unsupported
        // Execs list
        val outputActual = readExpectedFile(new File(outputResults), "\"")
        val unsupportedExecs = {
          outputActual.select(QualOutputWriter.UNSUPPORTED_EXECS).first.getString(0)
        }
        assert(!unsupportedExecs.contains("BroadcastHashJoin"))
      }
    }
  }

  test("test different values for platform argument") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "timezone") { spark =>
        import spark.implicits._
        val testData = Seq((1, 1662519019), (2, 1662519020)).toDF("id", "timestamp")
        spark.sparkContext.setJobDescription("timestamp functions as potential problems")
        testData.createOrReplaceTempView("t1")
        spark.sql("SELECT id, hour(current_timestamp()), second(to_timestamp(timestamp)) FROM t1")
      }

      PlatformNames.getAllNames.foreach { platform =>
        // run the qualification tool for each platform
        TrampolineUtil.withTempDir { outPath =>
          val appArgs = new QualificationArgs(Array(
            "--output-directory",
            outPath.getAbsolutePath,
            "--platform",
            platform,
            eventLog))

          val (exit, _) = QualificationMain.mainInternal(appArgs)
          assert(exit == 0)

          // the code above that runs the Spark query stops the Spark Session,
          // so create a new one to read in the csv file
          createSparkSession()

          // validate that the SQL description in the csv file escapes commas properly
          val outputResults = s"$outPath/rapids_4_spark_qualification_output/" +
            s"rapids_4_spark_qualification_output.csv"
          val outputActual = readExpectedFile(new File(outputResults))
          assert(outputActual.collect().length == 1)
        }
      }
    }
  }

  test("test frequency of repeated job") {
    val logFiles = Array(s"$logDir/empty_eventlog",  s"$logDir/nested_type_eventlog")
    runQualificationTest(logFiles, "multi_run_freq_test_expectation.csv")
  }

  test("test CSV qual output with escaped characters") {
    val jobNames = List("test,name",  "\"test\"name\"", "\"", ",", ",\"")
    jobNames.foreach { jobName =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) =
          ToolTestUtils.generateEventLog(eventLogDir, jobName) { spark =>
            import spark.implicits._
            val testData = Seq((1), (2)).toDF("id")
            spark.sparkContext.setJobDescription("run job with problematic name")
            testData.createOrReplaceTempView("t1")
            spark.sql("SELECT id FROM t1")
          }

        // run the qualification tool
        TrampolineUtil.withTempDir { outpath =>
          val appArgs = new QualificationArgs(Array(
            "--per-sql",
            "--output-directory",
            outpath.getAbsolutePath,
            eventLog))

          val (exit, sumInfo@_) = QualificationMain.mainInternal(appArgs)
          assert(exit == 0)

          // the code above that runs the Spark query stops the Sparksession
          // so create a new one to read in the csv file
          createSparkSession()

          // validate that the SQL description in the csv file escapes commas properly
          val outputResults = s"$outpath/rapids_4_spark_qualification_output/" +
            s"rapids_4_spark_qualification_output.csv"
          val outputActual = readExpectedFile(new File(outputResults), "\"")
          assert(outputActual.select(QualOutputWriter.APP_NAME_STR).first.getString(0) == jobName)

          val persqlResults = s"$outpath/rapids_4_spark_qualification_output/" +
            s"rapids_4_spark_qualification_output_persql.csv"
          val outputPerSqlActual = readPerSqlFile(new File(persqlResults), "\"")
          val rows = outputPerSqlActual.collect()
          assert(rows(1)(0).toString == jobName)
        }
      }
    }
  }

  test("scan hive text-format is supported") {
    // The unit test loads text file into Hive table. Then it runs SQL hive query that generates
    // "Scan hive". If the Qualification fails to support the "Scan hive", then the format would
    // appear in the unsupportedOperators.csv file or the "non-supported read format" column
    TrampolineUtil.withTempDir { warehouseDir =>
      // text file is pair-key-value "key: val_$key"
      val textFilePath = ToolTestUtils.getTestResourcePath("key-value-pairs.txt")
      // set the directory where the store is kept
      TrampolineUtil.withTempDir { outpath =>
        val derbyDir = s"${outpath.getAbsolutePath}/derby"
        System.setProperty("derby.system.home", s"$derbyDir")
        val sparkConfs = Map(
          "spark.sql.warehouse.dir" -> warehouseDir.getAbsolutePath,
          "spark.driver.extraJavaOptions" -> s"-Dderby.system.home='$derbyDir'")
        val allArgs = Array(
          "--report-read-schema", // enable report read schema
          "--output-directory",
          outpath.getAbsolutePath())

        TrampolineUtil.withTempDir { eventLogDir =>
          // set the name to "hiv3" on purpose to avoid any matches on "hive".
          val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "scanHiv3App",
            Some(sparkConfs), enableHive = true) { spark =>
            // scalastyle:off line.size.limit
            // the following set of queries will generate the following physical plan:
            //   [{"nodeName":"Scan hive default.src","simpleString":"Scan hive default.src [key#6, value#7],
            //    HiveTableRelation [`default`.`src`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe,
            //    Data Cols: [key#6, value#7], Partition Cols: []]","children":[],"metadata":{},
            //    "metrics":[{"name":"number of output rows","accumulatorId":12,"metricType":"sum"}]}]
            // scalastyle:on line.size.limit
            spark.sql("DROP TABLE IF EXISTS src")
            spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
            spark.sql(s"LOAD DATA LOCAL INPATH '$textFilePath' INTO TABLE src")
            spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
          }

          val appArgs = new QualificationArgs(allArgs ++ Array(eventLog))
          val (exit, appSum) = QualificationMain.mainInternal(appArgs)

          assert(exit == 0)
          // Verify that results contain a single app
          assert(appSum.nonEmpty)
          // Verify that the texthive is listed in the readFileFormats column
          // The "readSchema" column should be "hivetext[]:hivetext[]:hivetext[]:hivetext[]"
          assert(appSum.head.readFileFormats.exists(_.contains("hivetext")))
          // Verify that the texthive is not listed in the unsupported formats
          // The "Unsupported Read File Formats and Types" column should be empty
          assert(appSum.head.readFileFormatAndTypesNotSupported.isEmpty)
          // Next, we check that the content of the unsupportedOps has no entry for "hive".
          val unsupportedOpsCSV = s"$outpath/rapids_4_spark_qualification_output/" +
            s"rapids_4_spark_qualification_output_unsupportedOperators.csv"
          val inputSource = UTF8Source.fromFile(unsupportedOpsCSV)
          try {
            val unsupportedRows = inputSource.getLines.toSeq
            assert(unsupportedRows.head.contains(
              "App ID,SQL ID,Stage ID,ExecId,Unsupported Type,Unsupported Operator"))
            assert(!unsupportedRows.exists(_.contains("hive")))
          } finally {
            inputSource.close()
          }
        }
      }
    }
  }

  // Expected results as a map of event log -> cluster info.
  // scalastyle:off line.size.limit
  val expectedClusterInfoMap: Seq[(String, Option[ExistingClusterInfo])] = Seq(
    "eventlog_2nodes_8cores" -> // 2 executor nodes with 8 cores.
      Some(ExistingClusterInfo(vendor = PlatformNames.DEFAULT, coresPerExecutor = 8,
        numExecsPerNode = 1, numExecutors = 2, numWorkerNodes = 2, executorHeapMemory = 0L,
        dynamicAllocationEnabled = false, "N/A", "N/A", "N/A", driverHost = Some("10.10.10.100"))),
    "eventlog_3nodes_12cores_multiple_executors" -> // 3 nodes, each with 2 executors having 12 cores.
      Some(ExistingClusterInfo(vendor = PlatformNames.DEFAULT, coresPerExecutor = 12,
        numExecsPerNode = -1, numExecutors = 4, numWorkerNodes = 3, executorHeapMemory = 0L,
        dynamicAllocationEnabled = false, "N/A", "N/A", "N/A", driverHost = Some("10.59.184.210"))),
    "eventlog_4nodes_8cores_dynamic_alloc.zstd" -> // using dynamic allocation, total of 5 nodes, each with max 7
      // executor running having 4 cores. At the end it had 1 active executor.
      Some(ExistingClusterInfo(vendor = PlatformNames.DEFAULT, coresPerExecutor = 4,
        numExecsPerNode = -1, numExecutors = 7, numWorkerNodes = 5, executorHeapMemory = 20480L,
        dynamicAllocationEnabled = true, dynamicAllocationMaxExecutors = "2147483647",
        dynamicAllocationMinExecutors = "0", dynamicAllocationInitialExecutors = "2",
        driverHost = Some("10.10.6.9"))),
    "eventlog_3nodes_12cores_variable_cores" -> // 3 nodes with varying cores: 8, 12, and 8, each with 1 executor.
      Some(ExistingClusterInfo(vendor = PlatformNames.DEFAULT, coresPerExecutor = 12,
        numExecsPerNode = 1, numExecutors = 3, numWorkerNodes = 3, executorHeapMemory = 0L,
        dynamicAllocationEnabled = false, "N/A", "N/A", "N/A", driverHost = Some("10.10.10.100"))),
    "eventlog_3nodes_12cores_exec_removed" -> // 2 nodes, each with 1 executor having 12 cores, 1 executor removed.
      Some(ExistingClusterInfo(vendor = PlatformNames.DEFAULT, coresPerExecutor = 12,
        numExecsPerNode = 1, numExecutors = 2, numWorkerNodes = 2, executorHeapMemory = 0L,
        dynamicAllocationEnabled = false, "N/A", "N/A", "N/A", driverHost = Some("10.10.10.100"))),
    "eventlog_driver_only" -> None // Event log with driver only
  )
  // scalastyle:on line.size.limit

  expectedClusterInfoMap.foreach { case (eventlogPath, expectedClusterInfo) =>
    test(s"test cluster information JSON - $eventlogPath") {
      val logFile = s"$logDir/cluster_information/$eventlogPath"
      runQualificationAndTestClusterInfo(logFile, PlatformNames.DEFAULT, expectedClusterInfo)
    }
  }

  // Expected results as a map of platform -> cluster info.
  val expectedPlatformClusterInfoMap: Seq[(String, ExistingClusterInfo)] = Seq(
    PlatformNames.DATABRICKS_AWS ->
        ExistingClusterInfo(vendor = PlatformNames.DATABRICKS_AWS,
          coresPerExecutor = 8,
          numExecsPerNode = 1,
          numExecutors = 2,
          numWorkerNodes = 2,
          executorHeapMemory = 0L,
          dynamicAllocationEnabled = false,
          "N/A", "N/A", "N/A",
          driverNodeType = Some("m6gd.2xlarge"),
          workerNodeType = Some("m6gd.2xlarge"),
          driverHost = Some("10.10.10.100"),
          clusterId = Some("1212-214324-test"),
          clusterName = Some("test-db-aws-cluster")),
    PlatformNames.DATABRICKS_AZURE ->
      ExistingClusterInfo(vendor = PlatformNames.DATABRICKS_AZURE,
        coresPerExecutor = 8,
        numExecsPerNode = 1,
        numExecutors = 2,
        numWorkerNodes = 2,
        executorHeapMemory = 0L,
        dynamicAllocationEnabled = false,
        "N/A", "N/A", "N/A",
        driverNodeType = Some("Standard_E8ds_v4"),
        workerNodeType = Some("Standard_E8ds_v4"),
        driverHost = Some("10.10.10.100"),
        clusterId = Some("1212-214324-test"),
        clusterName = Some("test-db-azure-cluster")),
    PlatformNames.DATAPROC ->
      ExistingClusterInfo(vendor = PlatformNames.DATAPROC,
        coresPerExecutor = 8,
        numExecsPerNode = 1,
        numExecutors = 2,
        numWorkerNodes = 2,
        executorHeapMemory = 0L,
        dynamicAllocationEnabled = false,
        "N/A", "N/A", "N/A",
        driverHost = Some("dataproc-test-m.c.internal")),
    PlatformNames.EMR ->
      ExistingClusterInfo(vendor = PlatformNames.EMR,
        coresPerExecutor = 8,
        numExecsPerNode = 1,
        numExecutors = 2,
        numWorkerNodes = 2,
        executorHeapMemory = 0L,
        dynamicAllocationEnabled = false,
        "N/A", "N/A", "N/A",
        driverHost = Some("10.10.10.100"),
        clusterId = Some("j-123AB678XY321")),
    PlatformNames.ONPREM ->
      ExistingClusterInfo(vendor = PlatformNames.ONPREM,
        coresPerExecutor = 8,
        numExecsPerNode = 1,
        numExecutors = 2,
        numWorkerNodes = 2,
        executorHeapMemory = 0L,
        dynamicAllocationEnabled = false,
        "N/A", "N/A", "N/A",
        driverHost = Some("10.10.10.100"))
      )

  expectedPlatformClusterInfoMap.foreach { case (platform, expectedClusterInfo) =>
    test(s"test cluster information JSON for platform - $platform ") {
      val logFile = s"$logDir/cluster_information/platform/$platform"
      runQualificationAndTestClusterInfo(logFile, platform, Some(expectedClusterInfo))
    }
  }

  /**
   * Runs the qualification tool and verifies cluster information against expected values.
   */
  private def runQualificationAndTestClusterInfo(eventlogPath: String, platform: String,
      expectedClusterInfo: Option[ExistingClusterInfo]): Unit = {
    TrampolineUtil.withTempDir { outPath =>
      val baseArgs = Array("--output-directory", outPath.getAbsolutePath, "--platform", platform)
      val appArgs = new QualificationArgs(baseArgs :+ eventlogPath)
      val (exitCode, result) = QualificationMain.mainInternal(appArgs)
      assert(exitCode == 0 && result.size == 1,
        "Qualification tool returned unexpected results.")

      // Read JSON as [{'appId': 'app-id-1', ..}]
      def readJson(path: String): Array[ClusterSummary] = {
        val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
        mapper.readValue(new File(path), classOf[Array[ClusterSummary]])
      }

      // Read output JSON and create a set of (event log, cluster info)
      val outputResultFile = s"$outPath/${QualOutputWriter.LOGFILE_NAME}/" +
        s"${QualOutputWriter.LOGFILE_NAME}_cluster_information.json"
      val actualClusterInfo = readJson(outputResultFile).headOption.flatMap(_.clusterInfo)
      assert(actualClusterInfo == expectedClusterInfo,
        "Actual cluster info does not match the expected cluster info.")
    }
  }

  test("test cluster information generation is disabled") {
    // Execute the qualification tool
    TrampolineUtil.withTempDir { outPath =>
      val allArgs = Array(
        "--output-directory",
        outPath.getAbsolutePath,
        "--no-cluster-report",
        s"$logDir/cluster_information/eventlog_2nodes_8cores")

      val appArgs = new QualificationArgs(allArgs)
      val (exitCode, result) = QualificationMain.mainInternal(appArgs)
      assert(exitCode == 0 && result.size == 1)

      // Assert that JSON file does not exists
      val outputResultFile = s"$outPath/${QualOutputWriter.LOGFILE_NAME}/" +
        s"${QualOutputWriter.LOGFILE_NAME}_cluster_information.json"
      assert(!new File(outputResultFile).exists())
    }
  }

  test("test status report generation for wildcard event log") {
    val logFiles = Array(
      s"$logDir/cluster_information/eventlog_3node*") // correct wildcard event log with 3 matches
    // Status counts: 3 SUCCESS, 0 FAILURE, 0 SKIPPED, 0 UNKNOWN
    val expectedStatus = Some(StatusReportCounts(3, 0, 0, 0))
    runQualificationTest(logFiles, expectedStatus = expectedStatus)
  }

  test("test status report generation for incorrect wildcard event logs") {
    val logFiles = Array(
      s"$logDir/cluster_information/eventlog_xxxx*",  // incorrect wildcard event log
      s"$logDir/cluster_information/eventlog_yyyy*")  // incorrect wildcard event log
    // Status counts: 0 SUCCESS, 2 FAILURE, 0 SKIPPED, 0 UNKNOWN
    val expectedStatus = Some(StatusReportCounts(0, 2, 0, 0))
    runQualificationTest(logFiles, expectedStatus = expectedStatus)
  }

  test("test status report generation for mixed event log") {
    val logFiles = Array(
      s"$logDir/nds_q86_test",                        // correct event log
      s"$logDir/cluster_information/eventlog_2node*", // correct wildcard event log with 1 match
      s"$logDir/cluster_information/eventlog_xxxx*")  // incorrect wildcard event log
    // Status counts: 2 SUCCESS, 1 FAILURE, 0 SKIPPED, 0 UNKNOWN
    val expectedStatus = Some(StatusReportCounts(2, 1, 0, 0))
    runQualificationTest(logFiles, expectedStatus = expectedStatus)
  }

  test("test support for photon event log") {
    val logFiles = Array(s"$logDir/nds_q88_photon_db_13_3.zstd")  // photon event log
    // Status counts: 1 SUCCESS, 0 FAILURE, 0 SKIPPED, 0 UNKNOWN
    val expectedStatus = Some(StatusReportCounts(1, 0, 0, 0))
    runQualificationTest(logFiles, expectedStatus = expectedStatus)
  }

  test("process multiple attempts of the same app ID and skip lower attempts") {
    TrampolineUtil.withTempDir { outPath =>
      val baseArgs = Array("--output-directory",
        outPath.getAbsolutePath,
        "-n", "12",
        s"$logDir/multiple_attempts/*")
      val appArgs = new QualificationArgs(baseArgs)
      val (exitCode, result) = QualificationMain.mainInternal(appArgs)
      assert(exitCode == 0 && result.size == 1,
        "Qualification tool returned unexpected results.")

      val statusResultFile = s"$outPath/${QualOutputWriter.LOGFILE_NAME}/" +
        s"${QualOutputWriter.LOGFILE_NAME}_status.csv"

      // Verify that the status file contains the expected messages for skipped
      // attempts (1, 2, 3) and thus only the latest attempt (4) is processed.
      val statusFileContents = UTF8Source.fromFile(statusResultFile).mkString
      Seq(1, 2, 3).foreach { attemptId =>
        val expectedMessage = s"skipping this attempt $attemptId as a newer " +
          "attemptId is being processed"
        assert(statusFileContents.contains(expectedMessage),
          s"Expected message not found in status file: $expectedMessage")
      }

      // Status counts: 1 SUCCESS, 0 FAILURE, 3 SKIPPED, 0 UNKNOWN
      val expectedStatusCount = StatusReportCounts(1, 0, 3, 0)
      ToolTestUtils.compareStatusReport(sparkSession, expectedStatusCount, statusResultFile)
    }
  }

  ignore("process multiple event logs with same app ID and attempt ID: Not supported") {
    TrampolineUtil.withTempDir { outPath =>
      val baseArgs = Array("--output-directory",
        outPath.getAbsolutePath,
        s"$logDir/eventlog_same_app_id_1.zstd",
        s"$logDir/eventlog_same_app_id_2.zstd")
      val appArgs = new QualificationArgs(baseArgs)
      val (exitCode, result) = QualificationMain.mainInternal(appArgs)
      assert(exitCode == 0 && result.size == 1,
        "Qualification tool returned unexpected results.")

      val statusResultFile = s"$outPath/${QualOutputWriter.LOGFILE_NAME}/" +
        s"${QualOutputWriter.LOGFILE_NAME}_status.csv"

      // Only one of the event logs should be processed and the other should be skipped.
      // Status counts: 1 SUCCESS, 0 FAILURE, 1 SKIPPED, 0 UNKNOWN
      val expectedStatusCount = StatusReportCounts(1, 0, 1, 0)
      ToolTestUtils.compareStatusReport(sparkSession, expectedStatusCount, statusResultFile)
    }
  }
}

class ToolTestListener extends SparkListener {
  val completedStages = new ListBuffer[SparkListenerStageCompleted]()
  var executorCpuTime = 0L

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    executorCpuTime += NANOSECONDS.toMillis(taskEnd.taskMetrics.executorCpuTime)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    completedStages.append(stageCompleted)
  }
}
