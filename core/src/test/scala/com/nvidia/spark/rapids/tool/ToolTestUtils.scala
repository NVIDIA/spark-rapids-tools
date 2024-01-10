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

package com.nvidia.spark.rapids.tool

import java.io.{File, FilenameFilter, FileNotFoundException}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.profiling.ProfileArgs
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession, TrampolineUtil}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil
import org.apache.spark.sql.types._

object ToolTestUtils extends Logging {

  // Scheme fields for Status Report file
  private val csvStatusFields = Seq(
    (QualOutputWriter.STATUS_REPORT_PATH_STR, StringType),
    (QualOutputWriter.STATUS_REPORT_STATUS_STR, StringType),
    (QualOutputWriter.STATUS_REPORT_DESC_STR, StringType))

  val statusReportSchema =
    new StructType(csvStatusFields.map(f => StructField(f._1, f._2)).toArray)

  def getTestResourceFile(file: String): File = {
    new File(getClass.getClassLoader.getResource(file).getFile)
  }

  def getTestResourcePath(file: String): String = {
    getTestResourceFile(file).getCanonicalPath
  }

  def runAndCollect(appName: String)
    (fun: SparkSession => DataFrame): String = {

    // we need to close any existing sessions to ensure that we can
    // create a session with a new event log dir
    TrampolineUtil.cleanupAnyExistingSession()

    lazy val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()

    // execute the query and generate events
    val df = fun(spark)
    df.collect()

    val appId = spark.sparkContext.applicationId

    // close the event log
    spark.close()
    appId
  }

  def generateEventLog(eventLogDir: File, appName: String,
      confs: Option[Map[String, String]] = None)
    (fun: SparkSession => DataFrame): (String, String) = {

    // we need to close any existing sessions to ensure that we can
    // create a session with a new event log dir
    TrampolineUtil.cleanupAnyExistingSession()

    lazy val sparkBuilder = SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", eventLogDir.getAbsolutePath)
    confs.foreach(_.foreach {case (k, v) => sparkBuilder.config(k, v)})
    lazy val spark  = sparkBuilder.getOrCreate()

    // execute the query and generate events
    val df = fun(spark)
    df.collect()

    val appId = spark.sparkContext.applicationId

    // close the event log
    spark.close()

    // find the event log
    val files = listFilesMatching(eventLogDir, !_.startsWith("."))
    if (files.length != 1) {
      throw new FileNotFoundException(s"Could not find event log in ${eventLogDir.getAbsolutePath}")
    }
    (files.head.getAbsolutePath, appId)
  }

  def listFilesMatching(dir: File, matcher: String => Boolean): Array[File] = {
    dir.listFiles(new FilenameFilter {
      override def accept(file: File, s: String): Boolean = matcher(s)
    })
  }

  def compareDataFrames(df: DataFrame, expectedDf: DataFrame): Unit = {
    val diffCount = df.except(expectedDf).union(expectedDf.except(df)).count
    if (diffCount != 0) {
      logWarning("Diff expected vs actual:")
      expectedDf.show(1000, false)
      df.show(1000, false)
    }
    assert(diffCount == 0)
  }

  def readExpectationCSV(sparkSession: SparkSession, path: String,
      schema: Option[StructType] = None, escape: String = "\\"): DataFrame = {
    // make sure to change null value so empty strings don't show up as nulls
    if (schema.isDefined) {
      sparkSession.read.option("header", "true").option("nullValue", "-").option("escape", escape)
        .schema(schema.get).csv(path)
    } else {
      sparkSession.read.option("header", "true").option("nullValue", "-").option("escape", escape)
        .csv(path)
    }
  }

  def processProfileApps(logs: Array[String],
      sparkSession: SparkSession): ArrayBuffer[ApplicationInfo] = {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(logs)
    var index: Int = 1
    for (path <- appArgs.eventlog()) {
      val eventLogInfo = EventLogPathProcessor
        .getEventLogInfo(path, RapidsToolsConfUtil.newHadoopConf())
      assert(eventLogInfo.size >= 1, s"event log not parsed as expected $path")
      apps += new ApplicationInfo(RapidsToolsConfUtil.newHadoopConf(),
        eventLogInfo.head._1, index)
      index += 1
    }
    apps
  }

  /**
   * Performs an expected status report dataframe to validate the counts
   * of different status categories (SUCCESS, FAILURE, UNKNOWN).
   */
  def compareStatusReport(sparkSession: SparkSession, outpath: String,
      expStatusReportCount: StatusReportCounts): Unit = {
    val filename = s"$outpath/rapids_4_spark_qualification_output/" +
      s"rapids_4_spark_qualification_output_status.csv"
    val csvFile = new File(filename)

    // If the status report file does not exist, all applications are expected to be failures.
    if(!csvFile.exists()) {
      assert(expStatusReportCount.success == 0 &&
        expStatusReportCount.unknown == 0 &&
        expStatusReportCount.failure > 0)
    } else {
      val expectedStatusDf = readExpectationCSV(sparkSession, filename, Some(statusReportSchema))

      // Function to count the occurrences of a specific status in the DataFrame
      def countStatus(status: String): Long =
        expectedStatusDf.filter(col("STATUS") === lit(status)).count()

      val actualStatusReportCount = StatusReportCounts(
        countStatus("SUCCESS"),
        countStatus("FAILURE"),
        countStatus("UNKNOWN"))
      assert(actualStatusReportCount == expStatusReportCount)
    }
  }
}

/**
 * Case class representing the counts of different status categories in a status report.
 */
case class StatusReportCounts(success: Long, failure: Long, unknown: Long)
