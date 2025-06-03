/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.nvidia.spark.rapids.tool.profiling.ProfileArgs
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter
import com.nvidia.spark.rapids.tool.tuning.{GpuWorkerProps, SparkProperties, TargetClusterProps, WorkerInfo}
import org.apache.hadoop.fs.Path
import org.yaml.snakeyaml.{DumperOptions, Yaml}
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession, TrampolineUtil}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.rapids.tool.ClusterSummary
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil
import org.apache.spark.sql.types._

object ToolTestUtils extends Logging {

  // Scheme fields for Status Report file
  private val csvStatusFields = Seq(
    (QualOutputWriter.STATUS_REPORT_PATH_STR, StringType),
    (QualOutputWriter.STATUS_REPORT_STATUS_STR, StringType),
    (QualOutputWriter.STATUS_REPORT_APP_ID, StringType),
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
      confs: Option[Map[String, String]] = None,
      enableHive: Boolean = false)
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
    if (enableHive) {
      sparkBuilder.enableHiveSupport()
    }
    confs.foreach(_.foreach {case (k, v) => sparkBuilder.config(k, v)})
    lazy val spark = sparkBuilder.getOrCreate()

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
      expectedDf.show(1000, truncate = false)
      df.show(1000, truncate = false)
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
    val platform = PlatformFactory.createInstance(appArgs.platform())
    for (path <- appArgs.eventlog()) {
      val eventLogInfo = EventLogPathProcessor
        .getEventLogInfo(path, RapidsToolsConfUtil.newHadoopConf())
      assert(eventLogInfo.nonEmpty, s"event log not parsed as expected $path")
      apps += new ApplicationInfo(RapidsToolsConfUtil.newHadoopConf(),
        eventLogInfo.head._1, platform)
      index += 1
    }
    apps
  }

  /**
   * Performs an expected status report dataframe to validate the counts
   * of different status categories (SUCCESS, FAILURE, UNKNOWN, SKIPPED).
   */
  def compareStatusReport(sparkSession: SparkSession, expStatusReportCount: StatusReportCounts,
      filePath: String): Unit = {
    val csvFile = new File(filePath)

    // If the status report file does not exist, all applications are expected to be failures.
    if(!csvFile.exists()) {
      assert(expStatusReportCount.success == 0 &&
        expStatusReportCount.unknown == 0 &&
        expStatusReportCount.failure > 0)
    } else {
      val expectedStatusDf = readExpectationCSV(sparkSession, filePath, Some(statusReportSchema))

      // Function to count the occurrences of a specific status in the DataFrame
      def countStatus(status: String): Long =
        expectedStatusDf.filter(col("STATUS") === lit(status)).count()

      val actualStatusReportCount = StatusReportCounts(
        countStatus("SUCCESS"),
        countStatus("FAILURE"),
        countStatus("SKIPPED"),
        countStatus("UNKNOWN"))
      assert(actualStatusReportCount == expStatusReportCount,
        s"Expected status report counts: $expStatusReportCount, " +
          s"but got: $actualStatusReportCount")
    }
  }

  /**
   * Load a JSON file containing an array of ClusterSummary objects.
   * @param path The path to the JSON file.
   * @return An array of ClusterSummary objects.
   */
  def loadClusterSummaryFromJson(path: String): Array[ClusterSummary] = {
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    mapper.readValue(new File(path), classOf[Array[ClusterSummary]])
  }

  def buildTargetClusterInfo(
      instanceType: Option[String] = None,
      gpuCount: Option[Int] = None,
      gpuMemory: Option[String] = None,
      gpuDevice: Option[String] = None,
      enforcedSparkProperties: Map[String, String] = Map.empty): TargetClusterProps = {
    import scala.collection.JavaConverters._
    val gpuWorkerProps = new GpuWorkerProps(
      gpuMemory.getOrElse(""), gpuCount.getOrElse(0), gpuDevice.getOrElse(""))
    val workerProps = new WorkerInfo(instanceType.getOrElse(""), gpuWorkerProps)
    val sparkProps = new SparkProperties()
    sparkProps.getEnforced.putAll(enforcedSparkProperties.asJava)
    new TargetClusterProps(workerProps, sparkProps)
  }

  def buildTargetClusterInfoAsString(
      instanceType: Option[String] = None,
      gpuCount: Option[Int] = None,
      gpuMemory: Option[String] = None,
      gpuDevice: Option[String] = None,
      enforcedSparkProperties: Map[String, String] = Map.empty): String = {
    val targetCluster = buildTargetClusterInfo(instanceType, gpuCount, gpuMemory,
      gpuDevice, enforcedSparkProperties)
    // set the options to convert the object into formatted yaml content
    val options = new DumperOptions()
    options.setIndent(2)
    options.setPrettyFlow(true)
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(options)
    val rawString = yaml.dump(targetCluster)
    // Skip the first line as it contains "the name of the class"
    rawString.split("\n").drop(1).mkString("\n")
  }

  def createTargetClusterInfoFile(
      outputDirectory: String,
      instanceType: Option[String] = None,
      gpuCount: Option[Int] = None,
      gpuMemory: Option[String] = None,
      gpuDevice: Option[String] = None,
      enforcedSparkProperties: Map[String, String] = Map.empty): Path = {
    val fileWriter = new ToolTextFileWriter(outputDirectory, "targetClusterInfo.yaml",
      "Target Cluster Info")
    try {
      val targetClusterInfoString = buildTargetClusterInfoAsString(instanceType, gpuCount,
        gpuMemory, gpuDevice, enforcedSparkProperties)
      fileWriter.write(targetClusterInfoString)
      fileWriter.getFileOutputPath
    } finally {
      fileWriter.close()
    }
  }
}

/**
 * Case class representing the counts of different status categories in a status report.
 */
case class StatusReportCounts(success: Long, failure: Long, skipped: Long, unknown: Long)
