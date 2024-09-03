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

package com.nvidia.spark.rapids.tool.profiling

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, StatusReportCounts, ToolTestUtils}
import com.nvidia.spark.rapids.tool.views.RawMetricProfilerView
import org.apache.hadoop.io.IOUtils
import org.scalatest.FunSuite

import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.profiling._
import org.apache.spark.sql.rapids.tool.util.FSUtils

class ApplicationInfoSuite extends FunSuite with Logging {

  lazy val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }

  lazy val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

  private val expRoot = ToolTestUtils.getTestResourceFile("ProfilingExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
  private val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")

  test("test single event") {
    testSqlCompression()
  }

  test("zstd: test sqlMetrics duration and execute cpu time") {
    testSqlCompression(Option("zstd"))
  }

  test("snappy: test sqlMetrics duration and execute cpu time") {
    testSqlCompression(Option("snappy"))
  }

  test("lzf: test sqlMetrics duration and execute cpu time") {
    testSqlCompression(Option("lz4"))
  }

  test("lz4: test sqlMetrics duration and execute cpu time") {
    testSqlCompression(Option("lzf"))
  }

  private def testSqlCompression(compressionNameOpt: Option[String] = None) = {
    val rawLog = s"$logDir/eventlog_minimal_events"
    compressionNameOpt.foreach { compressionName =>
      val codec = TrampolineUtil.createCodec(sparkSession.sparkContext.getConf,
        compressionName)
      TrampolineUtil.withTempDir { tempDir =>
        val compressionFileName = new File(tempDir,
          "eventlog_minimal_events." + compressionName)
        val inputStream = Files.newInputStream(Paths.get(rawLog))
        val outputStream = codec.compressedOutputStream(
          Files.newOutputStream(compressionFileName.toPath, StandardOpenOption.CREATE))
        // copy and close streams
        IOUtils.copyBytes(inputStream, outputStream, 4096, true)
        testSingleEventFile(Array(tempDir.toString))
        compressionFileName.delete()
      }
    }
    if (compressionNameOpt.isEmpty) {
      testSingleEventFile(Array(rawLog))
    }
  }

  private def testSingleEventFile(logs: Array[String]): Unit = {
    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    assert(apps.size == 1)
    val firstApp = apps.head
    assert(firstApp.sparkVersion.equals("3.1.1"))
    assert(firstApp.gpuMode.equals(true))
    assert(firstApp.jobIdToInfo.keys.toSeq.contains(1))
    val stageInfo = firstApp.stageManager.getStage(0, 0)
    assert(stageInfo.isDefined && stageInfo.get.stageInfo.numTasks.equals(1))
    assert(firstApp.stageManager.getStage(2, 0).isDefined)
    assert(firstApp.taskManager.getTasks(firstApp.index, 0).head.successful.equals(true))
    assert(firstApp.taskManager.getTasks(firstApp.index, 0).head.endReason.equals("Success"))
    val execInfo = firstApp.executorIdToInfo.get(firstApp.executorIdToInfo.keys.head)
    assert(execInfo.isDefined && execInfo.get.totalCores.equals(8))
    val rp = firstApp.resourceProfIdToInfo.get(firstApp.resourceProfIdToInfo.keys.head)
    assert(rp.isDefined)
    val memory = rp.get.executorResources(ResourceProfile.MEMORY)
    assert(memory.amount.equals(1024L))
  }

  test("test rapids jar") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/rapids_join_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    assert(apps.head.sparkVersion.equals("3.0.1"))
    assert(apps.head.gpuMode.equals(true))
    val rapidsJar =
      apps.head.classpathEntries.filterKeys(_ matches ".*rapids-4-spark_2.12-0.5.0.jar.*")
    val cuDFJar = apps.head.classpathEntries.filterKeys(_ matches ".*cudf-0.19.2-cuda11.jar.*")
    assert(rapidsJar.size == 1, "Rapids jar check")
    assert(cuDFJar.size == 1, "CUDF jar check")

    val collect = new CollectInformation(apps)
    val rapidsJarResults = collect.getRapidsJARInfo
    assert(rapidsJarResults.size === 2)
    assert(rapidsJarResults.filter(_.jar.contains("rapids-4-spark_2.12-0.5.0.jar")).size === 1)
    assert(rapidsJarResults.filter(_.jar.contains("cudf-0.19.2-cuda11.jar")).size === 1)

    assert(apps.head.getEventLogPath == s"file:$logDir/rapids_join_eventlog.zstd")
  }

  test("test sql and resourceprofile eventlog") {
    val eventLog = s"$logDir/rp_sql_eventlog.zstd"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
    }
  }

  test("test spark2 eventlog") {
    val eventLog = Array(s"$logDir/spark2-eventlog.zstd")
    val apps = ToolTestUtils.processProfileApps(eventLog, sparkSession)
    assert(apps.size == 1)
    assert(apps.head.sparkVersion.equals("2.2.3"))
    assert(apps.head.gpuMode.equals(false))
    assert(apps.head.jobIdToInfo.keys.toSeq.size == 6)
    assert(apps.head.jobIdToInfo.keys.toSeq.contains(0))
    val stage0 = apps.head.stageManager.getStage(0, 0)
    assert(stage0.isDefined)
    assert(stage0.get.stageInfo.numTasks.equals(1))
  }

  test("test no sql eventlog") {
    val eventLog = s"$logDir/rp_nosql_eventlog"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
    }
  }

  test("test printSQLPlanMetrics") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/rapids_join_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    val collect = new CollectInformation(apps)
    val sqlMetrics = collect.getSQLPlanMetrics
    val resultExpectation =
      new File(expRoot, "rapids_join_eventlog_sqlmetrics_expectation.csv")
    assert(sqlMetrics.size == 83)
    val sqlMetricsWithDelim = sqlMetrics.map { metrics =>
      metrics.copy(stageIds = ProfileUtils.replaceDelimiter(metrics.stageIds, ","))
    }
    import sparkSession.implicits._
    val df = sqlMetricsWithDelim.toDF
    val dfExpect = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectation.getPath())
    ToolTestUtils.compareDataFrames(df, dfExpect)
  }

  test("test GpuMetrics in eventlog") {
    TrampolineUtil.withTempDir { outputDir =>
      TrampolineUtil.withTempDir { tmpEventLogDir =>
        val eventLogFilePath = Paths.get(tmpEventLogDir.getAbsolutePath, "gpu_metrics_eventlog")
        // scalastyle:off line.size.limit
        val eventLogContent =
          """{"Event":"SparkListenerLogStart","Spark Version":"3.2.1"}
            |{"Event":"SparkListenerApplicationStart","App Name":"GPUMetrics", "App ID":"local-16261043003", "Timestamp":123456, "User":"User1"}
            |{"Event":"SparkListenerTaskEnd","Stage ID":10,"Stage Attempt ID":0,"Task Type":"ShuffleMapTask","Task End Reason":{"Reason":"Success"},"Task Info":{"Task ID":5073,"Index":5054,"Attempt":0,"Partition ID":5054,"Launch Time":1712248533994,"Executor ID":"100","Host":"10.154.65.143","Locality":"PROCESS_LOCAL","Speculative":false,"Getting Result Time":0,"Finish Time":1712253284920,"Failed":false,"Killed":false,"Accumulables":[{"ID":1010,"Name":"gpuSemaphoreWait","Update":"00:00:00.492","Value":"03:13:31.359","Internal":false,"Count Failed Values":true},{"ID":1018,"Name":"gpuSpillToHostTime","Update":"00:00:00.845","Value":"00:29:39.521","Internal":false,"Count Failed Values":true},{"ID":1016,"Name":"gpuSplitAndRetryCount","Update":"1","Value":"2","Internal":false,"Count Failed Values":true}]}}
            |{"Event":"SparkListenerTaskEnd","Stage ID":10,"Stage Attempt ID":0,"Task Type":"ShuffleMapTask","Task End Reason":{"Reason":"Success"},"Task Info":{"Task ID":2913,"Index":2894,"Attempt":0,"Partition ID":2894,"Launch Time":1712248532696,"Executor ID":"24","Host":"10.154.65.135","Locality":"PROCESS_LOCAL","Speculative":false,"Getting Result Time":0,"Finish Time":1712253285639,"Failed":false,"Killed":false,"Accumulables":[{"ID":1010,"Name":"gpuSemaphoreWait","Update":"00:00:00.758","Value":"03:13:32.117","Internal":false,"Count Failed Values":true},{"ID":1015,"Name":"gpuReadSpillFromDiskTime","Update":"00:00:02.599","Value":"00:33:37.153","Internal":false,"Count Failed Values":true},{"ID":1018,"Name":"gpuSpillToHostTime","Update":"00:00:00.845","Value":"00:29:39.521","Internal":false,"Count Failed Values":true}]}}
            |{"Event":"SparkListenerTaskEnd","Stage ID":11,"Stage Attempt ID":0,"Task Type":"ShuffleMapTask","Task End Reason":{"Reason":"Success"},"Task Info":{"Task ID":2045,"Index":2026,"Attempt":0,"Partition ID":2026,"Launch Time":1712248530708,"Executor ID":"84","Host":"10.154.65.233","Locality":"PROCESS_LOCAL","Speculative":false,"Getting Result Time":0,"Finish Time":1712253285667,"Failed":false,"Killed":false,"Accumulables":[{"ID":1010,"Name":"gpuSemaphoreWait","Update":"00:00:00.003","Value":"03:13:32.120","Internal":false,"Count Failed Values":true},{"ID":1015,"Name":"gpuReadSpillFromDiskTime","Update":"00:00:00.955","Value":"00:33:38.108","Internal":false,"Count Failed Values":true}]}}""".stripMargin
        // scalastyle:on line.size.limit
        Files.write(eventLogFilePath, eventLogContent.getBytes(StandardCharsets.UTF_8))

        val profileArgs = Array(
          "--output-directory", outputDir.getAbsolutePath,
          eventLogFilePath.toString
        )

        val appArgs = new ProfileArgs(profileArgs)
        val (exit, _) = ProfileMain.mainInternal(appArgs)
        assert(exit == 0)

        val apps = ArrayBuffer[ApplicationInfo]()
        var index = 1

        val eventLogPaths = appArgs.eventlog()
        eventLogPaths.foreach { path =>
          val eventLogInfo = EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1
          apps += new ApplicationInfo(hadoopConf, eventLogInfo, index)
          index += 1
        }
        assert(apps.size == 1)

        val collect = new CollectInformation(apps)
        val stageLevelResults = collect.getStageLevelMetrics

        // Sample eventlog has 4 gpu metrics - gpuSemaphoreWait, gpuReadSpillFromDiskTime
        // gpuSpillToHostTime, gpuSplitAndRetryCount. But gpuSemaphoreWait and
        // gpuReadSpillFromDiskTime metrics are updated in 2 stages(stage 10 and 11).
        // So the result will have 6 rows in total since we are reporting stage level metrics.
        assert(stageLevelResults.size == 6)
        // gpu metrics
        val gpuSemaphoreWait = stageLevelResults.find(_.accMetaRef.getName() == "gpuSemaphoreWait")
        assert(gpuSemaphoreWait.isDefined)
      }
    }
  }

  test("test printSQLPlans") {
    TrampolineUtil.withTempDir { tempOutputDir =>
      val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/rapids_join_eventlog.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path,
            sparkSession.sparkContext.hadoopConfiguration).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      CollectInformation.printSQLPlans(apps, tempOutputDir.getAbsolutePath)
      val outputDir = new File(tempOutputDir, apps.head.appId)
      val dotDirs = ToolTestUtils.listFilesMatching(outputDir, _.endsWith("planDescriptions.log"))
      assert(dotDirs.length === 1)
    }
  }

  test("test read GPU datasourcev1") {
    TrampolineUtil.withTempDir { _ =>
      val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/eventlog-gpu-dsv1.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path,
            sparkSession.sparkContext.hadoopConfiguration).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps)
      val dsRes = collect.getDataSourceInfo
      assert(dsRes.size == 5)
      val allFormats = dsRes.map { r =>
        r.format
      }.toSet
      val expectedFormats = Set("Text", "CSV(GPU)", "Parquet(GPU)", "ORC(GPU)", "JSON(GPU)")
      assert(allFormats.equals(expectedFormats))
      val allSchema = dsRes.map { r =>
        r.schema
      }.toSet
      assert(allSchema.forall(_.nonEmpty))
      val schemaParquet = dsRes.filter { r =>
        r.sqlID == 4
      }
      assert(schemaParquet.size == 1)
      val schemaText = dsRes.filter { r =>
        r.sqlID == 0
      }
      assert(schemaText.size == 1)
      val parquetRow = schemaParquet.head
      assert(parquetRow.schema.contains("loan_id"))
    }
  }

  test("test read GPU datasourcev2") {
    TrampolineUtil.withTempDir { _ =>
      val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/eventlog-gpu-dsv2.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps)
      val dsRes = collect.getDataSourceInfo
      assert(dsRes.size == 5)
      val allFormats = dsRes.map { r =>
        r.format
      }.toSet
      val expectedFormats =
        Set("Text", "gpucsv(GPU)", "gpujson(GPU)", "gpuparquet(GPU)", "gpuorc(GPU)")
      assert(allFormats.equals(expectedFormats))
      val allSchema = dsRes.map { r =>
        r.schema
      }.toSet
      assert(allSchema.forall(_.nonEmpty))
      val schemaParquet = dsRes.filter { r =>
        r.sqlID == 3
      }
      assert(schemaParquet.size == 1)
      val parquetRow = schemaParquet.head
      assert(parquetRow.schema.contains("loan_id"))
    }
  }

  test("test read datasourcev1") {
    TrampolineUtil.withTempDir { _ =>
      val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/eventlog_dsv1.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps)
      val dsRes = collect.getDataSourceInfo
      assert(dsRes.size == 7)
      val allFormats = dsRes.map { r =>
        r.format
      }.toSet
      val expectedFormats = Set("Text", "CSV", "Parquet", "ORC", "JSON")
      assert(allFormats.equals(expectedFormats))
      val allSchema = dsRes.map { r =>
        r.schema
      }.toSet
      assert(allSchema.forall(_.nonEmpty))
      val schemaParquet = dsRes.filter { r =>
        r.sqlID == 2
      }
      assert(schemaParquet.size == 1)
      val parquetRow = schemaParquet.head
      assert(parquetRow.schema.contains("loan400"))
      assert(parquetRow.location.contains("lotscolumnsout"))
    }
  }

  test("test read datasourcev2") {
    TrampolineUtil.withTempDir { _ =>
      val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/eventlog_dsv2.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
        index += 1
      }

      assert(apps.size == 1)
      val collect = new CollectInformation(apps)
      val dsRes = collect.getDataSourceInfo
      assert(dsRes.size == 9)
      val allFormats = dsRes.map { r =>
        r.format
      }.toSet
      val expectedFormats = Set("Text", "csv", "parquet", "orc", "json")
      assert(allFormats.equals(expectedFormats))
      val allSchema = dsRes.map { r =>
        r.schema
      }.toSet
      assert(allSchema.forall(_.nonEmpty))
      val schemaParquet = dsRes.filter { r =>
        r.sqlID == 2
      }
      assert(schemaParquet.size == 1)
      val parquetRow = schemaParquet.head
      // schema is truncated in v2
      assert(!parquetRow.schema.contains("loan400"))
      assert(parquetRow.schema.contains("..."))
      assert(parquetRow.location.contains("lotscolumnsout"))
    }
  }

  test("test IOMetrics") {
    TrampolineUtil.withTempDir { _ =>
      val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/eventlog-gpu-dsv1.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val aggResults = RawMetricProfilerView.getAggMetrics(apps)
      val ioMetrics = aggResults.ioAggs
      assert(ioMetrics.size == 5)
      val metricsSqlId1 = ioMetrics.filter(metrics => metrics.sqlId == 1)
      assert(metricsSqlId1.size == 1)
      assert(metricsSqlId1.head.inputBytesReadSum == 1348)
      assert(metricsSqlId1.head.inputRecordsReadSum == 66)
      assert(metricsSqlId1.head.outputBytesWrittenSum == 0)
      assert(metricsSqlId1.head.outputRecordsWrittenSum == 0)
      assert(metricsSqlId1.head.diskBytesSpilledSum == 0)
      assert(metricsSqlId1.head.memoryBytesSpilledSum == 0)
      assert(metricsSqlId1.head.srTotalBytesReadSum == 0)
      assert(metricsSqlId1.head.swTotalBytesWriteSum == 0)
    }
  }

  test("test jdbc read") {
    TrampolineUtil.withTempDir { _ =>
      val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$qualLogDir/jdbc_eventlog.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps)
      val dsRes = collect.getDataSourceInfo
      val format = dsRes.map(r => r.format).toSet.mkString
      val expectedFormat = "JDBC"
      val location = dsRes.map(r => r.location).toSet.mkString
      val expectedLocation = "TBLS"
      assert(format.equals(expectedFormat))
      assert(location.equals(expectedLocation))
      dsRes.foreach { r =>
        assert(r.schema.contains("bigint"))
      }
    }
  }

  test("test printJobInfo") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/rp_sql_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path,hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    val collect = new CollectInformation(apps)
    val jobInfo = collect.getJobInfo

    assert(jobInfo.size == 2)
    val firstRow = jobInfo.head
    assert(firstRow.jobID === 0)
    assert(firstRow.stageIds.size === 1)
    assert(firstRow.sqlID === None)
    assert(firstRow.startTime === 1622846402778L)
    assert(firstRow.endTime === Some(1622846410240L))

    val secondRow = jobInfo(1)
    assert(secondRow.jobID === 1)
    assert(secondRow.stageIds.size === 4)
    assert(secondRow.sqlID.isDefined && secondRow.sqlID.get === 0)
    assert(secondRow.startTime === 1622846431114L)
    assert(secondRow.endTime === Some(1622846441591L))
  }

  test("test sqlToStages") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/rp_sql_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    val collect = new CollectInformation(apps)
    val sqlToStageInfo = collect.getSQLToStage

    assert(sqlToStageInfo.size == 4)
    val firstRow = sqlToStageInfo.head
    assert(firstRow.stageId === 1)
    assert(firstRow.sqlID === 0)
    assert(firstRow.jobID === 1)
    assert(firstRow.duration === Some(8174))
    assert(firstRow.nodeNames.mkString(",") === "Exchange(9),WholeStageCodegen (1)(10),Scan(13)")
  }

  test("test wholeStage mapping") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/rp_sql_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    val collect = new CollectInformation(apps)
    val wholeStageMapping = collect.getWholeStageCodeGenMapping

    assert(wholeStageMapping.size == 10)
    val firstRow = wholeStageMapping.head
    assert(firstRow.nodeID === 0)
    assert(firstRow.sqlID === 0)
    assert(firstRow.parent === "WholeStageCodegen (6)")
    assert(firstRow.child === "HashAggregate")
    assert(firstRow.childNodeID === 1)
    val lastRow = wholeStageMapping(9)
    assert(lastRow.nodeID === 17)
    assert(lastRow.sqlID === 0)
    assert(lastRow.parent === "WholeStageCodegen (3)")
    assert(lastRow.child === "SerializeFromObject")
    assert(lastRow.childNodeID === 19)
  }


  test("test multiple resource profile in single app") {
    val apps :ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(Array(s"$logDir/rp_nosql_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    assert(apps.head.resourceProfIdToInfo.size == 2)

    val row0 = apps.head.resourceProfIdToInfo(0)
    assert(row0.resourceProfileId.equals(0))
    val execMem0 = row0.executorResources.get(ResourceProfile.MEMORY)
    val execCores0 = row0.executorResources.get(ResourceProfile.CORES)
    assert(execMem0.isDefined && execMem0.get.amount === 20480L)
    assert(execCores0.isDefined && execCores0.get.amount === 4)

    val row1 = apps.head.resourceProfIdToInfo(1)
    assert(row1.resourceProfileId.equals(1))
    val execMem1 = row1.executorResources.get(ResourceProfile.MEMORY)
    val execCores1 = row1.executorResources.get(ResourceProfile.CORES)
    assert(execMem1.isDefined && execMem1.get.amount === 6144L)
    assert(execCores1.isDefined && execCores1.get.amount === 2)
  }

  test("test spark2 and spark3 event logs") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(Array(s"$logDir/tasks_executors_fail_compressed_eventlog.zstd",
      s"$logDir/spark2-eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 2)
    val collect = new CollectInformation(apps)
    val execInfos = collect.getExecutorInfo
    // just the fact it worked makes sure we can run with both files
    // since we give them indexes above they should be in the right order
    // and spark2 event info should be second
    val firstRow = execInfos.head
    assert(firstRow.resourceProfileId === 0)

    val secondRow = execInfos(1)
    assert(secondRow.resourceProfileId === 0)
  }

  test("test filename match") {
    val matchFileName = "udf"
    val appArgs = new ProfileArgs(Array(
      "--match-event-logs",
      matchFileName,
      s"$qualLogDir/udf_func_eventlog",
      s"$qualLogDir/udf_dataset_eventlog",
      s"$qualLogDir/dataset_eventlog"
    ))

    val (result, _) = EventLogPathProcessor.processAllPaths(appArgs.filterCriteria.toOption,
      appArgs.matchEventLogs.toOption, appArgs.eventlog(), hadoopConf)
    assert(result.length == 2)
  }

  test("test filter file newest") {
    val tempFile1 = File.createTempFile("tempOutputFile1", "")
    val tempFile2 = File.createTempFile("tempOutputFile2", "")
    val tempFile3 = File.createTempFile("tempOutputFile3", "")
    val tempFile4 = File.createTempFile("tempOutputFile4", "")
    try {
      tempFile1.deleteOnExit()
      tempFile2.deleteOnExit()
      tempFile3.deleteOnExit()
      tempFile4.deleteOnExit()

      tempFile1.setLastModified(98765432) // newest file
      tempFile2.setLastModified(12324567) // oldest file
      tempFile3.setLastModified(34567891) // second newest file
      tempFile4.setLastModified(23456789)
      val filterNew = "2-newest-filesystem"
      val appArgs = new ProfileArgs(Array(
        "--filter-criteria",
        filterNew,
        tempFile1.toString,
        tempFile2.toString,
        tempFile3.toString
      ))

      val (result, _) = EventLogPathProcessor.processAllPaths(appArgs.filterCriteria.toOption,
        appArgs.matchEventLogs.toOption, appArgs.eventlog(), hadoopConf)
      assert(result.length == 2)
      // Validate 2 newest files
      assert(result(0).eventLog.getName.equals(tempFile1.getName))
      assert(result(1).eventLog.getName.equals(tempFile3.getName))
    } finally {
      tempFile1.delete()
      tempFile2.delete()
      tempFile3.delete()
      tempFile4.delete()
    }
  }

  test("test filter file oldest and file name match") {

    val tempFile1 = File.createTempFile("tempOutputFile1", "")
    val tempFile2 = File.createTempFile("tempOutputFile2", "")
    val tempFile3 = File.createTempFile("tempOutputFile3", "")
    val tempFile4 = File.createTempFile("tempOutputFile4", "")
    try {
      tempFile1.deleteOnExit()
      tempFile2.deleteOnExit()
      tempFile3.deleteOnExit()
      tempFile4.deleteOnExit()

      tempFile1.setLastModified(98765432) // newest file
      tempFile2.setLastModified(12324567) // oldest file
      tempFile3.setLastModified(34567891) // second newest file
      tempFile4.setLastModified(23456789)

      val filterOld = "3-oldest-filesystem"
      val matchFileName = "temp"
      val appArgs = new ProfileArgs(Array(
        "--filter-criteria",
        filterOld,
        "--match-event-logs",
        matchFileName,
        tempFile1.toString,
        tempFile2.toString,
        tempFile3.toString,
        tempFile4.toString
      ))

      val (result, _) = EventLogPathProcessor.processAllPaths(appArgs.filterCriteria.toOption,
        appArgs.matchEventLogs.toOption, appArgs.eventlog(), hadoopConf)
      assert(result.length == 3)
      // Validate 3 oldest files
      assert(result(0).eventLog.getName.equals(tempFile2.getName))
      assert(result(1).eventLog.getName.equals(tempFile4.getName))
      assert(result(2).eventLog.getName.equals(tempFile3.getName))
    } finally {
      tempFile1.delete()
      tempFile2.delete()
      tempFile3.delete()
      tempFile4.delete()
    }
  }

  test("test reading driver log") {
    val driverlog = s"$logDir/driverlog"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--driverlog", driverlog,
        "--output-directory",
        tempDir.getAbsolutePath))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/driver")
      val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
        f.endsWith(".csv")
      })
      assert(dotDirs.length === 1)
      for (file <- dotDirs) {
        assert(file.getAbsolutePath.endsWith(".csv"))
        val df = sparkSession.read.option("header", "true").csv(file.getAbsolutePath)
        val res = df.collect()
        assert(res.nonEmpty)
        val unsupportedHex = df.filter(df("operatorName") === "Hex").count()
        assert(unsupportedHex == 1)
        assert(res.size == 3)
      }
    }
  }

  test("test gds-ucx-parameters") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/gds_ucx_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    val collect = new CollectInformation(apps)
    for (_ <- apps) {
      val rapidsProps = collect.getRapidsProperties
      val rows = rapidsProps.map(_.rows.head)
      assert(rows.length == 5) // 5 properties captured.
      // verify  ucx parameters are captured.
      assert(rows.contains("spark.executorEnv.UCX_RNDV_SCHEME"))

      //verify gds parameters are captured.
      assert(rows.contains("spark.rapids.memory.gpu.direct.storage.spill.alignedIO"))

      val sparkProps = collect.getSparkProperties
      val sparkPropsRows = sparkProps.map(_.rows.head)
      assert(sparkPropsRows.contains("spark.eventLog.dir"))
      assert(sparkPropsRows.contains("spark.plugins"))
    }
  }

  test("test executor info local mode") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/spark2-eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    val collect = new CollectInformation(apps)
    val execInfo = collect.getExecutorInfo
    assert(execInfo.size == 1)
    assert(execInfo.head.numExecutors === 1)
    assert(execInfo.head.maxMem === 384093388L)
  }

  test("test executor info cluster mode") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/tasks_executors_fail_compressed_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    val collect = new CollectInformation(apps)
    val execInfo = collect.getExecutorInfo
    assert(execInfo.size == 1)
    assert(execInfo.head.numExecutors === 8)
    assert(execInfo.head.maxMem === 5538054144L)
  }

  test("test status reports for multiple eventlogs") {
    // Test with normal and malformed eventlogs and verify their status reports
    val bad_eventLog = s"$logDir/malformed_json_eventlog.zstd"
    val eventLog = s"$logDir/rapids_join_eventlog.zstd"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--csv",
        "--output-directory",
        tempDir.getAbsolutePath,
        bad_eventLog,
        eventLog))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)

      // Status counts: 1 SUCCESS, 0 FAILURE, 0 SKIPPED, 1 UNKNOWN
      val expectedStatusCount = StatusReportCounts(1, 0, 0, 1)
      // Compare the expected status counts with the actual status counts from the application
      ToolTestUtils.compareStatusReport(sparkSession, expectedStatusCount,
        s"${tempDir.getAbsolutePath}/rapids_4_spark_profile/profiling_status.csv")
    }
  }

  test("test csv file output with failures") {
    val eventLog = s"$logDir/tasks_executors_fail_compressed_eventlog.zstd"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--csv",
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/application_1603128018386_7846")

      // assert that a file was generated
      val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
        f.endsWith(".csv")
      })
      // compare the number of files generated
      assert(dotDirs.length === 20)
      for (file <- dotDirs) {
        assert(file.getAbsolutePath.endsWith(".csv"))
        // just load each one to make sure formatted properly
        val df = sparkSession.read.option("header", "true").csv(file.getAbsolutePath)
        val res = df.collect()
        assert(res.nonEmpty)
      }

      // Status counts: 1 SUCCESS, 0 FAILURE, 0 SKIPPED, 0 UNKNOWN
      val expectedStatusCount = StatusReportCounts(1, 0, 0, 0)
      // Compare the expected status counts with the actual status counts from the application
      ToolTestUtils.compareStatusReport(sparkSession, expectedStatusCount,
        s"${tempDir.getAbsolutePath}/rapids_4_spark_profile/profiling_status.csv")
    }
  }

  test("test csv file output gpu") {
    val eventLog = s"$qualLogDir/udf_dataset_eventlog"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--csv",
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/local-1651188809790")

      // assert that a file was generated
      val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
        f.endsWith(".csv")
      })
      // compare the number of files generated
      assert(dotDirs.length === 16)
      for (file <- dotDirs) {
        assert(file.getAbsolutePath.endsWith(".csv"))
        // just load each one to make sure formatted properly
        val df = sparkSession.read.option("header", "true").csv(file.getAbsolutePath)
        val res = df.collect()
        assert(res.nonEmpty)
      }

      // Status counts: 1 SUCCESS, 0 FAILURE, 0 SKIPPED, 0 UNKNOWN
      val expectedStatusCount = StatusReportCounts(1, 0, 0, 0)
      // Compare the expected status counts with the actual status counts from the application
      ToolTestUtils.compareStatusReport(sparkSession, expectedStatusCount,
        s"${tempDir.getAbsolutePath}/rapids_4_spark_profile/profiling_status.csv")
    }
  }

  test("test csv file output compare mode") {
    val eventLog1 = s"$logDir/rapids_join_eventlog.zstd"
    val eventLog2 = s"$logDir/rapids_join_eventlog2.zstd"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--csv",
        "-c",
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog1,
        eventLog2))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/compare")

      // assert that a file was generated
      val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
        f.endsWith(".csv")
      })
      // compare the number of files generated
      assert(dotDirs.length === 20)
      for (file <- dotDirs) {
        assert(file.getAbsolutePath.endsWith(".csv"))
        // just load each one to make sure formatted properly
        val df = sparkSession.read.option("header", "true").csv(file.getAbsolutePath)
        val res = df.collect()
        assert(res.nonEmpty)
      }

      // Status counts: 2 SUCCESS, 0 FAILURE, 0 SKIPPED, 0 UNKNOWN
      val expectedStatusCount = StatusReportCounts(2, 0, 0, 0)
      // Compare the expected status counts with the actual status counts from the application
      ToolTestUtils.compareStatusReport(sparkSession, expectedStatusCount,
        s"${tempDir.getAbsolutePath}/rapids_4_spark_profile/profiling_status.csv")
    }
  }

  test("test csv file output combined mode") {
    val eventLog1 = s"$logDir/rapids_join_eventlog.zstd"
    val eventLog2 = s"$logDir/rapids_join_eventlog2.zstd"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--csv",
        "--combined",
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog1,
        eventLog2))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/combined")

      // assert that a file was generated
      val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
        f.endsWith(".csv")
      })
      // compare the number of files generated
      assert(dotDirs.length === 18)
      for (file <- dotDirs) {
        assert(file.getAbsolutePath.endsWith(".csv"))
        // just load each one to make sure formatted properly
        val df = sparkSession.read.option("header", "true").csv(file.getAbsolutePath)
        val res = df.collect()
        assert(res.nonEmpty)
      }

      // Status counts: 2 SUCCESS, 0 FAILURE, 0 SKIPPED, 0 UNKNOWN
      val expectedStatusCount = StatusReportCounts(2, 0, 0, 0)
      // Compare the expected status counts with the actual status counts from the application
      ToolTestUtils.compareStatusReport(sparkSession, expectedStatusCount,
        s"${tempDir.getAbsolutePath}/rapids_4_spark_profile/profiling_status.csv")
    }
  }

  test("test collectionAccumulator") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "collectaccum") { spark =>
        val a = spark.sparkContext.collectionAccumulator[Long]("testCollect")
        val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4))
        // run something to add it to collectionAccumulator
        rdd.foreach(x => a.add(x))
        import spark.implicits._
        rdd.toDF
      }

      TrampolineUtil.withTempDir { collectFileDir =>
        val appArgs = new ProfileArgs(Array(
          "--output-directory",
          collectFileDir.getAbsolutePath,
          eventLog))

        val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
        var index: Int = 1
        val eventlogPaths = appArgs.eventlog()
        for (path <- eventlogPaths) {
          apps += new ApplicationInfo(hadoopConf,
            EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
          index += 1
        }
        // the fact we generated app properly shows we can handle collectionAccumulators
        // right now we just ignore them so nothing else to check
        assert(apps.size == 1)
      }
    }
  }

  test("test gpu reused subquery") {
    val apps = ToolTestUtils.processProfileApps(Array(s"$logDir/nds_q66_gpu.zstd"), sparkSession)
    val collect = new CollectInformation(apps)
    val sqlToStageInfo = collect.getSQLToStage
    val countScanParquet = sqlToStageInfo.flatMap(_.nodeNames).count(_.contains("GpuScan parquet"))
    // There are 12 `GpuScan parquet` raw nodes, but 4 are inside `ReusedExchange`, 1 is inside
    // `ReusedSubquery`, so we expect 7.
    assert(countScanParquet == 7)
  }

  test("test gpu event log with SparkRapidsBuildInfoEvent") {
    TrampolineUtil.withTempDir { tempDir =>
      val eventLogFilePath = Paths.get(tempDir.getAbsolutePath, "test_eventlog")
      // scalastyle:off line.size.limit
      val eventLogContent =
        """{"Event":"SparkListenerLogStart","Spark Version":"3.2.1"}
          |{"Event":"com.nvidia.spark.rapids.SparkRapidsBuildInfoEvent","sparkRapidsBuildInfo":{"url":"https://github.com/NVIDIA/spark-rapids.git","branch":"HEAD","revision":"f932a7802bbf31b6205358d1abd7c7b49c8bea3c","version":"24.06.0","date":"2024-06-13T19:48:28Z","cudf_version":"24.06.0","user":"root"},"sparkRapidsJniBuildInfo":{"url":"https://github.com/NVIDIA/spark-rapids-jni.git","branch":"HEAD","gpu_architectures":"70;75;80;86;90","revision":"e9c92a5339437ce0cd72bc384084bd7ee45b37f9","version":"24.06.0","date":"2024-06-08T01:21:57Z","user":"root"},"cudfBuildInfo":{"url":"https://github.com/rapidsai/cudf.git","branch":"HEAD","gpu_architectures":"70;75;80;86;90","revision":"7c706cc4004d5feaae92544b3b29a00c64f7ed86","version":"24.06.0","date":"2024-06-08T01:21:55Z","user":"root"},"sparkRapidsPrivateBuildInfo":{"url":"https://gitlab-master.nvidia.com/nvspark/spark-rapids-private.git","branch":"HEAD","revision":"755b4dd03c753cacb7d141f3b3c8ff9f83888b69","version":"24.06.0","date":"2024-06-08T11:44:03Z","user":"root"}}
          |{"Event":"SparkListenerApplicationStart","App Name":"GPUMetrics", "App ID":"local-16261043003", "Timestamp":123456, "User":"User1"}
          |{"Event":"SparkListenerTaskEnd","Stage ID":10,"Stage Attempt ID":0,"Task Type":"ShuffleMapTask","Task End Reason":{"Reason":"Success"},"Task Info":{"Task ID":5073,"Index":5054,"Attempt":0,"Partition ID":5054,"Launch Time":1712248533994,"Executor ID":"100","Host":"10.154.65.143","Locality":"PROCESS_LOCAL","Speculative":false,"Getting Result Time":0,"Finish Time":1712253284920,"Failed":false,"Killed":false,"Accumulables":[{"ID":1010,"Name":"gpuSemaphoreWait","Update":"00:00:00.492","Value":"03:13:31.359","Internal":false,"Count Failed Values":true},{"ID":1018,"Name":"gpuSpillToHostTime","Update":"00:00:00.845","Value":"00:29:39.521","Internal":false,"Count Failed Values":true},{"ID":1016,"Name":"gpuSplitAndRetryCount","Update":"1","Value":"2","Internal":false,"Count Failed Values":true}]}}
          |{"Event":"SparkListenerTaskEnd","Stage ID":10,"Stage Attempt ID":0,"Task Type":"ShuffleMapTask","Task End Reason":{"Reason":"Success"},"Task Info":{"Task ID":2913,"Index":2894,"Attempt":0,"Partition ID":2894,"Launch Time":1712248532696,"Executor ID":"24","Host":"10.154.65.135","Locality":"PROCESS_LOCAL","Speculative":false,"Getting Result Time":0,"Finish Time":1712253285639,"Failed":false,"Killed":false,"Accumulables":[{"ID":1010,"Name":"gpuSemaphoreWait","Update":"00:00:00.758","Value":"03:13:32.117","Internal":false,"Count Failed Values":true},{"ID":1015,"Name":"gpuReadSpillFromDiskTime","Update":"00:00:02.599","Value":"00:33:37.153","Internal":false,"Count Failed Values":true},{"ID":1018,"Name":"gpuSpillToHostTime","Update":"00:00:00.845","Value":"00:29:39.521","Internal":false,"Count Failed Values":true}]}}
          |{"Event":"SparkListenerTaskEnd","Stage ID":11,"Stage Attempt ID":0,"Task Type":"ShuffleMapTask","Task End Reason":{"Reason":"Success"},"Task Info":{"Task ID":2045,"Index":2026,"Attempt":0,"Partition ID":2026,"Launch Time":1712248530708,"Executor ID":"84","Host":"10.154.65.233","Locality":"PROCESS_LOCAL","Speculative":false,"Getting Result Time":0,"Finish Time":1712253285667,"Failed":false,"Killed":false,"Accumulables":[{"ID":1010,"Name":"gpuSemaphoreWait","Update":"00:00:00.003","Value":"03:13:32.120","Internal":false,"Count Failed Values":true},{"ID":1015,"Name":"gpuReadSpillFromDiskTime","Update":"00:00:00.955","Value":"00:33:38.108","Internal":false,"Count Failed Values":true}]}}""".stripMargin
      // scalastyle:on line.size.limit
      Files.write(eventLogFilePath, eventLogContent.getBytes(StandardCharsets.UTF_8))

      val appArgs = new ProfileArgs(Array(
        "--csv",
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLogFilePath.toString))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)

      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/local-16261043003")
      // assert that a json file was generated
      val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
        f.endsWith(".json")
      })
      assert(dotDirs.length === 1)

      val actualFilePath = s"${tempSubDir.getAbsolutePath}/spark_rapids_build_info.json"
      val actualResult = FSUtils.readFileContentAsUTF8(actualFilePath)
      val expectedResult =
        s"""|[ {
            |  "sparkRapidsBuildInfo" : {
            |    "url" : "https://github.com/NVIDIA/spark-rapids.git",
            |    "branch" : "HEAD",
            |    "revision" : "f932a7802bbf31b6205358d1abd7c7b49c8bea3c",
            |    "version" : "24.06.0",
            |    "date" : "2024-06-13T19:48:28Z",
            |    "cudf_version" : "24.06.0",
            |    "user" : "root"
            |  },
            |  "sparkRapidsJniBuildInfo" : {
            |    "url" : "https://github.com/NVIDIA/spark-rapids-jni.git",
            |    "branch" : "HEAD",
            |    "gpu_architectures" : "70;75;80;86;90",
            |    "revision" : "e9c92a5339437ce0cd72bc384084bd7ee45b37f9",
            |    "version" : "24.06.0",
            |    "date" : "2024-06-08T01:21:57Z",
            |    "user" : "root"
            |  },
            |  "cudfBuildInfo" : {
            |    "url" : "https://github.com/rapidsai/cudf.git",
            |    "branch" : "HEAD",
            |    "gpu_architectures" : "70;75;80;86;90",
            |    "revision" : "7c706cc4004d5feaae92544b3b29a00c64f7ed86",
            |    "version" : "24.06.0",
            |    "date" : "2024-06-08T01:21:55Z",
            |    "user" : "root"
            |  },
            |  "sparkRapidsPrivateBuildInfo" : {
            |    "url" : "https://gitlab-master.nvidia.com/nvspark/spark-rapids-private.git",
            |    "branch" : "HEAD",
            |    "revision" : "755b4dd03c753cacb7d141f3b3c8ff9f83888b69",
            |    "version" : "24.06.0",
            |    "date" : "2024-06-08T11:44:03Z",
            |    "user" : "root"
            |  }
            |} ]""".stripMargin
      // assert that the spark rapids build info json file is same as expected
      assert(actualResult == expectedResult)
    }
  }

  // TODO: GPU event log run with spark rapids 24.06+ includes SparkRapidsBuildInfoEvent.
  // Once the test event logs are updated, we should remove this test.
  test("test gpu event log with no SparkRapidsBuildInfoEvent") {
    val eventLog = s"$logDir/nds_q66_gpu.zstd"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--csv",
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)

      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/application_1701368813061_0008")
      // assert that a json file was generated
      val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
        f.endsWith(".json")
      })
      assert(dotDirs.length === 1)

      val actualFilePath = s"${tempSubDir.getAbsolutePath}/spark_rapids_build_info.json"
      val actualResult = FSUtils.readFileContentAsUTF8(actualFilePath)
      val expectedResult =
        s"""|[ {
            |  "sparkRapidsBuildInfo" : { },
            |  "sparkRapidsJniBuildInfo" : { },
            |  "cudfBuildInfo" : { },
            |  "sparkRapidsPrivateBuildInfo" : { }
            |} ]""".stripMargin
      // assert that the spark rapids build info json file is same as expected
      assert(actualResult == expectedResult)
    }
  }
}
