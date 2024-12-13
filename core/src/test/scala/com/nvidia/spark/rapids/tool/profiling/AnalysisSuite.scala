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

import com.nvidia.spark.rapids.tool.ToolTestUtils
import com.nvidia.spark.rapids.tool.views.{ProfDataSourceView, RawMetricProfilerView}
import org.scalatest.FunSuite

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

case class TestStageDiagnosticResult(
    appIndex: Int,
    appName: String,
    appId: String,
    stageId: Long,
    duration: Option[Long],
    numTasks: Int,
    memoryBytesSpilledMBMin: Long,
    memoryBytesSpilledMBMed: Long,
    memoryBytesSpilledMBMax: Long,
    memoryBytesSpilledMBSum: Long,
    diskBytesSpilledMBMin: Long,
    diskBytesSpilledMBMed: Long,
    diskBytesSpilledMBMax: Long,
    diskBytesSpilledMBSum: Long,
    inputBytesReadMin: Long,
    inputBytesReadMed: Long,
    inputBytesReadMax: Long,
    inputBytesReadSum: Long,
    outputBytesWrittenMin: Long,
    outputBytesWrittenMed: Long,
    outputBytesWrittenMax: Long,
    outputBytesWrittenSum: Long,
    srTotalBytesReadMin: Long,
    srTotalBytesReadMed: Long,
    srTotalBytesReadMax: Long,
    srTotalBytesReadSum: Long,
    swBytesWrittenMin: Long,
    swBytesWrittenMed: Long,
    swBytesWrittenMax: Long,
    swBytesWrittenSum: Long,
    srFetchWaitTimeMin: Long,
    srFetchWaitTimeMed: Long,
    srFetchWaitTimeMax: Long,
    srFetchWaitTimeSum: Long,
    swWriteTimeMin: Long,
    swWriteTimeMed: Long,
    swWriteTimeMax: Long,
    swWriteTimeSum: Long,
    gpuSemaphoreWaitSum: Long,
    nodeNames: Seq[String])

case class TestIODiagnosticResult(
    appIndex: Int,
    appName: String,
    appId: String,
    sqlId: Long,
    stageId: Long,
    duration: Long,
    nodeId: Long,
    nodeName: String,
    outputRowsMin: Long,
    outputRowsMed: Long,
    outputRowsMax: Long,
    outputRowsSum: Long,
    scanTimeMin: Long,
    scanTimeMed: Long,
    scanTimeMax: Long,
    scanTimeSum: Long,
    outputBatchesMin: Long,
    outputBatchesMed: Long,
    outputBatchesMax: Long,
    outputBatchesSum: Long,
    bufferTimeMin: Long,
    bufferTimeMed: Long,
    bufferTimeMax: Long,
    bufferTimeSum: Long,
    shuffleWriteTimeMin: Long,
    shuffleWriteTimeMed: Long,
    shuffleWriteTimeMax: Long,
    shuffleWriteTimeSum: Long,
    fetchWaitTimeMin: Long,
    fetchWaitTimeMed: Long,
    fetchWaitTimeMax: Long,
    fetchWaitTimeSum: Long,
    gpuDecodeTimeMin: Long,
    gpuDecodeTimeMed: Long,
    gpuDecodeTimeMax: Long,
    gpuDecodeTimeSum: Long)

class AnalysisSuite extends FunSuite {

  private def createTestStageDiagnosticResult(diagnosticsResults: Seq[StageDiagnosticResult]):
      Seq[TestStageDiagnosticResult] = {
    def bytesToMB(numBytes: Long): Long = numBytes / (1024 * 1024)
    def nanoToMilliSec(numNano: Long): Long = numNano / 1000000
    diagnosticsResults.map { result =>
      TestStageDiagnosticResult(
        result.appIndex,
        result.appName,
        result.appId,
        result.stageId,
        result.duration,
        result.numTasks,
        bytesToMB(result.memoryBytesSpilled.min),
        bytesToMB(result.memoryBytesSpilled.median),
        bytesToMB(result.memoryBytesSpilled.max),
        bytesToMB(result.memoryBytesSpilled.total),
        bytesToMB(result.diskBytesSpilled.min),
        bytesToMB(result.diskBytesSpilled.median),
        bytesToMB(result.diskBytesSpilled.max),
        bytesToMB(result.diskBytesSpilled.total),
        result.inputBytesRead.min,
        result.inputBytesRead.median,
        result.inputBytesRead.max,
        result.inputBytesRead.total,
        result.outputBytesWritten.min,
        result.outputBytesWritten.median,
        result.outputBytesWritten.max,
        result.outputBytesWritten.total,
        result.srTotalBytesReadMin,
        result.srTotalBytesReadMed,
        result.srTotalBytesReadMax,
        result.srTotalBytesReadSum,
        result.swBytesWritten.min,
        result.swBytesWritten.median,
        result.swBytesWritten.max,
        result.swBytesWritten.total,
        nanoToMilliSec(result.srFetchWaitTime.min),
        nanoToMilliSec(result.srFetchWaitTime.median),
        nanoToMilliSec(result.srFetchWaitTime.max),
        nanoToMilliSec(result.srFetchWaitTime.total),
        nanoToMilliSec(result.swWriteTime.min),
        nanoToMilliSec(result.swWriteTime.median),
        nanoToMilliSec(result.swWriteTime.max),
        nanoToMilliSec(result.swWriteTime.total),
        result.gpuSemaphoreWait.total,
        result.nodeNames)
    }
  }

  private def createTestIODiagnosticResult(diagnosticsResults: Seq[IODiagnosticResult]):
      Seq[TestIODiagnosticResult] = {
    diagnosticsResults.map {result =>
      TestIODiagnosticResult(
        result.appIndex,
        result.appName,
        result.appId,
        result.sqlId,
        result.stageId,
        result.duration,
        result.nodeId,
        result.nodeName,
        result.outputRows.min,
        result.outputRows.med,
        result.outputRows.max,
        result.outputRows.total,
        result.scanTime.min,
        result.scanTime.med,
        result.scanTime.max,
        result.scanTime.total,
        result.outputBatches.min,
        result.outputBatches.med,
        result.outputBatches.max,
        result.outputBatches.total,
        result.bufferTime.min,
        result.bufferTime.med,
        result.bufferTime.max,
        result.bufferTime.total,
        result.shuffleWriteTime.min,
        result.shuffleWriteTime.med,
        result.shuffleWriteTime.max,
        result.shuffleWriteTime.total,
        result.fetchWaitTime.min,
        result.fetchWaitTime.med,
        result.fetchWaitTime.max,
        result.fetchWaitTime.total,
        result.gpuDecodeTime.min,
        result.gpuDecodeTime.med,
        result.gpuDecodeTime.max,
        result.gpuDecodeTime.total)
    }
  }

  lazy val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }

  private val expRoot = ToolTestUtils.getTestResourceFile("ProfilingExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
  private val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
  // AutoTuner added a field in SQLTaskAggMetricsProfileResult but it is not among the output
  private val skippedColumnsInSqlAggProfile = Seq("inputBytesReadAvg")

  test("test sqlMetricsAggregation simple") {
    val expectFile = (metric: String) => {
      s"rapids_join_eventlog_${metric}metricsagg_expectation.csv"
    }
    testSqlMetricsAggregation(Array(s"$logDir/rapids_join_eventlog.zstd"),
      expectFile("sql"), expectFile("job"), expectFile("stage"))
  }

  test("test sqlMetricsAggregation second single app") {
    val expectFile = (metric: String) => {
      s"rapids_join_eventlog_${metric}metricsagg2_expectation.csv"
    }
    testSqlMetricsAggregation(Array(s"$logDir/rapids_join_eventlog2.zstd"),
      expectFile("sql"), expectFile("job"), expectFile("stage"))
  }

  test("test sqlMetricsAggregation 2 combined") {
    val expectFile = (metric: String) => {
      s"rapids_join_eventlog_${metric}metricsaggmulti_expectation.csv"
    }
    testSqlMetricsAggregation(
      Array(s"$logDir/rapids_join_eventlog.zstd", s"$logDir/rapids_join_eventlog2.zstd"),
      expectFile("sql"), expectFile("job"), expectFile("stage"))
  }

  test("test photon sql metrics aggregation") {
    val fileName = "nds_q88_photon_db_13_3"
    val expectFile = (metric: String) => {
      s"${fileName}_${metric}_metrics_agg_expectation.csv"
    }
    testSqlMetricsAggregation(Array(s"${qualLogDir}/${fileName}.zstd"),
      expectFile("sql"), expectFile("job"), expectFile("stage"))
  }

  test("test stage-level diagnostic metrics") {
    val expectFile = "rapids_join_eventlog_stagediagnosticmetrics_expectation.csv"
    val logs = Array(s"$logDir/rapids_join_eventlog.zstd")
    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    assert(apps.size == logs.size)

    // This step is to compute stage to node names and diagnostic metrics mappings,
    // which is used in collecting diagnostic metrics.
    val collect = new CollectInformation(apps)
    collect.getSQLToStage
    collect.getStageLevelMetrics

    val diagnosticResults = RawMetricProfilerView.getAggMetrics(apps)
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    val actualDf = createTestStageDiagnosticResult(diagnosticResults.stageDiagnostics).toDF.
      withColumn("nodeNames", concat_ws(",", col("nodeNames")))
    compareMetrics(actualDf, expectFile)
  }

  test("test IO diagnostic metrics") {
    val expectFile = "rapids_join_eventlog_iodiagnosticmetrics_expectation.csv"
    val logs = Array(s"$logDir/rapids_join_eventlog.zstd")
    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    assert(apps.size == logs.size)

    val collect = new CollectInformation(apps)
    // Computes IO diagnostic metrics mapping which is later used in getIODiagnosticMetrics
    collect.getSQLPlanMetrics
    val diagnosticResults = collect.getIODiagnosticMetrics

    import sparkSession.implicits._
    val actualDf = createTestIODiagnosticResult(diagnosticResults).toDF
    compareMetrics(actualDf, expectFile)
  }

  private def testSqlMetricsAggregation(logs: Array[String], expectFileSQL: String,
      expectFileJob: String, expectFileStage: String): Unit = {
    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    assert(apps.size == logs.size)
    val aggResults = RawMetricProfilerView.getAggMetrics(apps)
    import sparkSession.implicits._
    // Check the SQL metrics
    val sqlAggsFiltered = aggResults.sqlAggs.toDF.drop(skippedColumnsInSqlAggProfile: _*)
    compareMetrics(sqlAggsFiltered, expectFileSQL)
    // Check the job metrics
    compareMetrics(aggResults.jobAggs.toDF, expectFileJob)
    // Check the stage metrics
    compareMetrics(aggResults.stageAggs.toDF, expectFileStage)
  }

  private def compareMetrics(actualDf: DataFrame, expectFileName: String): Unit = {
    val expectationFile = new File(expRoot, expectFileName)
    val dfExpect = ToolTestUtils.readExpectationCSV(sparkSession, expectationFile.getPath())
    ToolTestUtils.compareDataFrames(actualDf, dfExpect)
  }

  test("test sqlMetrics duration, execute cpu time and potential_problems") {
    val logs = Array(s"$qualLogDir/complex_dec_eventlog.zstd")
    val expectFile = "rapids_duration_and_cpu_expectation.csv"

    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    val aggResults = RawMetricProfilerView.getAggMetrics(apps)
    import sparkSession.implicits._
    val sqlAggDurCpu = aggResults.sqlDurAggs
    val resultExpectation = new File(expRoot, expectFile)
    val schema = new StructType()
      .add("appIndex",IntegerType,true)
      .add("appID",StringType,true)
      .add("rootsqlID",LongType,true)
      .add("sqlID",LongType,true)
      .add("sqlDuration",LongType,true)
      .add("containsDataset",BooleanType,true)
      .add("appDuration",LongType,true)
      .add("potentialProbs",StringType,true)
      .add("executorCpuTime",DoubleType,true)
    val actualDf = sqlAggDurCpu.toDF

    val dfExpect = sparkSession.read.option("header", "true").option("nullValue", "-")
      .schema(schema).csv(resultExpectation.getPath())

    ToolTestUtils.compareDataFrames(actualDf, dfExpect)
  }

  test("test shuffleSkewCheck empty") {
    val apps =
      ToolTestUtils.processProfileApps(Array(s"$logDir/rapids_join_eventlog.zstd"), sparkSession)
    assert(apps.size == 1)

    val aggResults = RawMetricProfilerView.getAggMetrics(apps)
    val shuffleSkewInfo = aggResults.taskShuffleSkew
    assert(shuffleSkewInfo.isEmpty)
  }

  test("test contains dataset false") {
    val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
    val logs = Array(s"$qualLogDir/nds_q86_test")

    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    val aggResults = RawMetricProfilerView.getAggMetrics(apps)
    val sqlDurAndCpu = aggResults.sqlDurAggs
    val containsDs = sqlDurAndCpu.filter(_.containsDataset === true)
    assert(containsDs.isEmpty)
  }

  test("test contains dataset true") {
    val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
    val logs = Array(s"$qualLogDir/dataset_eventlog")

    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    val aggResults = RawMetricProfilerView.getAggMetrics(apps)
    val sqlDurAndCpu = aggResults.sqlDurAggs
    val containsDs = sqlDurAndCpu.filter(_.containsDataset === true)
    assert(containsDs.size == 1)
  }

  test("test duration for null appInfo") {
    val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
    val logs = Array(s"$qualLogDir/dataset_eventlog")

    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    apps.foreach { app =>
      app.appMetaData = None
    }
    val aggResults = RawMetricProfilerView.getAggMetrics(apps)
    val metrics = aggResults.sqlDurAggs
    metrics.foreach(m => assert(m.appDuration.get == 0L))
  }

  test("test photon scan metrics") {
    val fileName = "nds_q88_photon_db_13_3"
    val logs = Array(s"${qualLogDir}/${fileName}.zstd")
    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    val dataSourceResults = ProfDataSourceView.getRawView(apps)
    assert(dataSourceResults.exists(_.scan_time > 0))
  }
}
