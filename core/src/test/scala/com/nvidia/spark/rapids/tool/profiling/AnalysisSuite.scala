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

class AnalysisSuite extends FunSuite {

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

  test("test stage-level diagnostic aggregation simple") {
    val expectFile = "rapids_join_eventlog_stagediagnosticmetricsagg_expectation.csv"
    val logs = Array(s"$logDir/rapids_join_eventlog.zstd")
    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    assert(apps.size == logs.size)

    // This step is to compute stage to node names mapping
    val collect = new CollectInformation(apps)
    collect.getSQLToStage

    val aggResults = RawMetricProfilerView.getAggMetrics(apps)
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    val actualDf = aggResults.stageDiagnostics.toDF.
      withColumn("nodeNames", concat_ws(",", col("nodeNames")))
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
