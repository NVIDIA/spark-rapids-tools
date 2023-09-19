/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser

import java.io.{File, PrintWriter}

import scala.collection.mutable
import scala.io.Source
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.BaseTestSuite
import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, ToolTestUtils}
import com.nvidia.spark.rapids.tool.qualification._
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.sql.TrampolineUtil
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ceil, col, collect_list, count, explode, floor, hex, json_tuple, round, row_number, sum, translate}
import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil


class SQLPlanParserSuite extends BaseTestSuite {

  private val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
  private val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")

  private def assertSizeAndNotSupported(size: Int, execs: Seq[ExecInfo]): Unit = {
    for (t <- Seq(execs)) {
      assert(t.size == size, t)
      assert(t.forall(_.speedupFactor == 1), t)
      assert(t.forall(_.isSupported == false), t)
      assert(t.forall(_.children.isEmpty), t)
      assert(t.forall(_.duration.isEmpty), t)
    }
  }

  private def assertSizeAndSupported(size: Int, execs: Seq[ExecInfo],
    expectedDur: Seq[Option[Long]] = Seq.empty, extraText: String = ""): Unit = {
    for (t <- Seq(execs)) {
      assert(t.size == size, s"$extraText $t")
      assert(t.forall(_.isSupported == true), s"$extraText $t")
      assert(t.forall(_.children.isEmpty), s"$extraText $t")
      if (expectedDur.nonEmpty) {
        val durations = t.map(_.duration)
        val foo = durations.diff(expectedDur)
        assert(durations.diff(expectedDur).isEmpty,
          s"$extraText durations differ expected ${expectedDur.mkString(",")} " +
            s"but got ${durations.mkString(",")}")
      } else {
        assert(t.forall(_.duration.isEmpty), s"$extraText $t")
      }
    }
  }

  private def createAppFromEventlog(eventLog: String): QualificationAppInfo = {
    val hadoopConf = RapidsToolsConfUtil.newHadoopConf()
    val (_, allEventLogs) = EventLogPathProcessor.processAllPaths(
      None, None, List(eventLog), hadoopConf)
    val pluginTypeChecker = new PluginTypeChecker()
    assert(allEventLogs.size == 1)
    val appResult = QualificationAppInfo.createApp(allEventLogs.head, hadoopConf,
      pluginTypeChecker, reportSqlLevel = false, mlOpsEnabled = false, ignoreTransitions = false)
    appResult match {
      case Right(app) => app
      case Left(_) => throw new AssertionError("Cannot create application")
    }
  }

  private def getAllExecsFromPlan(plans: Seq[PlanInfo]): Seq[ExecInfo] = {
    val topExecInfo = plans.flatMap(_.execInfo)
    topExecInfo.flatMap { e =>
      e.children.getOrElse(Seq.empty) :+ e
    }
  }

  test("Error parser does not cause entire app to fail") {
    // The purpose of this test is to make sure that the SQLParser won't trigger an exception that
    // causes the entire app analysis to fail.
    // In order to simulate unexpected scenarios, the test modifies the eventlog on the fly by
    // injecting faulty expressions.
    //
    // For example:
    //    Filter (((value#8 <> 100) AND (value#8 > 50)) OR (value#8 = 0))
    //    One of the predicates is converted from "<" to "<>".
    //    The latter is an invalid predicate causing the parser to throw a scala-match error.
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
      // create a temporary file to write the modified events
      val faultyEventlog = new File(s"$eventLogDir/faulty_eventlog")
      val pWriter = new PrintWriter(faultyEventlog)
      val bufferedSource = Source.fromFile(eventLog)
      try {
        bufferedSource.getLines.map( l =>
          if (l.contains("SparkListenerSQLExecutionStart")) {
            l.replaceAll("value#2 <", "value#2 <>")
          } else {
            l
          }
        ).foreach(modifiedLine => pWriter.println(modifiedLine))
      } finally {
        bufferedSource.close()
        pWriter.close()
      }
      // start processing the faulty eventlog.
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(faultyEventlog.getAbsolutePath)
      assert(app.sqlPlans.size == 1)
      try {
        app.sqlPlans.foreach { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "",
            pluginTypeChecker, app)
        }
      } catch {
        case NonFatal(e) =>
          throw new TestFailedException(
            s"The SQLParser crashed while processing incorrect expression", e, 0)
      }
    }
  }

  test("WholeStage with Filter, Project, Sort and SortMergeJoin") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "WholeStageFilterProject") { spark =>
        import spark.implicits._
        val df = spark.sparkContext.makeRDD(1 to 100000, 6).toDF
        val df2 = spark.sparkContext.makeRDD(1 to 100000, 6).toDF
        df.select( $"value" as "a")
          .join(df2.select($"value" as "b"), $"a" === $"b")
          .filter($"b" < 100)
          .sort($"b")
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      val sparkVersion = app.sparkVersion
      assert(app.sqlPlans.size == 1)
      app.sqlPlans.foreach { case (sqlID, plan) =>
        val planInfo = SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "",
          pluginTypeChecker, app)

        val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
        val allChildren = wholeStages.flatMap(_.children).flatten
        val sorts = allChildren.filter(_.exec == "Sort")
        val filters = allChildren.filter(_.exec == "Filter")
        val projects = allChildren.filter(_.exec == "Project")
        val smj = allChildren.filter(_.exec == "SortMergeJoin")
        val bhj = allChildren.filter(_.exec == "BroadcastHashJoin")
        val (execInfoSize, wholeStagesSize, numSupported, expChildren, numSort, numSMJ, numBHJ) =
          if (ToolUtils.isSpark320OrLater(sparkVersion)) {
            // - The plan has BroadcastExchange which will change the expected number of execs.
            //   Keeping in mind that the parser marks duplicate execs as "shouldRemove" instead of
            //   deleting them
            // - All "wholestages" should all supported
            // - The children of wholestages will be down by 2 compared to pre-3.2
            //    Sort
            //    BroadcastHashJoin
            //    Project
            //    Filter
            //    SerializeFromObject
            //    Project
            //    Filter
            //    SerializeFromObject
            (14, 4, 4, 8, 1, 0, 1)
          } else {
            // only 2 in the above example have projects and filters, 3 have sort and 1 has SMJ
            // - The children of wholestages will be:
            //    Sort
            //    SortMergeJoin
            //    Sort
            //    Project
            //    Filter
            //    SerializeFromObject
            //    Sort
            //    Project
            //    Filter
            //    SerializeFromObject
            (11, 6, 6, 10, 3, 1, 0)
          }
        assert(planInfo.execInfo.size == execInfoSize)
        assert(wholeStages.size == wholeStagesSize)
        assert(wholeStages.filter(_.isSupported).size == numSupported)
        assert(wholeStages.forall(_.duration.nonEmpty))
        assert(allChildren.size == expChildren)
        assertSizeAndSupported(2, filters)
        assertSizeAndSupported(2, projects)
        assertSizeAndSupported(numSort, sorts)
        assertSizeAndSupported(numSMJ, smj)
        assertSizeAndSupported(numBHJ, bhj)
      }
    }
  }

  test("Parse Execs within WholeStageCodeGen in Order") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "Execs within WSCG ") { spark =>
        import spark.implicits._
        val df = Seq(("foo", 1L, 1.2), ("foo", 2L, 2.2), ("bar", 2L, 3.2),
          ("bar", 2L, 4.2)).toDF("x", "y", "z")
        df.cube($"x", ceil($"y")).count
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      app.sqlPlans.foreach { case (sqlID, plan) =>
        val planInfo = SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "",
          pluginTypeChecker, app)
        val allExecInfo = planInfo.execInfo
        val expectedAllExecInfoSize = if (ToolUtils.isSpark320OrLater()) {
          // AdaptiveSparkPlan, WholeStageCodegen, AQEShuffleRead, Exchange, WholeStageCodegen
          5
        } else {
          // WholeStageCodegen, Exchange, WholeStageCodegen
          3
        }
        val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 2)
        // Expanding the children of WholeStageCodegen
        val allExecs = allExecInfo.map(x => if (x.exec.startsWith("WholeStage")) {
          x.children.getOrElse(Seq.empty)
        } else {
          Seq(x)
        }).flatten.reverse
        val expectedOrder = if (ToolUtils.isSpark320OrLater()) {
          // Order should be: LocalTableScan, Expand, HashAggregate, Exchange,
          // AQEShuffleRead, HashAggregate, AdaptiveSparkPlan
          Seq("LocalTableScan", "Expand", "HashAggregate", "Exchange", "AQEShuffleRead",
            "HashAggregate", "AdaptiveSparkPlan")
        } else {
          // Order should be: LocalTableScan, Expand, HashAggregate, Exchange, HashAggregate
          Seq("LocalTableScan", "Expand", "HashAggregate", "Exchange", "HashAggregate")
        }
        assert(allExecs.map(_.exec) == expectedOrder)
      }
    }
  }

  test("HashAggregate") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "sqlMetric") { spark =>
        import spark.implicits._
        spark.range(10).
            groupBy('id % 3 as "group").agg(sum("id") as "sum")
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      app.sqlPlans.foreach { case (sqlID, plan) =>
        val planInfo = SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "",
          pluginTypeChecker, app)
        val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 2)
        val numSupported = wholeStages.filter(_.isSupported).size
        assert(numSupported == 2)
        assert(wholeStages.forall(_.duration.nonEmpty))
        val allChildren = wholeStages.flatMap(_.children).flatten
        val hashAggregate = allChildren.filter(_.exec == "HashAggregate")
        assertSizeAndSupported(2, hashAggregate)
      }
    }
  }

  test("FileSourceScan") {
    val eventLog = s"$profileLogDir/eventlog_dsv1.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size == 7)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    val json = allExecInfo.filter(_.exec.contains("Scan json"))
    val orc = allExecInfo.filter(_.exec.contains("Scan orc"))
    val parquet = allExecInfo.filter(_.exec.contains("Scan parquet"))
    val text = allExecInfo.filter(_.exec.contains("Scan text"))
    val csv = allExecInfo.filter(_.exec.contains("Scan csv"))
    assertSizeAndNotSupported(2, json.toSeq)
    assertSizeAndNotSupported(1, text.toSeq)
    for (t <- Seq(parquet, csv)) {
      assertSizeAndSupported(1, t.toSeq)
    }
    assertSizeAndSupported(2, orc.toSeq)
  }

  test("BatchScan") {
    val eventLog = s"$profileLogDir/eventlog_dsv2.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size == 9)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    // Note that the text scan from this file is v1 so ignore it
    val json = allExecInfo.filter(_.exec.contains("BatchScan json"))
    val orc = allExecInfo.filter(_.exec.contains("BatchScan orc"))
    val parquet = allExecInfo.filter(_.exec.contains("BatchScan parquet"))
    val csv = allExecInfo.filter(_.exec.contains("BatchScan csv"))
    assertSizeAndNotSupported(3, json.toSeq)
    assertSizeAndSupported(1, csv.toSeq)
    for (t <- Seq(orc, parquet)) {
      assertSizeAndSupported(2, t.toSeq)
    }
  }

  test("InsertIntoHadoopFsRelationCommand") {
    val dataWriteCMD = DataWritingCommandExecParser.insertIntoHadoopCMD
    TrampolineUtil.withTempDir { outputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, dataWriteCMD) { spark =>
          import spark.implicits._
          val df = spark.sparkContext.makeRDD(1 to 10000, 6).toDF
          val dfWithStrings = df.select(col("value").cast("string"))
          dfWithStrings.write.text(s"$outputLoc/testtext")
          df.write.parquet(s"$outputLoc/testparquet")
          df.write.orc(s"$outputLoc/testorc")
          df.write.json(s"$outputLoc/testjson")
          df.write.csv(s"$outputLoc/testcsv")
          df
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        assert(app.sqlPlans.size == 6)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
        }
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val text = allExecInfo.filter(_.exec.contains(s"$dataWriteCMD text"))
        val json = allExecInfo.filter(_.exec.contains(s"$dataWriteCMD json"))
        val orc = allExecInfo.filter(_.exec.contains(s"$dataWriteCMD orc"))
        val parquet =
          allExecInfo.filter(_.exec.contains(s"$dataWriteCMD parquet"))
        val csv = allExecInfo.filter(_.exec.contains(s"$dataWriteCMD csv"))
        for (t <- Seq(json, csv, text)) {
          assertSizeAndNotSupported(1, t.toSeq)
        }
        for (t <- Seq(orc, parquet)) {
          assertSizeAndSupported(1, t.toSeq)
        }
      }
    }
  }

  test("CreateDataSourceTableAsSelectCommand") {
    // using event log to not deal with enabling hive support
    val eventLog = s"$qualLogDir/createdatasourcetable_eventlog.zstd"
    val app = createAppFromEventlog(eventLog)
    val pluginTypeChecker = new PluginTypeChecker()
    assert(app.sqlPlans.size == 1)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "",
        pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    val parquet = {
      allExecInfo.filter(_.exec.contains("CreateDataSourceTableAsSelectCommand"))
    }
    assertSizeAndNotSupported(1, parquet.toSeq)
  }

  test("Stages and jobs failure") {
    val eventLog = s"$profileLogDir/tasks_executors_fail_compressed_eventlog.zstd"
    val app = createAppFromEventlog(eventLog)
    val stats = app.aggregateStats()
    assert(stats.nonEmpty)
    val recommendation = stats.get.estimatedInfo.recommendation
    assert(recommendation.equals("Not Applicable"))
  }

  test("InMemoryTableScan") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "InMemoryTableScan") { spark =>
        import spark.implicits._
        val df = spark.sparkContext.makeRDD(1 to 10000, 6).toDF
        val dfc = df.cache()
        dfc
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val tableScan = allExecInfo.filter(_.exec == ("InMemoryTableScan"))
      assertSizeAndSupported(1, tableScan.toSeq)
    }
  }

  test("BroadcastExchangeExec, SubqueryBroadcastExec and Exchange") {
    val eventLog = s"$qualLogDir/nds_q86_test"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size > 0)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    val broadcasts = allExecInfo.filter(_.exec == "BroadcastExchange")
    assertSizeAndSupported(3, broadcasts.toSeq,
      expectedDur = Seq(Some(1154), Some(1154), Some(1855)))
    val subqueryBroadcast = allExecInfo.filter(_.exec == "SubqueryBroadcast")
    assertSizeAndSupported(1, subqueryBroadcast.toSeq, expectedDur = Seq(Some(1175)))
    val exchanges = allExecInfo.filter(_.exec == "Exchange")
    assertSizeAndSupported(2, exchanges.toSeq, expectedDur = Seq(Some(15688), Some(8)))
  }

  test("ReusedExchangeExec") {
    // The eventlog has a ReusedExchange that causes multiple nodes to be duplicate.
    // This unit test is to check that we set the duplicate nodes as shouldRemove
    // to avoid adding them multiple times to the speedups.
    val eventLog = s"$qualLogDir/nds_q86_test"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    // reusedExchange is added as a supportedExec
    val reusedExchangeExecs = allExecInfo.filter(_.exec == "ReusedExchange")
    assertSizeAndSupported(1, reusedExchangeExecs)
    val nodesWithRemove = allExecInfo.filter(_.shouldRemove)
    // There are 7 nodes that are duplicated including 1 ReusedExchangeExec node and
    // 1 WholeStageCodegen.
    assert(1 == nodesWithRemove.count(
      exec => exec.expr.contains("WholeStageCodegen")))
    assert(1 == nodesWithRemove.count(exec => exec.exec.equals("ReusedExchange")))
  }

  test("CustomShuffleReaderExec") {
    // this is eventlog because CustomShuffleReaderExec only available before 3.2.0
    val eventLog = s"$qualLogDir/customshuffle_eventlog.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size > 0)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    val reader = allExecInfo.filter(_.exec == "CustomShuffleReader")
    assertSizeAndSupported(2, reader.toSeq)
  }

  test("AQEShuffleReadExec") {
    // this is eventlog because AQEShuffleReadExec only available after 3.2.0
    val eventLog = s"$qualLogDir/aqeshuffle_eventlog.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size > 0)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    val reader = allExecInfo.filter(_.exec == "AQEShuffleRead")
    assertSizeAndSupported(2, reader.toSeq)
  }

  test("Parsing various Execs - Coalesce, CollectLimit, Expand, Range, Sample" +
      "TakeOrderedAndProject and Union") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val df1 = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
        df1.coalesce(1).collect // Coalesce
        spark.range(10).where(col("id") === 2).collect // Range
        // TakeOrderedAndProject
        df1.orderBy($"value", ceil($"value"), round($"value")).limit(10).collect
        df1.limit(10).collect // CollectLimit
        df1.union(df1).collect // Union
        df1.rollup(ceil(col("value"))).agg(ceil(col("value"))).collect // Expand
        df1.sample(0.1) // Sample
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 7)
      val supportedExecs = Array("Coalesce", "Expand", "Range", "Sample",
        "TakeOrderedAndProject", "Union")
      val unsupportedExecs = Array("CollectLimit")
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      for (execName <- supportedExecs) {
        val execs = allExecInfo.filter(_.exec == execName)
        assertSizeAndSupported(1, execs.toSeq, expectedDur = Seq.empty, extraText = execName)
      }
      for (execName <- unsupportedExecs) {
        val execs = allExecInfo.filter(_.exec == execName)
        assertSizeAndNotSupported(1, execs.toSeq)
      }
    }
  }

  test("Parse Execs - CartesianProduct and Generate") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val genDf = spark.sparkContext.parallelize(List(List(1, 2, 3), List(4, 5, 6)), 4).toDF
        val joinDf1 = spark.sparkContext.makeRDD(1 to 10, 4).toDF
        val joinDf2 = spark.sparkContext.makeRDD(1 to 10, 4).toDF
        genDf.select(explode($"value")).collect
        joinDf1.crossJoin(joinDf2)
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 2)
      val supportedExecs = Array("CartesianProduct", "Generate")
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      for (execName <- supportedExecs) {
        val supportedExec = allExecInfo.filter(_.exec == execName)
        assertSizeAndSupported(1, supportedExec.toSeq)
      }
    }
  }

  test("Parse Execs and supported exprs - BroadcastHashJoin, " +
    "BroadcastNestedLoopJoin and ShuffledHashJoin") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val df1 = Seq((1, "ABC", 1.2), (2, "DEF", 2.3), (3, "GHI", 1.4)).toDF(
          "emp_id", "name", "dept")
        val df2 = Seq(("Fin", 1.2, "ABC"), ("IT", 2.3, "DEF")).toDF(
          "dept_name", "dept_id", "name")
        // BroadcastHashJoin
        df1.join(df2, df1("name") === df2("name") &&
          ceil(df1("dept")) === ceil(df2("dept_id")), "left_outer").collect
        // ShuffledHashJoin
        df1.createOrReplaceTempView("t1")
        df2.createOrReplaceTempView("t2")
        spark.sql("select /*+ SHUFFLE_HASH(t1) */ * from t1 " +
          "INNER JOIN t2 ON t1.name = t2.name AND CEIL(t1.DEPT) = CEIL(t2.DEPT_ID)").collect
        // BroadcastNestedLoopJoin
        df1.join(df2, ceil(df1("dept")) <= ceil(df2("dept_id"))
          && floor(df1("dept")) <= floor(df2("dept_id")), "inner")
      }

      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 5)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val bhj = allExecInfo.filter(_.exec == "BroadcastHashJoin")
      assertSizeAndSupported(1, bhj)
      val broadcastNestedJoin = allExecInfo.filter(_.exec == "BroadcastNestedLoopJoin")
      assertSizeAndSupported(1, broadcastNestedJoin)
      val shj = allExecInfo.filter(_.exec == "ShuffledHashJoin")
      assertSizeAndSupported(1, shj)
    }
  }

  test("Expressions not supported in  BroadcastHashJoin, BroadcastNestedLoopJoin " +
    "and ShuffledHashJoin") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val df1 = Seq((1, "ABC", 1.2), (2, "DEF", 2.3), (3, "GHI", 1.4)).toDF(
          "emp_id", "name", "dept")
        val df2 = Seq(("Fin", 1.2, "ABC"), ("IT", 2.3, "DEF")).toDF(
          "dept_name", "dept_id", "name")
        // BroadcastHashJoin
        // hex is not supported in GPU yet.
        df1.join(df2, df1("name") === df2("name") &&
          hex(df1("dept")) === hex(df2("dept_id")), "left_outer").collect
        // ShuffledHashJoin
        df1.createOrReplaceTempView("t1")
        df2.createOrReplaceTempView("t2")
        spark.sql("select /*+ SHUFFLE_HASH(t1) */ * from t1 " +
          "INNER JOIN t2 ON t1.name = t2.name AND HEX(t1.DEPT) = HEX(t2.DEPT_ID)").collect
        // BroadcastNestedLoopJoin
        df1.join(df2, ceil(df1("dept")) <= ceil(df2("dept_id"))
          && hex(df1("dept")) <= hex(df2("dept_id")), "inner")
      }

      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 5)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val bhj = allExecInfo.filter(_.exec == "BroadcastHashJoin")
      assertSizeAndNotSupported(1, bhj)
      val broadcastNestedJoin = allExecInfo.filter(_.exec == "BroadcastNestedLoopJoin")
      assertSizeAndNotSupported(1, broadcastNestedJoin)
      val shj = allExecInfo.filter(_.exec == "ShuffledHashJoin")
       assertSizeAndNotSupported(1, shj)
    }
  }

  test("Expressions not supported in SortMergeJoin") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val broadcastConfs = Map("spark.sql.autoBroadcastJoinThreshold" -> "-1")
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric",
        Some(broadcastConfs)) { spark =>
        import spark.implicits._
        val df1 = Seq((1, "ABC", 1.2), (2, "DEF", 2.3), (3, "GHI", 1.4)).toDF(
          "emp_id", "name", "dept")
        val df2 = Seq(("Fin", 1.2, "ABC"), ("IT", 2.3, "DEF")).toDF(
          "dept_name", "dept_id", "name")
        // hex is not supported in GPU yet.
        df1.join(df2, df1("name") === df2("name") &&
          hex(df1("dept")) === hex(df2("dept_id")), "left_outer")
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val smj = allExecInfo.filter(_.exec == "SortMergeJoin")
      assertSizeAndNotSupported(1, smj)
    }
  }

  test("Parse Exec - SortAggregate") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val aggConfs = Map("spark.sql.execution.useObjectHashAggregateExec" -> "false")
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric",
        Some(aggConfs)) { spark =>
        import spark.implicits._
        val df1 = Seq((1, "a"), (1, "aa"), (1, "a"), (2, "b"),
                         (2, "b"), (3, "c"), (3, "c")).toDF("num", "letter")
        df1.groupBy("num").agg(collect_list("letter").as("collected_letters"))
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val execInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val sortAggregate = execInfo.filter(_.exec == "SortAggregate")
      assertSizeAndSupported(2, sortAggregate)
    }
  }

  test("Parse Exec - ObjectHashAggregate") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val df1 = Seq((1, "a"), (1, "aa"), (1, "a"), (2, "b"),
          (2, "b"), (3, "c"), (3, "c")).toDF("num", "letter")
        df1.groupBy("num").agg(collect_list("letter").as("collected_letters"))
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val execInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val objectHashAggregate = execInfo.filter(_.exec == "ObjectHashAggregate")
      assertSizeAndSupported(2, objectHashAggregate)
    }
  }

  test("WindowExec and expressions within WIndowExec") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val metrics = Seq(
          (0, 0, 0), (1, 0, 1), (2, 5, 2), (3, 0, 3), (4, 0, 1), (5, 5, 3), (6, 5, 0)
        ).toDF("id", "device", "level")
        val rangeWithTwoDevicesById = Window.partitionBy('device).orderBy('id)
        metrics.withColumn("sum", sum('level) over rangeWithTwoDevicesById)
            .withColumn("row_number", row_number.over(rangeWithTwoDevicesById))
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val execInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val windowExecs = execInfo.filter(_.exec == "Window")
      assertSizeAndSupported(1, windowExecs)
    }
  }

  test("Parse Pandas execs - AggregateInPandas, ArrowEvalPython, " +
      "FlatMapGroupsInPandas, MapInPandas, WindowInPandas") {
    val eventLog = s"$qualLogDir/pandas_execs_eventlog.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size > 0)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    val flatMapGroups = allExecInfo.filter(_.exec == "FlatMapGroupsInPandas")
    assertSizeAndSupported(1, flatMapGroups)
    val aggregateInPandas = allExecInfo.filter(_.exec == "AggregateInPandas")
    assertSizeAndSupported(1, aggregateInPandas)
    // this event log had UDF for ArrowEvalPath so shows up as not supported
    val arrowEvalPython = allExecInfo.filter(_.exec == "ArrowEvalPython")
    assertSizeAndSupported(1, arrowEvalPython)
    val mapInPandas = allExecInfo.filter(_.exec == "MapInPandas")
    assertSizeAndSupported(1, mapInPandas)
    // WindowInPandas configured off by default
    val windowInPandas = allExecInfo.filter(_.exec == "WindowInPandas")
    assertSizeAndNotSupported(1, windowInPandas)
  }

  // GlobalLimit and LocalLimit is not in physical plan when collect is called on the dataframe.
  // We are reading from static eventlogs to test these execs.
  test("Parse execs - LocalLimit and GlobalLimit") {
    val eventLog = s"$qualLogDir/global_local_limit_eventlog.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size == 1)
    val supportedExecs = Array("GlobalLimit", "LocalLimit")
    app.sqlPlans.foreach { case (sqlID, plan) =>
      val planInfo = SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      // GlobalLimit and LocalLimit are inside WholeStageCodegen. So getting the children of
      // WholeStageCodegenExec
      val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
      val allChildren = wholeStages.flatMap(_.children).flatten
      for (execName <- supportedExecs) {
        val supportedExec = allChildren.filter(_.exec == execName)
        assertSizeAndSupported(1, supportedExec)
      }
    }
  }

  test("Expression not supported in FilterExec") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "Expressions in FilterExec") { spark =>
        import spark.implicits._
        val df1 = spark.sparkContext.parallelize(List(10, 20, 30, 40)).toDF
        df1.filter(hex($"value") === "A") // hex is not supported in GPU yet.
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      app.sqlPlans.foreach { case (sqlID, plan) =>
        val planInfo = SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "",
          pluginTypeChecker, app)
        val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 1)
        val allChildren = wholeStages.flatMap(_.children).flatten
        val filters = allChildren.filter(_.exec == "Filter")
        assertSizeAndNotSupported(1, filters)
      }
    }
  }

  test("Expression not supported in Expand") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "Expressions in ExpandExec") { spark =>
        import spark.implicits._
        val df = Seq(("foo", 1L, 1.2), ("foo", 2L, 2.2), ("bar", 2L, 3.2),
          ("bar", 2L, 4.2)).toDF("x", "y", "z")
        df.cube($"x", ceil($"y"), hex($"z")).count // hex is not supported in GPU yet.
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      app.sqlPlans.foreach { case (sqlID, plan) =>
        val planInfo = SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "",
          pluginTypeChecker, app)
        val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 2)
        val allChildren = wholeStages.flatMap(_.children).flatten
        val filters = allChildren.filter(_.exec == "Expand")
        assertSizeAndNotSupported(1, filters)
      }
    }
  }

  test("Expression not supported in TakeOrderedAndProject") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "Expressions in TakeOrderedAndProject") { spark =>
        import spark.implicits._
        val df = Seq(("foo", 1L, 1.2), ("foo", 2L, 2.2), ("bar", 2L, 3.2),
          ("bar", 2L, 4.2)).toDF("x", "y", "z")
        df.orderBy(hex($"y"), $"z").limit(2) // hex is not supported in GPU yet.
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val execInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val takeOrderedAndProject = execInfo.filter(_.exec == "TakeOrderedAndProject")
      assertSizeAndNotSupported(1, takeOrderedAndProject)
    }
  }

  test("Expression not supported in Generate") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "Expressions in Generate") { spark =>
        import spark.implicits._
        val jsonString =
          """{"Zipcode":123,"ZipCodeType":"STANDARD",
            |"City":"ABCDE","State":"YZ"}""".stripMargin
        val data = Seq((1, jsonString))
        val df = data.toDF("id", "jsonValues")
        //json_tuple which is called from GenerateExec is not supported in GPU yet.
        df.select(col("id"), json_tuple(col("jsonValues"), "Zipcode", "ZipCodeType", "City"))
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val execInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val generateExprs = execInfo.filter(_.exec == "Generate")
      assertSizeAndNotSupported(1, generateExprs)
    }
  }

  test("Expressions supported in SortAggregateExec") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val aggConfs = Map("spark.sql.execution.useObjectHashAggregateExec" -> "false")
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric",
        Some(aggConfs)) { spark =>
        import spark.implicits._
        val df1 = Seq((1, "a"), (1, "aa"), (1, "a"), (2, "b"),
          (2, "b"), (3, "c"), (3, "c")).toDF("num", "letter")
        df1.groupBy("num").agg(collect_list("letter").as("collected_letters"),
          count("letter").as("letter_count"))
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val execInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val sortAggregate = execInfo.filter(_.exec == "SortAggregate")
      assertSizeAndSupported(2, sortAggregate)
    }
  }

  test("Expressions supported in SortExec") {
    TrampolineUtil.withTempDir { parquetoutputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "ProjectExprsSupported") { spark =>
          import spark.implicits._
          val df1 = Seq((1.7, "a"), (1.6, "aa"), (1.1, "b"), (2.5, "a"), (2.2, "b"),
            (3.2, "a"), (10.6, "c")).toDF("num", "letter")
          df1.write.parquet(s"$parquetoutputLoc/testsortExec")
          val df2 = spark.read.parquet(s"$parquetoutputLoc/testsortExec")
          df2.sort("num").collect
          df2.orderBy("num").collect
          df2.select(round(col("num")), col("letter")).sort(round(col("num")), col("letter").desc)
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        assert(app.sqlPlans.size == 4)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
        }
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val sortExec = allExecInfo.filter(_.exec.contains("Sort"))
        assert(sortExec.size == 3)
        assertSizeAndSupported(3, sortExec)
      }
    }
  }

  test("Expressions supported in ProjectExec") {
    TrampolineUtil.withTempDir { parquetoutputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "ProjectExprsSupported") { spark =>
          import spark.implicits._
          import org.apache.spark.sql.types.StringType
          val df1 = Seq(9.9, 10.2, 11.6, 12.5).toDF("value")
          df1.write.parquet(s"$parquetoutputLoc/testtext")
          val df2 = spark.read.parquet(s"$parquetoutputLoc/testtext")
          df2.select(df2("value").cast(StringType), ceil(df2("value")), df2("value"))
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        assert(app.sqlPlans.size == 2)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
        }
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val wholeStages = allExecInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 1)
        assert(wholeStages.forall(_.duration.nonEmpty))
        val allChildren = wholeStages.flatMap(_.children).flatten
        val projects = allChildren.filter(_.exec == "Project")
        assertSizeAndSupported(1, projects)
      }
    }
  }

  test("Expressions not supported in ProjectExec") {
    TrampolineUtil.withTempDir { parquetoutputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "ProjectExprsNotSupported") { spark =>
          import spark.implicits._
          val df1 = spark.sparkContext.parallelize(List(10, 20, 30, 40)).toDF
          df1.write.parquet(s"$parquetoutputLoc/testtext")
          val df2 = spark.read.parquet(s"$parquetoutputLoc/testtext")
          df2.select(hex($"value") === "A")
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        assert(app.sqlPlans.size == 2)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "test desc", pluginTypeChecker, app)
        }
        parsedPlans.foreach { pInfo =>
          assert(pInfo.sqlDesc == "test desc")
        }
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val wholeStages = allExecInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.forall(_.duration.nonEmpty))
        val allChildren = wholeStages.flatMap(_.children).flatten
        val projects = allChildren.filter(_.exec == "Project")
        assertSizeAndNotSupported(1, projects)
      }
    }
  }

  test("translate is supported in ProjectExec") {
    TrampolineUtil.withTempDir { parquetoutputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "ProjectExprsSupported") { spark =>
          import spark.implicits._
          val df1 = Seq("", "abc", "ABC", "AaBbCc").toDF("value")
          // write df1 to parquet to transform LocalTableScan to ProjectExec
          df1.write.parquet(s"$parquetoutputLoc/testtext")
          val df2 = spark.read.parquet(s"$parquetoutputLoc/testtext")
          // translate should be part of ProjectExec
          df2.select(translate(df2("value"), "ABC", "123"))
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        assert(app.sqlPlans.size == 2)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
        }
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val wholeStages = allExecInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 1)
        assert(wholeStages.forall(_.duration.nonEmpty))
        val allChildren = wholeStages.flatMap(_.children).flatten
        val projects = allChildren.filter(_.exec == "Project")
        assertSizeAndSupported(1, projects)
      }
    }
  }

  test("Timestamp functions supported in ProjectExec") {
    TrampolineUtil.withTempDir { parquetoutputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "ProjectExprsSupported") { spark =>
          import spark.implicits._
          val init_df = Seq((1230219000123123L, 1230219000123L, 1230219000.123))
          val df1 = init_df.toDF("micro", "millis", "seconds")
          df1.write.parquet(s"$parquetoutputLoc/testtext")
          val df2 = spark.read.parquet(s"$parquetoutputLoc/testtext")
          df2.selectExpr("timestamp_micros(micro)", "timestamp_millis(millis)",
                         "timestamp_seconds(seconds)")
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        assert(app.sqlPlans.size == 2)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
        }
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val wholeStages = allExecInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 1)
        assert(wholeStages.forall(_.duration.nonEmpty))
        val allChildren = wholeStages.flatMap(_.children).flatten
        val projects = allChildren.filter(_.exec == "Project")
        assertSizeAndSupported(1, projects)
      }
    }
  }

  test("Parse SQL function Name in HashAggregateExec") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val df1 = Seq((1, "a"), (1, "aa"), (1, "a"), (2, "b"),
          (2, "b"), (3, "c"), (3, "c")).toDF("num", "letter")
        // Average is Expression name and `avg` is SQL function name.
        // SQL function name is in the eventlog as shown below.
        // HashAggregate(keys=[letter#187], functions=[avg(cast(num#186 as bigint))])
        df1.groupBy("letter").avg("num")
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val execInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val hashAggregate = execInfo.filter(_.exec == "HashAggregate")
      assertSizeAndSupported(2, hashAggregate)
    }
  }

  test("Parsing Conditional Expressions") {
    // scalastyle:off line.size.limit
    val expressionsMap: mutable.HashMap[String, Array[String]] = mutable.HashMap(
      "(((lower(partition_act#90) = moduleview) && (isnotnull(productarr#22) && NOT (productarr#22=[]))) || (lower(moduletype#13) = saveforlater))" ->
        Array("lower", "isnotnull", "EqualTo", "And",  "Not", "Or"),
      "(IsNotNull(c_customer_id))" ->
        Array("IsNotNull"),
      "(isnotnull(names#15) AND StartsWith(names#15, OR))" ->
        Array("isnotnull", "And", "StartsWith"),
      "((isnotnull(s_state#68) AND (s_state#68 = TN)) OR (hex(cast(value#0 as bigint)) = B))" ->
        Array("isnotnull", "And", "Or", "hex", "EqualTo"),
      // Test that AND followed by '(' without space can be parsed
      "((isnotnull(s_state#68) AND(s_state#68 = TN)) OR (hex(cast(value#0 as bigint)) = B))" ->
        Array("isnotnull", "And", "Or", "hex", "EqualTo"),
      "(((isnotnull(d_year#498) AND isnotnull(d_moy#500)) AND (d_year#498 = 1998)) AND (d_moy#500 = 12))" ->
        Array("isnotnull", "And", "EqualTo"),
      "IsNotNull(d_year) AND IsNotNull(d_moy) AND EqualTo(d_year,1998) AND EqualTo(d_moy,12)" ->
        Array("IsNotNull", "And", "EqualTo"),
      // check that a predicate with a single variable name is fine
      "flagVariable" ->
        Array(),
      // check that a predicate with a single function call
      "isnotnull(c_customer_sk#412)" ->
        Array("isnotnull"),
      "((substr(ca_zip#457, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) OR ca_state#456 IN (CA,WA,GA)) OR (cs_sales_price#20 > 500.00))" ->
        Array("substr", "In", "Or", "GreaterThan"),
      // test the operator is at the beginning of expression and not followed by space
      "NOT(isnotnull(d_moy))" ->
        Array("Not", "isnotnull")
    )
    // scalastyle:on line.size.limit
    for ((condExpr, expectedExpression) <- expressionsMap) {
      val parsedExpressionsMine = SQLPlanParser.parseConditionalExpressions(condExpr)
      val currOutput = parsedExpressionsMine.sorted
      val expectedOutput = expectedExpression.sorted
      assert(currOutput sameElements expectedOutput,
        s"The parsed expressions are not as expected. Expression: ${condExpr}, " +
          s"Expected: ${expectedOutput.mkString}, " +
          s"Output: ${currOutput.mkString}")
    }
  }

  runConditionalTest("SaveIntoDataSourceCommand is supported",
    checkDeltaLakeSparkRelease) {
    // This unit test enables deltaLake which generate SaveIntoDataSourceCommand
    // It is ignored for Spark-340+ because delta releases are not available.
    TrampolineUtil.withTempDir { outputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val dataWriteCMD = DataWritingCommandExecParser.saveIntoDataSrcCMD
        val deltaConfs = Map(
          "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
          ("spark.sql.catalog.spark_catalog" ->
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "DeltaLakeWrites", Some(deltaConfs)) { spark =>
          import spark.implicits._
          val df = spark.sparkContext.makeRDD(1 to 10000, 6).toDF
          df.write.format("delta").mode("overwrite").save(s"$outputLoc/testdelta")
          df
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
        }
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val deltaLakeWrites = allExecInfo.filter(_.exec.contains(s"$dataWriteCMD"))
        assertSizeAndSupported(1, deltaLakeWrites)
      }
    }
  }

  test("Parse conditional expression does not list ignored expressions") {
    val exprString = "((((((isnotnull(action#191L) AND isnotnull(content#192)) " +
      "AND isnotnull(content_name_16#197L)) AND (action#191L = 0)) AND NOT (content#192 = )) " +
      "AND (content_name_16#197L = 1)) AND NOT (split(split(split(replace(replace(replace" +
      "(replace(trim(replace(cast(unbase64(content#192) as string),  , ), Some( )), *., ), *, ), " +
      "https://, ), http://, ), /, -1)[0], :, -1)[0], \\?, -1)[0] = ))"
    val expected = Array("isnotnull", "split", "replace", "trim", "unbase64", "And",
      "EqualTo", "Not")
    val expressions = SQLPlanParser.parseFilterExpressions(exprString)
    expressions should ===(expected)
  }


  test("Parse aggregate expressions") {
    val exprString = "(keys=[], functions=[split(split(split(replace(replace(replace(replace(" +
      "trim(replace(cast(unbase64(content#192) as string),  , ), Some( )), *., ), *, ), " +
      "https://, ), http://, ), /, -1)[0], :, -1)[0], \\?, -1)[0]#199, " +
      "CASE WHEN (instr(replace(cast(unbase64(content#192) as string),  , ), *) = 0) " +
      "THEN concat(replace(cast(unbase64(content#192) as string),  , ), %) " +
      "ELSE replace(replace(replace(cast(unbase64(content#192) as string),  , ), %, " +
      "\\%), *, %) END#200])"
    val expected = Array("replace", "concat", "instr", "split", "trim", "unbase64")
    val expressions = SQLPlanParser.parseAggregateExpressions(exprString)
    expressions should ===(expected)
  }

  runConditionalTest("promote_precision is supported for Spark LT 3.4.0: issue-517",
    ignoreExprForSparkGTE340) {
    // Spark-3.4.0 removed the promote_precision SQL function
    // the SQL generates the following physical plan
    // (1) Project [CheckOverflow((promote_precision(cast(dec1#24 as decimal(13,2)))
    //     + promote_precision(cast(dec2#25 as decimal(13,2)))), DecimalType(13,2))
    //     AS (dec1 + dec2)#30]
    // For Spark3.4.0, the promote_precision was removed from the plan.
    // (1) Project [(dec1#24 + dec2#25) AS (dec1 + dec2)#30]
    TrampolineUtil.withTempDir { parquetoutputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "projectPromotePrecision") { spark =>
          import spark.implicits._
          import org.apache.spark.sql.types.DecimalType
          val df = Seq(("12347.21", "1234154"), ("92233.08", "1")).toDF
            .withColumn("dec1", col("_1").cast(DecimalType(7, 2)))
            .withColumn("dec2", col("_2").cast(DecimalType(10, 0)))
          // write the df to parquet to transform localTableScan to projectExec
          df.write.parquet(s"$parquetoutputLoc/testPromotePrecision")
          val df2 = spark.read.parquet(s"$parquetoutputLoc/testPromotePrecision")
          df2.selectExpr("dec1+dec2")
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
        }
        // The promote_precision should be part of the project exec.
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val projExecs = allExecInfo.filter(_.exec.contains("Project"))
        assertSizeAndSupported(1, projExecs)
      }
    }
  }

  test("current_database is not listed as unsupported: issue-547") {
    // current_database is a scalar value that does not cause any CPU fallbacks
    // we should not list it as unsupported expression if it appears in the SQL.
    TrampolineUtil.withTempDir { parquetoutputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "ignoreCurrentDatabase") { spark =>
          import spark.implicits._
          val df1 = spark.sparkContext.parallelize(List(10, 20, 30, 40)).toDF
          df1.write.parquet(s"$parquetoutputLoc/ignore_current_database")
          val df2 = spark.read.parquet(s"$parquetoutputLoc/ignore_current_database")
          // Note that the current_database will show up in project only if it is used as a column
          // name. For example:
          // > SELECT current_database(), current_database() as my_db;
          //   +------------------+-------+
          //   |current_database()|  my_db|
          //   +------------------+-------+
          //   |           default|default|
          //   |           default|default|
          //   |           default|default|
          //   |           default|default|
          //   +------------------+-------+
          //   == Physical Plan ==
          //   *(1) Project [default AS current_database()#10, default AS my_db#9]
          //   +- *(1) ColumnarToRow
          //      +- FileScan parquet [] Batched: true, DataFilters: [], Format: Parquet, Location:
          //         InMemoryFileIndex(1 paths)[file:/tmp_folder/T/toolTest..., PartitionFilters: []
          //         PushedFilters: [], ReadSchema: struct<>
          df2.selectExpr("current_database()","current_database() as my_db")
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
        }
        // The current_database should be part of the project-exec and the parser should ignore it.
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val projExecs = allExecInfo.filter(_.exec.contains("Project"))
        assertSizeAndSupported(1, projExecs)
      }
    }
  }

  test("ArrayBuffer should be ignored in Expand: issue-554") {
    // ArrayBuffer appears in some SQL queries.
    // It is a non-Spark expression and it should not be considered as SQL-Function
    val exprString = "[" +
      "ArrayBuffer(cast((cast(ds#1241L as double) / 100.0) as bigint), null, null, 0," +
      "            cast(if ((ret_type#1236 = 2)) 1 else 0 as bigint))," +
      "ArrayBuffer(cast((cast(ds#1241L as double) / 100.0) as bigint), wuid#1234, null, 1, null)," +
      "ArrayBuffer(cast((cast(ds#1241L as double) / 100.0) as bigint), null," +
      "            if ((if ((ret_type#1236 = 2)) 1 else 0 = 1)) wuid#1234 else null, 2, null)," +
      "[" +
      "CAST((CAST(supersql_t12.`ds` AS DOUBLE) / 100.0D) AS BIGINT)#1297L," +
      "supersql_t12.`wuid`#1298," +
      "(IF(((IF((supersql_t12.`ret_type` = 2), 1, 0)) = 1), supersql_t12.`wuid`," +
      "CAST(NULL AS STRING)))#1299," +
      "gid#1296," +
      "CAST((IF((supersql_t12.`ret_type` = 2), 1, 0)) AS BIGINT)#1300L]]"
    // Only "IF" should be picked up as a function name
    val expected = Array("IF")
    val expressions = SQLPlanParser.parseExpandExpressions(exprString)
    expressions should ===(expected)
  }
}
