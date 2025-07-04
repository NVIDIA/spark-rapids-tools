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

package com.nvidia.spark.rapids.tool.qualification

import java.io.{File, PrintWriter}
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.BaseWithSparkSuite
import com.nvidia.spark.rapids.tool.{EventlogProviderImpl, StatusReportCounts, ToolTestUtils}
import com.nvidia.spark.rapids.tool.planparser.DatabricksParseHelper
import com.nvidia.spark.rapids.tool.qualification.checkers.{QToolOutFileCheckerImpl, QToolResultCoreChecker, QToolStatusChecker, QToolTestCtxtBuilder}
import org.scalatest.Matchers._

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.util.UTF8Source


class QualificationSuite extends BaseWithSparkSuite {
  private def createDecimalFile(spark: SparkSession, dir: String): Unit = {
    import spark.implicits._
    val dfGen = Seq("1.32").toDF("value")
      .selectExpr("CAST(value AS DECIMAL(4, 2)) AS value")
    dfGen.write.parquet(dir)
  }

  private def createIntFile(spark: SparkSession, dir: String): Unit = {
    import spark.implicits._
    val t1 = Seq((1, 2), (3, 4), (1, 6)).toDF("a", "b")
    t1.write.parquet(dir)
  }

  test("potential problems timestamp") {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with timestamp")
          .withAppName("timeZoneStamp")
          .withFunc { (_, spark) =>
            import spark.implicits._
            val testData = Seq((1, 1662519019), (2, 1662519020)).toDF("id", "timestamp")
            spark.sparkContext.setJobDescription("timestamp functions as potential problems")
            testData.createOrReplaceTempView("t1")
            spark.sql(
              "SELECT id, hour(current_timestamp()), second(to_timestamp(timestamp)) FROM t1")
          })
      .withChecker(
        QToolResultCoreChecker("check app count and potential problems")
          .withExpectedSize(1)
          .withSuccessCode()
          .withCheckBlock(
            "TIMEZONE appears in the potential problems",
            qRes => {
              qRes.appSummaries.flatMap(_.potentialProblems) shouldBe
                Array("TIMEZONE hour()", "TIMEZONE current_timestamp()",
                  "TIMEZONE to_timestamp()", "TIMEZONE second()")
            }))
      .withChecker(
        QToolOutFileCheckerImpl("check the content of summary file")
          .withExpectedRows("only 1 row in the summary", 1)
          .withContentVisitor(
            "check that the potential problems is formatted correctly",
            csvContainer => {
              val rowsHead = csvContainer.csvRows.head
              val potentialProblems =
                rowsHead("Potential Problems")
              potentialProblems shouldBe
                "TIMEZONE hour():TIMEZONE current_timestamp():TIMEZONE to_timestamp():" +
                  "TIMEZONE second()"
            }))
      .build()
  }

  test("SMJ not supported on left outer join") {
    // verify that SortMergeJoin is in unsupported operators
    // if there is lower and IN operator in the left join condition
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with SMJ and IN op in the left join")
          .withAppName("smjLeftJoin")
          .withFunc { (_, spark) =>
            import spark.implicits._
            val data = Seq(("A", 20, "M", "2024-01-01"),
              ("B", 25, "M", "2024-12-12"), ("C", 30, "F", "2022-03-04"))
              .toDF("name", "id", "gender", "dob_str")
            data.createOrReplaceTempView("tableA")
            spark.conf.set("spark.rapids.sql.hasExtendedYearValues", "false")
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1")
            spark.sql("SELECT COUNT(*) FROM tableA a LEFT JOIN tableA b ON " +
              "lower(a.id) in ('1','2') AND a.name = b.name")
          })
      .withChecker(
        QToolResultCoreChecker("check app count and that the potential problems")
          .withExpectedSize(1)
          .withSuccessCode())
      .withChecker(
        QToolOutFileCheckerImpl("Unsupported operators should contain SortMergeJoin")
          .withTableLabel("unsupportedOpsCSVReport")
          .withContentVisitor(
            "SMJ appears in the Unsupported Operator column",
            csvF => {
              csvF.getColumn("Unsupported Operator") should contain ("SortMergeJoin")
            }))
      .build()
  }

  test("Generate UDF same") {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with UDF that writes decimal to parquet")
          .withAppName("generateUDFSame")
          .withFunc { (provider, spark) =>
            import org.apache.spark.sql.functions.udf
            val rootDir = provider.rootDir.get
            val tmpParquet = s"$rootDir/decparquet"
            createDecimalFile(spark, tmpParquet)
            val plusOne = udf((x: Int) => x + 1)
            import spark.implicits._
            spark.udf.register("plusOne", plusOne)
            val df = spark.read.parquet(tmpParquet)
            val df2 = df.withColumn("mult", $"value" * $"value")
            val df4 = df2.withColumn("udfcol", plusOne($"value"))
            df4
          })
      .withChecker(
        QToolResultCoreChecker("check app count and potential problems")
          .withExpectedSize(1)
          .withSuccessCode()
          .withCheckBlock(
            "UDF appears in the potential problems",
            qRes => {
              qRes.appSummaries.flatMap(_.potentialProblems) shouldBe
                Array("UDF")
            }))
      .withChecker(
        QToolOutFileCheckerImpl("check the content of summary file")
          .withExpectedRows("only 1 row in the summary", 1)
          .withContentVisitor(
            "check that the potential problems is formatted correctly",
            csvContainer => {
              val rowsHead = csvContainer.csvRows.head
              val potentialProblems =
                rowsHead("Potential Problems")
              potentialProblems shouldBe
                "UDF"
            }))
      .withChecker(
        QToolOutFileCheckerImpl("Unsupported operators should contain UDF")
          .withTableLabel("unsupportedOpsCSVReport")
          .withContentVisitor(
            "Project with the UDF expression appear in the Unsupported Operator column",
            csvF => {
              csvF.getColumn("Unsupported Operator") should contain allOf ("Project", "UDF")
            }))
      .build()
  }

  runConditionalTest("generate udf different sql ops",
    checkUDFDetectionSupportForSpark) {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with UDF that writes decimal to parquet")
          .withAppName("generateUDFDifferentSQLS")
          .withFunc { (provider, spark) =>
            import org.apache.spark.sql.functions.{desc, udf}
            import spark.implicits._
            val rootDir = provider.rootDir.get
            val tmpParquet = s"$rootDir/decparquet"
            val grpParquet = s"$rootDir/grpParquet"
            createDecimalFile(spark, tmpParquet)
            createIntFile(spark, grpParquet)
            val plusOne = udf((x: Int) => x + 1)
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
          })
      .withChecker(
        QToolResultCoreChecker("check app count and potential problems")
          .withExpectedSize(1)
          .withSuccessCode()
          .withCheckBlock(
            "UDF appears in the potential problems",
            qRes => {
              qRes.appSummaries.flatMap(_.potentialProblems) shouldBe
                Array("UDF")
            }))
      .withChecker(
        QToolOutFileCheckerImpl("check the content of summary file")
          .withExpectedRows("only 1 row in the summary", 1)
          .withContentVisitor(
            "check that the potential problems is formatted correctly",
            csvContainer => {
              val rowsHead = csvContainer.csvRows.head
              val potentialProblems =
                rowsHead("Potential Problems")
              potentialProblems shouldBe
                "UDF"
            }))
      .withChecker(
        QToolOutFileCheckerImpl("Unsupported operators should contain UDF")
          .withTableLabel("unsupportedOpsCSVReport")
          .withContentVisitor(
            "Project with the UDF expression appear in the Unsupported Operator column",
            csvF => {
              csvF.getColumn("Unsupported Operator") should contain allOf ("Project", "UDF")
            }))
      .build()
  }

  test("existence join as supported join type") {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with existenceJoin")
          .withAppName("existenceJoin")
          .withFunc { (_, spark) =>
            import spark.implicits._
            val df1 = Seq(("A", 20, 90), ("B", 25, 91), ("C", 30, 94)).toDF("name", "age", "score")
            val df2 = Seq(("A", 15, 90), ("B", 25, 92), ("C", 30, 94)).toDF("name", "age", "score")
            df1.createOrReplaceTempView("tableA")
            df2.createOrReplaceTempView("tableB")
            spark.sql("SELECT * from tableA as l where l.age > 24 or exists" +
              " (SELECT  * from tableB as r where l.age=r.age and l.score <= r.score)")
          })
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(1)
          .withSuccessCode())
      .withChecker(
        QToolOutFileCheckerImpl("Check the list of execs")
          .withTableLabel("execCSVReport")
          .withContentVisitor(
            "ExistenceJoin appears as BroadcastHashJoin in the Report and it is supported",
            csvF => {
              csvF.csvRows.filter { r =>
                r("Exec Name") == "ExistenceJoin" || r("Exec Name") == "BroadcastHashJoin"
              }.foreach {
                r => r("Exec Is Supported").toBoolean shouldBe true
              }
            }))
      .withChecker(
        QToolOutFileCheckerImpl("Unsupported operators does not contain the join execs")
          .withTableLabel("unsupportedOpsCSVReport")
          .withContentVisitor(
            "ExistenceJoin is supported since BroadcastHashJoin is not in unsupported list",
            csvF => {
              csvF.getColumn("Unsupported Operator") should contain noneOf (
                "ExistenceJoin", "BroadcastHashJoin")
            }))
      .build()
  }

  test("CSV qual output with escaped characters") {
    val jobNames = List("test,name", "\"test\"name\"", "\"", ",", ",\"")
    jobNames.foreach { jobName =>
      QToolTestCtxtBuilder()
        .withPerSQL()
        .withEvLogProvider(
          EventlogProviderImpl("create an app with existenceJoin")
            .withAppName(jobName)
            .withFunc { (_, spark) =>
              import spark.implicits._
              val testData = Seq(1, 2).toDF("id")
              spark.sparkContext.setJobDescription(s"run job with problematic name ($jobName)")
              testData.createOrReplaceTempView("t1")
              spark.sql("SELECT id FROM t1")
            })
        .withChecker(
          QToolResultCoreChecker("check app count")
            .withExpectedSize(1)
            .withSuccessCode())
        .withChecker(
          QToolOutFileCheckerImpl("check the content of summary file")
            .withExpectedRows("only 1 row in the summary", 1)
            .withContentVisitor(
              "validate the app_name escapes commas properly",
              csvF => {
                csvF.getColumn("App Name") shouldBe Array(jobName)
              }))
        .withChecker(
          QToolOutFileCheckerImpl("check the content of per-sql file")
            .withTableLabel("perSqlCSVReport")
            .withContentVisitor(
              "validate the app_name escapes commas properly in SQL description",
              csvF => {
                csvF.getColumn("SQL Description") should contain
                s"run job with problematic name ($jobName)"
              }))
        .build()
    }
  }

  test("custom reasons for operators disabled by default") {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with json op")
          .withAppName("generateJson")
          .withFunc { (provider, spark) =>
            import org.apache.spark.sql.functions.to_json
            import spark.implicits._
            val rootDir = provider.rootDir.get
            val outParquetFile = s"$rootDir/person_info"
            val data = Seq((1, ("Person1", 30)), (2, ("Person2", 25))).toDF("id", "person")
            data.write.parquet(s"$outParquetFile/person_info")
            val df = spark.read.parquet(s"$outParquetFile/person_info")
            df.withColumn("person_json", to_json($"person"))
          })
      .withChecker(
        QToolOutFileCheckerImpl("Unsupported operators should contain to_json")
          .withTableLabel("unsupportedOpsCSVReport")
          .withContentVisitor(
            "there should be at least 1 to_json expr",
            csvf => {
              csvf.csvRows.find { r =>
                r("Unsupported Operator").contains("to_json")
              } match {
                case Some(r) =>
                  r("Details") should include ("This is disabled by default")
                case _ =>
                  fail("Unsupported ops should include a row to_json")
              }
            }))
      .build()
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
        // 59 = "{"Event":"SparkListenerTaskEnd",
        // 60 = "{"Event":"SparkListenerStageCompleted"
        // 61 = "{"Event":"SparkListenerJobEnd","Job ID":5,"Completion Time":1718401564645,"
        // 62 = "{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd","
        // 63 = "{"Event":"SparkListenerApplicationEnd","Timestamp":1718401564663}"
        val tailLines = allEventLines.takeRight(5)
        val selectedLines: List[String] = allEventLines.dropRight(5)
        // scalastyle:off println
        selectedLines.foreach { line =>
          pwList.foreach(pw => pw.println(line))
        }
        // scalastyle:on println
        for (i <- tailLines.indices) {
          if (i == 0) {
            // add truncatedTaskEvent to the brokenEventlog
            // scalastyle:off println
            pwList(2).println(tailLines(i).substring(0, 59))
            // scalastyle:on println
          }
          // Write all the lines to the unfinishedLog and incompleteLog.
          // We do not want to ApplicationEnd in the incompleteLog
          val startListInd = if (i == tailLines.length - 1) {
            1 // index of unfinished
          } else {
            0 // index of incomplete
          }
          for (lIndex <- startListInd to 1) {
            // scalastyle:off println
            pwList(lIndex).println(tailLines(i))
            // scalastyle:on println
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
      // Status counts: 4 SUCCESS, 0 FAILURE, 0 UNKNOWN
      val logFiles = Array(
        eventLog,
        incompleteLog.getAbsolutePath,
        unfinishedLog.getAbsolutePath,
        brokenEvLog.getAbsolutePath)
      // test qualification one file at a time to avoid merging results as a single app
      for (evLogFile <- logFiles) {
        QToolTestCtxtBuilder(Array(evLogFile))
          .withChecker(QToolResultCoreChecker("check app count")
            .withExpectedSize(1)
            .withSuccessCode())
          .withChecker(
            QToolStatusChecker("Check that the app statuses are valid")
              .withExpectedCounts(StatusReportCounts(1, 0, 0, 0)))
          .build()
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
        QToolTestCtxtBuilder()
          .withEvLogProvider(
            EventlogProviderImpl("create an app with Hive Support")
              .withHiveEnabled()
              .withSparkConfigs(sparkConfs)
              // set the name to "hiv3" on purpose to avoid any matches on "hive".
              .withAppName("scanHiv3App")
              .withFunc { (_, spark) =>
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
              })
          .withChecker(QToolResultCoreChecker("check app count")
            .withExpectedSize(1)
            .withSuccessCode()
            .withCheckBlock(
              "verify the fields in the app summary. unsupported reads is empty",
              qRes => {
                val unsupReadFormats =
                  qRes.appSummaries.flatMap(_.readFileFormatAndTypesNotSupported)
                unsupReadFormats shouldBe empty
              }))
          .withChecker(
            QToolOutFileCheckerImpl("The unsupported Ops should not have hive")
              .withTableLabel("unsupportedOpsCSVReport")
              .withContentVisitor(
                "no scan appears in unsupported operator",
                csvf => {
                  csvf.csvRows.filter { r =>
                    r("Unsupported Operator").contains("hive")
                  } shouldBe empty
                }))
          .withChecker(
            QToolOutFileCheckerImpl("The exec file contains supported scan hive")
              .withTableLabel("execCSVReport")
              .withContentVisitor(
                "scan hive has supported flag set to true",
                csvf => {
                  csvf.csvRows.find { r =>
                    r("Exec Name").startsWith("Scan hivetext")
                  } match {
                    case Some(r) =>
                      // the row is found
                      r("Exec Is Supported") shouldBe "true"
                    case _ =>
                      fail("Exec Scan hivetext was not found in Exec report")
                  }
                }))
          .build()
      }
    }
  }

  test("stage reuse") {
    // TODO: Need to revisit that unit test to make more valid assertions to the output
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with existenceJoin")
          .withAppName("existenceJoin")
          .withFunc { (_, spark) =>
            import spark.implicits._
            val df = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
            val df2 = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
            val j1 = df.select( $"value" as "a")
              .join(df2.select($"value" as "b"), $"a" === $"b").cache()
            j1.count()
            j1.union(j1).count()
            // count above is an important thing, here we just make up small df to return
            spark.sparkContext.makeRDD(1 to 2).toDF
          })
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(1)
          .withSuccessCode())
      .build()
  }

  test("clusterTags configs") {
    val allTagsConfVal =
      """[{"key":"Vendor",
        |"value":"Databricks"},{"key":"Creator","value":"abc@company.com"},
        |{"key":"ClusterName","value":"job-215-run-1"},{"key":"ClusterId",
        |"value":"0617-131246-dray530"},{"key":"JobId","value":"215"},
        |{"key":"RunName","value":"test73longer"},{"key":"DatabricksEnvironment",
        |"value":"workerenv-7026851462233806"}]""".stripMargin
    val tagConfs = Map(DatabricksParseHelper.PROP_ALL_TAGS_KEY -> allTagsConfVal)
    // no need to pass the platform databricks as we want to make sure that this is handled by
    // parsing the eventlog
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with Databricks Cluster configs")
          .withHiveEnabled()
          .withSparkConfigs(tagConfs)
          .withAppName("DBClusterTags")
          .withFunc { (_, spark) =>
            import spark.implicits._
            val df1 = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
            df1.sample(0.1)
          })
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(1)
          .withSuccessCode())
      .withChecker(
        QToolOutFileCheckerImpl("check the content of the clusters_tags.csv")
          .withTableLabel("clusterTagsCSVReport")
          .withContentVisitor(
            "check the properties and values are as expected",
            csvf => {
              val expectedTags = Map(
                "ClusterId"-> "0617-131246-dray530",
                "JobId" -> "215",
                "RunName" -> "test73longer"
              )
              csvf.csvRows.collect {
                case r if expectedTags.keySet.contains(r("Property Name")) =>
                  r("Property Name") -> r("Property Value")
              }.toMap shouldBe expectedTags
            }))
      .build()
  }

  test("clusterTags redacted") {
    // test that redacted allcluster tags won't block extraction of clustertags
    val tagConfs =
      Map(DatabricksParseHelper.PROP_ALL_TAGS_KEY -> "*********(redacted)",
        DatabricksParseHelper.PROP_TAG_CLUSTER_ID_KEY -> "0617-131246-dray530",
        DatabricksParseHelper.PROP_TAG_CLUSTER_NAME_KEY -> "job-215-run-34243234")
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with Databricks Cluster configs")
          .withHiveEnabled()
          .withSparkConfigs(tagConfs)
          .withAppName("clustertagsRedacted")
          .withFunc { (_, spark) =>
            import spark.implicits._
            val df1 = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
            df1.sample(0.1)
          })
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(1)
          .withSuccessCode())
      .withChecker(
        QToolOutFileCheckerImpl("check the content of the clusters_tags.csv")
          .withTableLabel("clusterTagsCSVReport")
          .withContentVisitor(
            "check the properties and values are as expected. Property 'RunName' should be None",
            csvf => {
              val expectedTags = Map(
                "ClusterId"-> "0617-131246-dray530",
                "JobId" -> "215"
              )
              val allKeys = expectedTags.keySet ++ Set("RunName")
              csvf.csvRows.collect {
                case r if allKeys.contains(r("Property Name")) =>
                  r("Property Name") -> r("Property Value")
              }.toMap shouldBe expectedTags
            }))
      .build()
  }

  test("sql metric agg") {
    val listener = new ToolTestListener
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with existenceJoin")
          .withAppName("sqlmetricAgg")
          .withFunc { (_, spark) =>
            spark.sparkContext.addSparkListener(listener)
            import spark.implicits._
            val testData = Seq((1, 2), (3, 4)).toDF("a", "b")
            spark.sparkContext.setJobDescription("testing, csv delimiter, replacement")
            testData.createOrReplaceTempView("t1")
            testData.createOrReplaceTempView("t2")
            spark.sql("SELECT a, MAX(b) FROM (SELECT t1.a, t2.b " +
              "FROM t1 JOIN t2 ON t1.a = t2.a) AS t " +
              "GROUP BY a ORDER BY a")
          })
      .withPerSQL()
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(1)
          .withSuccessCode()
          .withCheckBlock(
            "Compare results to the Listener total",
            qRes => {
              val executorCpuTime =
                NANOSECONDS.toMillis(listener.executorCpuTime) // in milliseconds
              val executorRunTime = listener.completedStages // in milliseconds
                .map(_.stageInfo.taskMetrics.executorRunTime).sum
              val listenerCpuTimePercent =
                ToolUtils.calculateDurationPercent(executorCpuTime, executorRunTime)
              // compare metrics from event log with metrics from listener
              qRes.appSummaries.head.executorCpuTimePercent should equal(listenerCpuTimePercent)
            }))
      .withChecker(
        QToolOutFileCheckerImpl("check the content of per-sql file")
          .withTableLabel("perSqlCSVReport")
          .withExpectedRows(
            "3 rows in the sqlID",
            3)
          .withContentVisitor(
            "validate that sql description is correctly formatted",
            csvF => {
              csvF.getColumn("SQL Description") should contain (
                "testing, csv delimiter, replacement")
            }))
      .build()
  }

  test("unsupported expressions and execs") {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with existenceJoin")
          .withAppName("appWithExpressionHex")
          .withFunc { (_, spark) =>
            import org.apache.spark.sql.functions.hex
            import spark.implicits._
            val df1 = spark.sparkContext.parallelize(List(10, 20, 30, 40)).toDF
            df1.filter(hex($"value") === "A") // hex is not supported in GPU yet.
          })
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(1)
          .withSuccessCode())
      .withChecker(
        QToolOutFileCheckerImpl("Unsupported operators should contain hex")
          .withTableLabel("unsupportedOpsCSVReport")
          .withContentVisitor(
            "hex appears in the Unsupported Operator column",
            csvF => {
              csvF.getColumn("Unsupported Operator").count(c => c.contains("hex")) shouldBe 1
            }))
      .withChecker(
        QToolOutFileCheckerImpl("Check the list of execs unsupported(filter, scan, and serialize)")
          .withTableLabel("execCSVReport")
          .withContentVisitor(
            "ExistenceJoin appears as BroadcastHashJoin in the Report and it is supported",
            csvF => {
              val unsupportedExecs = csvF.csvRows.filterNot { r =>
                r("Exec Is Supported").toBoolean
              }
              unsupportedExecs.map(_("Exec Name")) should equal (
                Array("WholeStageCodegen", "Filter", "SerializeFromObject", "Scan unknown"))
            }))
      .build()
  }

  test("qualification app join") {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with existenceJoin")
          .withAppName("appWithExpressionHex")
          .withFunc { (_, spark) =>
            import spark.implicits._
            val testData = Seq((1, 2), (3, 4)).toDF("a", "b")
            testData.createOrReplaceTempView("t1")
            testData.createOrReplaceTempView("t2")
            spark.sql("SELECT a, MAX(b) FROM (SELECT t1.a, t2.b " +
              "FROM t1 JOIN t2 ON t1.a = t2.a) AS t " +
              "GROUP BY a ORDER BY a")
          })
      .withChecker(
        QToolOutFileCheckerImpl("Check the numeric fields")
          .withContentVisitor(
            "verify numeric columns are within valid range",
            csvF => {
              // cpu percentage 0-100
              csvF.getColumn("Executor CPU Time Percent")
                .count(v => v.toDouble > 100.0) shouldBe 0
            }))
      .build()
  }

  test("test sparkML") {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with ML and parquet")
          .withAppName("mlOpsParquet")
          .withFunc { (provider, spark) =>
            import org.apache.spark.ml.feature.PCA
            import org.apache.spark.ml.linalg.Vectors
            val rootDir = provider.rootDir.get
            val tmpParquet = s"$rootDir/mlOpsParquet"
            createDecimalFile(spark, tmpParquet)
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
          })
      .withToolArgs(Array("--ml-functions"))
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(1)
          .withSuccessCode())
      .withChecker(
        QToolOutFileCheckerImpl("Check the MLFunctions report")
          .withTableLabel("mlFunctionsCSVReport")
          .withContentVisitor(
            "check the columns to count the stageIDs and the functionNames",
            csvF => {
              // Spark3.2.+ generates a plan with 6 stages. StageID 3 and 4 are both
              // "isEmpty at RowMatrix.scala:441"
              val expStageCount = if (ToolUtils.isSpark320OrLater()) 6 else 5
              csvF.getColumn("Stage ID") should have size expStageCount
              csvF.getColumn("ML Functions").count { mlFn =>
                mlFn.contains("org.apache.spark.ml.feature.PCA.fit")
              } shouldBe expStageCount
            }))
      .withChecker(
        QToolOutFileCheckerImpl("Check the MLFunctionsDurations report")
          .withTableLabel("mlFunctionsDurationsCSVReport")
          .withContentVisitor(
            "ml_functions_durations should contain the correct function name PCA",
            csvF => {
              // Spark3.2.+ generates a plan with 6 stages. StageID 3 and 4 are both
              // "isEmpty at RowMatrix.scala:441"
              val expStageCount = if (ToolUtils.isSpark320OrLater()) 6 else 5
              csvF.getColumn("Stage Ids").mkString.split(";") should have size expStageCount
              csvF.getColumn("ML Function Name") shouldBe Array("PCA")
            }))
      .build()
  }
}

class ToolTestListener extends SparkListener {
  val completedStages = new ListBuffer[SparkListenerStageCompleted]()
  var executorCpuTime = 0L

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    executorCpuTime += taskEnd.taskMetrics.executorCpuTime // in nanoseconds
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    completedStages.append(stageCompleted)
  }
}
