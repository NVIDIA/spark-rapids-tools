/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import java.io.File

import com.nvidia.spark.rapids.BaseTestSuite
import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{DataFrame, Dataset, TrampolineUtil}
import org.apache.spark.sql.functions.hex
import org.apache.spark.sql.rapids.tool.qualification.RunningQualificationEventProcessor
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class RunningQualificationSuite extends BaseTestSuite {

  private val csvPerSQLFields = Seq(
    (QualOutputWriter.APP_ID_STR, StringType),
    (QualOutputWriter.ROOT_SQL_ID_STR, StringType),
    (QualOutputWriter.SQL_ID_STR, StringType),
    (QualOutputWriter.SQL_DESC_STR, StringType),
    (QualOutputWriter.SQL_DUR_STR, LongType),
    (QualOutputWriter.GPU_OPPORTUNITY_STR, LongType))

  private val perSQLSchema =
    new StructType(csvPerSQLFields.map(f => StructField(f._1, f._2, nullable = true)).toArray)

  private def readPerSqlFile(expected: File, escape: String = "\\"): DataFrame = {
    ToolTestUtils.readExpectationCSV(sparkSession, expected.getPath, Some(perSQLSchema), escape)
  }

  private def readPerSqlTextFile(expected: File): Dataset[String] = {
    sparkSession.read.textFile(expected.getPath)
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
            for (_ <- 1 to 7) {
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
          assert(dfPerSqlActual.columns.length == 6)
          val rows = dfPerSqlActual.collect()
          assert(rows.length == 2)
          val firstRow = rows(1)
          // , should be replaced with ;
          assert(firstRow(3).toString.contains("count at RunningQualificationSuite.scala"))

          // this reads everything into single column
          val dfPerSqlActualTxt = readPerSqlTextFile(new File(txtOutput0))
          assert(dfPerSqlActualTxt.columns.length == 1)
          val rowsTxt = dfPerSqlActualTxt.collect()
          // have to account for headers
          assert(rowsTxt.length == 6)
          val firstValueRow = rowsTxt(3)
          assert(firstValueRow.contains("QualificationSuite.scala"))
        }
      }
    }
  }

  test("running qualification print unsupported Execs and Exprs") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val qualApp = new RunningQualificationApp()
      val (_, _) = ToolTestUtils.generateEventLog(eventLogDir, "streaming") { spark =>
        val listener = qualApp.getEventListener
        spark.sparkContext.addSparkListener(listener)
        import spark.implicits._
        val df1 = spark.sparkContext.parallelize(List(10, 20, 30, 40)).toDF
        df1.filter(hex($"value") === "A") // hex is not supported in GPU yet.
      }
      // stdout output tests
      val sumOut = qualApp.getSummary()
      val detailedOut = qualApp.getDetailed()
      assert(sumOut.nonEmpty)
      assert(sumOut.startsWith("|") && sumOut.endsWith("|\n"))
      assert(detailedOut.nonEmpty)
      assert(detailedOut.startsWith("|") && detailedOut.endsWith("|\n"))
      val stdOut = sumOut.split("\n")
      val stdOutValues = stdOut(1).split("\\|")
      // index of unsupportedExecs
      val stdOutunsupportedExecs = stdOutValues(stdOutValues.length - 2)
      // index of unsupportedExprs
      val stdOutunsupportedExprs = stdOutValues(stdOutValues.length - 1)
      val expectedstdOutExecs = "Scan unknown;Filter;Se..."
      assert(stdOutunsupportedExecs == expectedstdOutExecs)
      // Exec value is Scan;Filter;SerializeFromObject and UNSUPPORTED_EXECS_MAX_SIZE is 25
      val expectedStdOutExecsMaxLength = 25
      // Expr value is hex and length of expr header is 23 (Unsupported Expressions)
      val expectedStdOutExprsMaxLength = 23
      assert(stdOutunsupportedExecs.length == expectedStdOutExecsMaxLength)
      assert(stdOutunsupportedExprs.length == expectedStdOutExprsMaxLength)
    }
  }

  test("running qualification app join") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val qualApp = new RunningQualificationApp()
      val (_, _) = ToolTestUtils.generateEventLog(eventLogDir, "streaming") { spark =>
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
        assert(csvHeader.contains("App ID,Root SQL ID,SQL ID,SQL Description," +
          "SQL DF Duration,GPU Opportunity"))
        val txtHeader = qualApp.getPerSqlTextHeader
        assert(txtHeader.contains(
          "|             App ID|" +
            "Root SQL ID|SQL ID|                                                              " +
            "                       SQL Description|SQL DF Duration|GPU Opportunity|"))
        val randHeader = qualApp.getPerSqlHeader(";", prettyPrint = true, 20)
        assert(randHeader.contains(";             App ID;" +
          "Root SQL ID;SQL ID;     SQL Description;SQL DF Duration;GPU Opportunity;"))
        val allSQLIds = qualApp.getAvailableSqlIDs
        val numSQLIds = allSQLIds.size
        assert(numSQLIds > 0)
        // We want to pick the last SqlID. Otherwise, we could pick the sqlIds related to Parquet
        // or Json.
        val sqlIdToLookup = allSQLIds.max
        val (csvOut, txtOut) = qualApp.getPerSqlTextAndCSVSummary(sqlIdToLookup)
        assert(csvOut.contains("collect at ToolTestUtils.scala:83") && csvOut.contains(","),
          s"CSV output was: $csvOut")
        assert(txtOut.contains("collect at ToolTestUtils.scala:83") && txtOut.contains("|"),
          s"TXT output was: $txtOut")
        val sqlOut = qualApp.getPerSQLSummary(sqlIdToLookup, ":", prettyPrint = true, 5)
        assert(sqlOut.contains("colle:"), s"SQL output was: $sqlOut")

        // test different delimiter
        val sumOut = qualApp.getSummary(":", prettyPrint = false)
        val rowsSumOut = sumOut.split("\n")
        assert(rowsSumOut.size == 2)
        val headers = rowsSumOut(0).split(":")
        val values = rowsSumOut(1).split(":")
        val appInfo = qualApp.aggregateStats()
        assert(appInfo.nonEmpty)
        assert(headers.size ==
          QualOutputWriter.getSummaryHeaderStringsAndSizes(30, 30).keys.size)
        // the unsupported expression is empty so, it does not appear as an entry in the values.
        assert(values.size == headers.size - 1)
        // 4 should be the SQL DF Duration
        assert(headers(4).contains("SQL DF"))
        assert(values(4).toInt > 0)
        val detailedOut = qualApp.getDetailed(":", prettyPrint = false)
        val rowsDetailedOut = detailedOut.split("\n")
        assert(rowsDetailedOut.size == 2)
        val headersDetailed = rowsDetailedOut(0).split(":").zipWithIndex
        val valuesDetailed = rowsDetailedOut(1).split(":")
        // Check if unsupported write format contains JSON
        headersDetailed.find( entry => entry._1.equals("Unsupported Write Data Format")) match {
          case Some(e) =>
            val ind = e._2
            assert(valuesDetailed(ind).contains("JSON"))
          case _ =>
            fail("Did not file expected column [Unsupported Write Data Format]")
        }
        qualApp.cleanupSQL(sqlIdToLookup)
        assert(qualApp.getAvailableSqlIDs.size == numSQLIds - 1)
      }
    }
  }
}
