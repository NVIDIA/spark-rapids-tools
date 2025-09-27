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
package com.nvidia.spark.rapids.tool.profiling

import java.io.File
import java.security.SecureRandom

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.util.FSUtils

class GenerateDotSuite extends AnyFunSuite with BeforeAndAfterAll with Logging {

  override def beforeAll(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
  }

  test("Generate DOT") {
    TrampolineUtil.withTempDir { eventLogDir =>

      val (eventLog, appId) = ToolTestUtils.generateEventLog(eventLogDir, "dot") { spark =>
        import spark.implicits._
        val t1 = Seq((1, 2), (3, 4)).toDF("a", "b")
        t1.createOrReplaceTempView("t1")
        spark.sql("SELECT a, MAX(b) FROM t1 GROUP BY a ORDER BY a")
      }

      // create new session for tool to use
      val spark2 = SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
      val sparkVersion = spark2.version
      TrampolineUtil.withTempDir { dotFileDir =>
        val appArgs = new ProfileArgs(Array(
          "--output-directory",
          dotFileDir.getAbsolutePath,
          "--generate-dot",
          eventLog))
        ProfileMain.mainInternal(appArgs)

        val tempSubDir = new File(dotFileDir, s"${Profiler.SUBDIR}/$appId")
        // assert that a file was generated
        val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
          f.endsWith(".dot")
        })
        assert(dotDirs.length === 2)

        // assert that the generated files looks something like what we expect
        var hashAggCount = 0
        var stageCount = 0
        for (file <- dotDirs) {
          assert(file.getAbsolutePath.endsWith(".dot"))
          val dotFileStr = FSUtils.readFileContentAsUTF8(file.getAbsolutePath)
          assert(dotFileStr.startsWith("digraph G {"))
          assert(dotFileStr.endsWith("}"))
          val hashAggr = "HashAggregate"
          val stageWord = "STAGE"
          hashAggCount += dotFileStr.sliding(hashAggr.length).count(_ == hashAggr)
          stageCount += dotFileStr.sliding(stageWord.length).count(_ == stageWord)
        }
        // expected hash aggregate count varies depending on the spark runtime
        val (expectedHashAggCount, clue) = if (ToolUtils.isSpark320OrLater(sparkVersion)) {
          // 5 in node labels + 5 in graph label
          // == Physical Plan ==
          // AdaptiveSparkPlan (17)
          // +- == Final Plan ==
          //   * Project (11)
          //   +- * Sort (10)
          //      +- AQEShuffleRead (9)
          //         +- ShuffleQueryStage (8), Statistics(sizeInBytes=48.0 B, rowCount=2)
          //            +- Exchange (7)
          //               +- * HashAggregate (6)
          //                  +- AQEShuffleRead (5)
          //                     +- ShuffleQueryStage (4), Statistics(sizeInBytes=48.0 B,rowCount=2)
          //                        +- Exchange (3)
          //                           +- * HashAggregate (2)
          //                              +- * LocalTableScan (1)
          (11, "Expected: 5 in node labels + 5 in graph label")
        } else {
          // 4 in node labels + 4 in graph label
          // == Physical Plan ==
          // TakeOrderedAndProject (5)
          // +- * HashAggregate (4)
          //    +- Exchange (3)
          //       +- * HashAggregate (2)
          //          +- * LocalTableScan (1)
          (8, "Expected: 4 in node labels + 4 in graph label")
        }
        assert(hashAggCount === expectedHashAggCount, clue)
        assert(stageCount === 4, "Expected: UNKNOWN Stage, Initial Aggregation, " +
          "Final Aggregation, Sorting final output")
      }
    }
  }

  test("Empty physical plan") {
    val planLabel = SparkPlanGraph.makeDotLabel(
      appId = "local-12345-1",
      sqlId = "120",
      physicalPlan = "")

    planLabelChecks(planLabel)
  }

  test("Long physical plan") {
    val random = new SecureRandom()
    val seed = System.currentTimeMillis()
    random.setSeed(seed)
    info("Seeding test with: " + seed)
    val numTests = 100
    val lineLengthRange = 50 until 200
    val planLengthSeq = mutable.ArrayBuffer.empty[Int]
    val labelLengthSeq = mutable.ArrayBuffer.empty[Int]

    // some imperfect randomness for edge cases
    for (iter <- 1 to numTests) {
      val lineLength = lineLengthRange.start +
        random.nextInt(lineLengthRange.length) -
        SparkPlanGraph.htmlLineBreak.length()

      val sign = if (random.nextBoolean()) 1 else -1
      val planLength = 16 * 1024 + sign * lineLength * (1 + random.nextInt(5))
      val initPlanStr = (0 to planLength / lineLength).map(_ => "a" * lineLength).mkString("\n")

      // throw some html characters in there to make sure escaped
      val planStr = if (iter == 2) {
        """ReadSchema: struct<_c1:string,_c2:string><br align="left"/>""" + initPlanStr
      } else {
        initPlanStr
      }

      planLengthSeq += planStr.length()

      val planLabel = SparkPlanGraph.makeDotLabel(
        appId = "local-12345-1",
        sqlId = "120",
        physicalPlan = planStr)

      labelLengthSeq += planLabel.length()

      planLabelChecks(planLabel)
      assert(planLabel.length() <= 16 * 1024)
      assert(planLabel.contains("a" * lineLength))
      assert(planLabel.contains(SparkPlanGraph.htmlLineBreak))
    }

    info(s"Plan length summary: min=${labelLengthSeq.min} max=${labelLengthSeq.max}")
    info(s"Plan label summary: min=${planLengthSeq.min} max=${planLengthSeq.max}")
  }

  private def planLabelChecks(planLabel: String): Unit = {
    assert(planLabel.startsWith("<<table "))
    assert(planLabel.endsWith("</table>>"))
    assert(planLabel.contains("local-12345-1"))
    assert(planLabel.contains("Plan for SQL ID : 120"))
  }
}
