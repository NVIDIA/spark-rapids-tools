/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.tuning

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.{PlatformFactory, PlatformNames}
import com.nvidia.spark.rapids.tool.profiling.Profiler
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.TableFor3

import org.apache.spark.sql.rapids.tool.util.PropertiesLoader

/**
 * Suite to test the Qualification Tool's AutoTuner
 */
class QualificationAutoTunerSuite extends BaseAutoTunerSuite {

  val autoTunerConfigsProvider: AutoTunerConfigsProvider = QualificationAutoTunerConfigsProvider

  /**
   * Default Spark properties to be used when building the Qualification AutoTuner
   */
  private def defaultSparkProps: mutable.Map[String, String] = {
    mutable.LinkedHashMap[String, String](
      "spark.executor.cores" -> "32",
      "spark.executor.instances" -> "1",
      "spark.executor.memory" -> "80g",
      "spark.executor.instances" -> "1"
    )
  }

  /**
   * Helper method to build a worker info string with CPU properties
   */
  protected def buildCpuWorkerInfoAsString(
       customProps: Option[mutable.Map[String, String]] = None,
       numCores: Option[Int] = Some(32),
       systemMemory: Option[String] = Some("122880MiB"),
       numWorkers: Option[Int] = Some(4)): String = {
    buildWorkerInfoAsString(customProps, numCores, systemMemory, numWorkers)
  }

  /**
   * Helper method to return an instance of the Qualification AutoTuner with default properties
   */
  private def buildDefaultAutoTuner(
      logEventsProps: mutable.Map[String, String] = defaultSparkProps): AutoTuner = {
    val workerInfo = buildCpuWorkerInfoAsString(None, Some(32),
      Some("212992MiB"), Some(5))
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some(testSparkVersion))
    val clusterPropsOpt = PropertiesLoader[ClusterProperties].loadFromContent(workerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.EMR, clusterPropsOpt)
    buildAutoTunerForTests(workerInfo, infoProvider, platform)
  }

  /**
   * Helper method to check if the expected lines exist in the AutoTuner output.
   */
  private def assertExpectedLinesExist(
      expectedResults: Seq[String], autoTunerOutput: String): Unit = {
    val missingLines = expectedResults.filterNot(autoTunerOutput.contains)

    if (missingLines.nonEmpty) {
      val errorMessage =
        s"""|=== Missing Lines ===
            |${missingLines.mkString("\n")}
            |
            |=== Actual Output ===
            |$autoTunerOutput
            |""".stripMargin
      fail(errorMessage)
    }
  }

  test("test AutoTuner for Qualification sets batch size to 1GB") {
    val autoTuner = buildDefaultAutoTuner()
    val (properties, comments) = autoTuner.getRecommendedProperties(showOnlyUpdatedProps =
      QualificationAutoTunerRunner.filterByUpdatedPropsEnabled)
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults = Seq(
        "--conf spark.rapids.sql.batchSizeBytes=1g",
        "- 'spark.rapids.sql.batchSizeBytes' was not set."
    )
    assertExpectedLinesExist(expectedResults, autoTunerOutput)
  }

  test("test AutoTuner for Qualification should not change shuffle partitions") {
    // Set shuffle partitions to 100. The AutoTuner should recommend the same value
    // because currently shuffle.partitions is one of the limitedLogicRecommendations.
    // It will not be added to the recommendations because the value has not changed.
    val autoTuner = buildDefaultAutoTuner(
      defaultSparkProps ++ mutable.Map("spark.sql.shuffle.partitions" -> "100")
    )
    val (properties, comments) = autoTuner.getRecommendedProperties(showOnlyUpdatedProps =
      QualificationAutoTunerRunner.filterByUpdatedPropsEnabled)
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults = Seq(
      "--conf spark.sql.shuffle.partitions=100"
    )
    assertExpectedLinesExist(expectedResults, autoTunerOutput)
  }

  // scalastyle:off line.size.limit
  val testData: TableFor3[String, String, Seq[String]] = Table(
    ("testName", "workerMemory", "expectedResults"),
    ("less memory available for executors",
      "16g",
      Seq(
        "--conf spark.executor.memory=[FILL_IN_VALUE]",
        "--conf spark.executor.memoryOverhead=[FILL_IN_VALUE]",
        "--conf spark.rapids.memory.pinnedPool.size=[FILL_IN_VALUE]",
        s"- ${ProfilingAutoTunerConfigsProvider.notEnoughMemCommentForKey("spark.executor.memory")}",
        s"- ${ProfilingAutoTunerConfigsProvider.notEnoughMemCommentForKey("spark.executor.memoryOverhead")}",
        s"- ${ProfilingAutoTunerConfigsProvider.notEnoughMemCommentForKey("spark.rapids.memory.pinnedPool.size")}",
        s"- ${QualificationAutoTunerConfigsProvider.notEnoughMemComment(40140)}"
      )),
    ("sufficient memory available for executors",
      "44g",
      Seq(
        "--conf spark.executor.memory=32g",
        "--conf spark.executor.memoryOverhead=11468m",
        "--conf spark.rapids.memory.pinnedPool.size=4g"
      ))
  )
  // scalastyle:on line.size.limit

  forAll(testData) { (testName: String, workerMemory: String, expectedResults: Seq[String]) =>
    test(s"test memory warnings for case: $testName") {
      val workerInfo = buildCpuWorkerInfoAsString(None, Some(16), Some(workerMemory), Some(2))
      val logEventsProps: mutable.Map[String, String] =
        mutable.LinkedHashMap[String, String](
          "spark.executor.cores" -> "8",
          "spark.executor.instances" -> "4",
          "spark.executor.memory" -> "8g",
          "spark.executor.memoryOverhead" -> "2g"
        )
      val clusterPropsOpt = PropertiesLoader[ClusterProperties].loadFromContent(workerInfo)
      val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
        logEventsProps, Some(testSparkVersion))
      val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, clusterPropsOpt)
      val autoTuner = buildAutoTunerForTests(workerInfo,
        infoProvider, platform, sparkMaster = Some(Yarn))
      val (properties, comments) = autoTuner.getRecommendedProperties(showOnlyUpdatedProps =
        QualificationAutoTunerRunner.filterByUpdatedPropsEnabled)
      val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
      assertExpectedLinesExist(expectedResults, autoTunerOutput)
    }
  }
}
