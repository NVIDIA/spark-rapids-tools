/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.tool.profiling.RecommendedCommentResult

/**
 * Test suite for EMR THP (Transparent Huge Pages) tuning plugin.
 *
 * This test suite verifies that the EMR THP tuning plugin correctly:
 * - Activates only on EMR platform
 * - Disables THP in both driver and executor JVM options
 * - Adds appropriate comments explaining the recommendations
 */
class EmrThpTuningPluginSuite extends ProfilingAutoTunerSuite {
  private val thpDisableFlag = "-XX:-UseTransparentHugePages"

  private def baseProps(extraProps: (String, String)*): mutable.Map[String, String] = {
    mutable.LinkedHashMap[String, String](
      "spark.executor.cores" -> "16",
      "spark.executor.instances" -> "4",
      "spark.executor.memory" -> "32g",
      "spark.executor.resource.gpu.amount" -> "1",
      "spark.plugins" -> "com.nvidia.spark.SQLPlugin"
    ) ++ extraProps
  }

  private def runAutoTuner(
      logEventsProps: mutable.Map[String, String],
      platformName: String = PlatformNames.EMR,
      sparkVersion: Option[String] = Some("3.4.1-amzn-1"),
      numCores: Int = 16,
      numWorkers: Int = 4):
      (Seq[TuningEntryTrait], Seq[RecommendedCommentResult]) = {
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0), logEventsProps, sparkVersion)
    val platform = PlatformFactory.createInstance(platformName)

    configureEventLogClusterInfoForTest(
      platform,
      numCores = numCores,
      numWorkers = numWorkers,
      gpuCount = 4,
      sparkProperties = logEventsProps.toMap
    )
    buildAutoTunerForTests(infoProvider, platform).getRecommendedProperties()
  }

  private def assertDriverAndExecutorSetToThp(properties: Seq[TuningEntryTrait]): Unit = {
    val driverJavaOpts = properties
      .find(_.name == "spark.driver.extraJavaOptions")
      .map(_.getTuneValue())
    val executorJavaOpts = properties.find(_.name == "spark.executor.extraJavaOptions")
      .map(_.getTuneValue())
    assert(driverJavaOpts.contains(thpDisableFlag),
      s"Driver should be set to only THP disable flag, actual: " +
        s"${driverJavaOpts.getOrElse("<missing>")}")
    assert(executorJavaOpts.contains(thpDisableFlag),
      s"Executor should be set to only THP disable flag, actual: " +
        s"${executorJavaOpts.getOrElse("<missing>")}")
  }

  private def assertNoThpRecommendations(properties: Seq[TuningEntryTrait]): Unit = {
    val driverJavaOpts = properties.find(_.name == "spark.driver.extraJavaOptions")
    val executorJavaOpts = properties.find(_.name == "spark.executor.extraJavaOptions")
    assert(driverJavaOpts.isEmpty, "Driver THP recommendation should be absent")
    assert(executorJavaOpts.isEmpty, "Executor THP recommendation should be absent")
  }

  private def assertContainsThpComments(comments: Seq[RecommendedCommentResult]): Unit = {
    val commentsStr = comments.map(_.comment).mkString("\n")
    val expectedSnippets = Seq(
      "Set 'spark.driver.extraJavaOptions=-XX:-UseTransparentHugePages' to disable " +
        "Transparent Huge Pages (THP) for EMR.",
      "Set 'spark.executor.extraJavaOptions=-XX:-UseTransparentHugePages' to disable " +
        "Transparent Huge Pages (THP) for EMR.",
      "does not preserve existing driver JVM options",
      "does not preserve existing executor JVM options",
      "append any additional options manually"
    )
    expectedSnippets.foreach { snippet =>
      assert(commentsStr.contains(snippet), s"Comments should include: $snippet")
    }
  }

  test("EMR THP plugin adds THP disable flag when not present") {
    val (properties, comments) = runAutoTuner(baseProps())
    assertDriverAndExecutorSetToThp(properties)
    assertContainsThpComments(comments)
  }

  test("EMR THP plugin overwrites existing driver and executor JVM options with THP disable flag") {
    val (properties, comments) = runAutoTuner(baseProps(
      "spark.driver.extraJavaOptions" -> "-Xms1g -Xmx2g",
      "spark.executor.extraJavaOptions" -> "-XX:+UseG1GC -Xmx4g"
    ))
    assertDriverAndExecutorSetToThp(properties)
    assertContainsThpComments(comments)
  }

  test("EMR THP plugin replaces THP enable flag for driver and executor with disable flag") {
    val (properties, comments) = runAutoTuner(baseProps(
      "spark.driver.extraJavaOptions" -> "-Xmx2g -XX:+UseTransparentHugePages",
      "spark.executor.extraJavaOptions" -> "-Xmx4g -XX:+UseTransparentHugePages"
    ))
    assertDriverAndExecutorSetToThp(properties)
    assertContainsThpComments(comments)
  }

  test("EMR THP plugin does not apply when THP disable flag is already present") {
    val (properties, _) = runAutoTuner(baseProps(
      "spark.driver.extraJavaOptions" -> "-Xmx2g -XX:-UseTransparentHugePages",
      "spark.executor.extraJavaOptions" -> "-XX:-UseTransparentHugePages -Xmx4g"
    ))
    assertNoThpRecommendations(properties)
  }

  test("EMR THP plugin only activates on EMR platform") {
    val (propertiesDataproc, _) = runAutoTuner(
      baseProps(),
      platformName = PlatformNames.DATAPROC,
      sparkVersion = Some("3.4.1")
    )
    assertNoThpRecommendations(propertiesDataproc)
  }
}
