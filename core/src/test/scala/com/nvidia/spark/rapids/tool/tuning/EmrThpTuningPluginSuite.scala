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
import com.nvidia.spark.rapids.tool.profiling.Profiler

/**
 * Test suite for EMR THP (Transparent Huge Pages) tuning plugin.
 *
 * This test suite verifies that the EMR THP tuning plugin correctly:
 * - Activates only on EMR platform
 * - Disables THP in both driver and executor JVM options
 * - Handles existing JVM options properly
 * - Replaces enabled THP flags with disabled flags
 * - Adds appropriate comments explaining the recommendations
 */
class EmrThpTuningPluginSuite extends ProfilingAutoTunerSuite {

  test("EMR THP plugin adds THP disable flag when not present") {
    // Mock properties with no existing extraJavaOptions
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "4",
        "spark.executor.memory" -> "32g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin"
      )

    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some("3.4.1-amzn-1"))
    val platform = PlatformFactory.createInstance(PlatformNames.EMR)

    configureEventLogClusterInfoForTest(
      platform,
      numCores = 16,
      numWorkers = 4,
      gpuCount = 4,
      sparkProperties = logEventsProps.toMap
    )

    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()

    // Verify that THP disable flags are added
    val driverJavaOpts = properties.find(_.name == "spark.driver.extraJavaOptions")
    val executorJavaOpts = properties.find(_.name == "spark.executor.extraJavaOptions")

    assert(driverJavaOpts.isDefined, "spark.driver.extraJavaOptions should be recommended")
    assert(executorJavaOpts.isDefined, "spark.executor.extraJavaOptions should be recommended")
    assert(driverJavaOpts.get.getTuneValue().contains("-XX:-UseTransparentHugePages"),
      "Driver JVM options should contain -XX:-UseTransparentHugePages")
    assert(executorJavaOpts.get.getTuneValue().contains("-XX:-UseTransparentHugePages"),
      "Executor JVM options should contain -XX:-UseTransparentHugePages")

    // Verify comments are added
    val commentsStr = comments.map(_.comment).mkString("\n")
    assert(commentsStr.contains("Transparent Huge Pages") || commentsStr.contains("THP"),
      "Comments should mention THP or Transparent Huge Pages")
  }

  test("EMR THP plugin appends THP disable flag to existing JVM options") {
    // Mock properties with existing extraJavaOptions
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "4",
        "spark.executor.memory" -> "32g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.driver.extraJavaOptions" -> "-Xms1g -Xmx2g",
        "spark.executor.extraJavaOptions" -> "-XX:+UseG1GC -Xmx4g"
      )

    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some("3.4.1-amzn-1"))
    val platform = PlatformFactory.createInstance(PlatformNames.EMR)

    configureEventLogClusterInfoForTest(
      platform,
      numCores = 16,
      numWorkers = 4,
      gpuCount = 4,
      sparkProperties = logEventsProps.toMap
    )

    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, _) = autoTuner.getRecommendedProperties()

    // Verify that THP disable flags are appended to existing options
    val driverJavaOpts = properties.find(_.name == "spark.driver.extraJavaOptions")
    val executorJavaOpts = properties.find(_.name == "spark.executor.extraJavaOptions")

    assert(driverJavaOpts.isDefined, "spark.driver.extraJavaOptions should be recommended")
    assert(executorJavaOpts.isDefined, "spark.executor.extraJavaOptions should be recommended")

    // Verify existing options are preserved
    assert(driverJavaOpts.get.getTuneValue().contains("-Xms1g"),
      "Existing driver JVM options should be preserved")
    assert(driverJavaOpts.get.getTuneValue().contains("-Xmx2g"),
      "Existing driver JVM options should be preserved")
    assert(executorJavaOpts.get.getTuneValue().contains("-XX:+UseG1GC"),
      "Existing executor JVM options should be preserved")
    assert(executorJavaOpts.get.getTuneValue().contains("-Xmx4g"),
      "Existing executor JVM options should be preserved")

    // Verify THP disable flag is appended
    assert(driverJavaOpts.get.getTuneValue().contains("-XX:-UseTransparentHugePages"),
      "THP disable flag should be appended to driver options")
    assert(executorJavaOpts.get.getTuneValue().contains("-XX:-UseTransparentHugePages"),
      "THP disable flag should be appended to executor options")
  }

  test("EMR THP plugin replaces THP enable flag with disable flag") {
    // Mock properties with THP explicitly enabled
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "4",
        "spark.executor.memory" -> "32g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.driver.extraJavaOptions" -> "-XX:+UseTransparentHugePages -Xms1g",
        "spark.executor.extraJavaOptions" -> "-Xmx4g -XX:+UseTransparentHugePages"
      )

    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some("3.4.1-amzn-1"))
    val platform = PlatformFactory.createInstance(PlatformNames.EMR)

    configureEventLogClusterInfoForTest(
      platform,
      numCores = 16,
      numWorkers = 4,
      gpuCount = 4,
      sparkProperties = logEventsProps.toMap
    )

    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, _) = autoTuner.getRecommendedProperties()

    // Verify that THP enable flags are replaced with disable flags
    val driverJavaOpts = properties.find(_.name == "spark.driver.extraJavaOptions")
    val executorJavaOpts = properties.find(_.name == "spark.executor.extraJavaOptions")

    assert(driverJavaOpts.isDefined, "spark.driver.extraJavaOptions should be recommended")
    assert(executorJavaOpts.isDefined, "spark.executor.extraJavaOptions should be recommended")

    // Verify enable flag is removed and disable flag is present
    assert(!driverJavaOpts.get.getTuneValue().contains("-XX:+UseTransparentHugePages"),
      "THP enable flag should be removed from driver options")
    assert(!executorJavaOpts.get.getTuneValue().contains("-XX:+UseTransparentHugePages"),
      "THP enable flag should be removed from executor options")
    assert(driverJavaOpts.get.getTuneValue().contains("-XX:-UseTransparentHugePages"),
      "THP disable flag should be present in driver options")
    assert(executorJavaOpts.get.getTuneValue().contains("-XX:-UseTransparentHugePages"),
      "THP disable flag should be present in executor options")

    // Verify other options are preserved
    assert(driverJavaOpts.get.getTuneValue().contains("-Xms1g"),
      "Other driver JVM options should be preserved")
    assert(executorJavaOpts.get.getTuneValue().contains("-Xmx4g"),
      "Other executor JVM options should be preserved")
  }

  test("EMR THP plugin does not apply when THP is already disabled") {
    // Mock properties with THP already disabled
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "4",
        "spark.executor.memory" -> "32g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.driver.extraJavaOptions" -> "-XX:-UseTransparentHugePages -Xms1g",
        "spark.executor.extraJavaOptions" -> "-Xmx4g -XX:-UseTransparentHugePages"
      )

    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some("3.4.1-amzn-1"))
    val platform = PlatformFactory.createInstance(PlatformNames.EMR)

    configureEventLogClusterInfoForTest(
      platform,
      numCores = 16,
      numWorkers = 4,
      gpuCount = 4,
      sparkProperties = logEventsProps.toMap
    )

    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, _) = autoTuner.getRecommendedProperties()

    // Verify that no changes are made to THP settings since they're already correct
    val driverJavaOpts = properties.find(_.name == "spark.driver.extraJavaOptions")
    val executorJavaOpts = properties.find(_.name == "spark.executor.extraJavaOptions")

    // The properties should not be in recommendations if they're already correct
    assert(driverJavaOpts.isEmpty ||
           driverJavaOpts.get.getTuneValue() == "-XX:-UseTransparentHugePages -Xms1g",
      "Driver options should not be changed if THP is already disabled")
    assert(executorJavaOpts.isEmpty ||
           executorJavaOpts.get.getTuneValue() == "-Xmx4g -XX:-UseTransparentHugePages",
      "Executor options should not be changed if THP is already disabled")
  }

  test("EMR THP plugin only activates on EMR platform") {
    // Test that the plugin doesn't activate on other platforms
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "4",
        "spark.executor.memory" -> "32g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin"
      )

    // Test with Dataproc platform
    val infoProviderDataproc = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some("3.4.1"))
    val platformDataproc = PlatformFactory.createInstance(PlatformNames.DATAPROC)

    configureEventLogClusterInfoForTest(
      platformDataproc,
      numCores = 16,
      numWorkers = 4,
      gpuCount = 4,
      sparkProperties = logEventsProps.toMap
    )

    val autoTunerDataproc = buildAutoTunerForTests(infoProviderDataproc, platformDataproc)
    val (propertiesDataproc, _) = autoTunerDataproc.getRecommendedProperties()

    // Verify that THP settings are not recommended for Dataproc
    val driverJavaOptsDataproc = propertiesDataproc.find(_.name == "spark.driver.extraJavaOptions")
    val executorJavaOptsDataproc =
      propertiesDataproc.find(_.name == "spark.executor.extraJavaOptions")

    assert(driverJavaOptsDataproc.isEmpty ||
           !driverJavaOptsDataproc.get.getTuneValue().contains("-XX:-UseTransparentHugePages"),
      "THP settings should not be recommended for Dataproc")
    assert(executorJavaOptsDataproc.isEmpty ||
           !executorJavaOptsDataproc.get.getTuneValue().contains("-XX:-UseTransparentHugePages"),
      "THP settings should not be recommended for Dataproc")
  }

  test("EMR THP plugin full integration test with autotuner output") {
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "32",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.resource.gpu.discoveryScript" ->
          "${SPARK_HOME}/examples/src/main/scripts/getGpusResources.sh",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin"
      )

    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some("3.4.1-amzn-1"))
    val platform = PlatformFactory.createInstance(PlatformNames.EMR)

    val sparkPropsWithMemory = logEventsProps + ("spark.executor.memory" -> "212992MiB")
    configureEventLogClusterInfoForTest(
      platform,
      numCores = 32,
      numWorkers = 5,
      gpuCount = 4,
      sparkProperties = sparkPropsWithMemory.toMap
    )

    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)

    // Verify output contains THP disable flags
    assert(autoTunerOutput.contains("spark.driver.extraJavaOptions"),
      "Output should contain driver extraJavaOptions")
    assert(autoTunerOutput.contains("spark.executor.extraJavaOptions"),
      "Output should contain executor extraJavaOptions")
    assert(autoTunerOutput.contains("-XX:-UseTransparentHugePages"),
      "Output should contain THP disable flag")

    // Verify comments mention THP
    assert(autoTunerOutput.contains("Transparent Huge Pages") ||
           autoTunerOutput.contains("THP") ||
           autoTunerOutput.contains("performance"),
      "Output should mention THP or performance improvements")
  }
}

