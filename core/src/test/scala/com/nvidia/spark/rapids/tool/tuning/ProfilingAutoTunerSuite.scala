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

import java.io.{File, FileNotFoundException}

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.{A100Gpu, AppSummaryInfoBaseProvider, GpuDevice, PlatformFactory, PlatformNames, T4Gpu, ToolTestUtils}
import com.nvidia.spark.rapids.tool.planparser.DatabricksParseHelper
import com.nvidia.spark.rapids.tool.profiling.{DriverLogUnsupportedOperators, ProfileArgs, ProfileMain, Profiler}
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.TableFor4

import org.apache.spark.sql.TrampolineUtil
import org.apache.spark.sql.rapids.tool.util.{FSUtils, WebCrawlerUtil}

/**
 * Suite to test the Profiling Tool's AutoTuner
 */
class ProfilingAutoTunerSuite extends BaseAutoTunerSuite {

  private val profilingLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")

  /**
   * Helper method to get the path to the output file
   */
  def getOutputFilePath(outputDir: File, fileName: String): String = {
    val profilerOutputDir = new File(outputDir, s"${Profiler.SUBDIR}")
    val outputFile = profilerOutputDir.listFiles()
      .filter(_.isDirectory)
      .flatMap(dir => dir.listFiles().filter(file => file.isFile && file.getName == fileName))
      .headOption

    outputFile match {
      case Some(file) => file.getAbsolutePath
      case None => throw new FileNotFoundException(
        s"File $fileName not found in ${profilerOutputDir.getAbsolutePath}")
    }
  }

  /**
   * Helper method to extract the AutoTuner results from the profile log content
   * TODO: We should store the AutoTuner results in a separate file.
   */
  def extractAutoTunerResults(profileLogContent: String): String = {
    val startSubstring = "### D. Recommended Configuration ###"
    val indexOfAutoTunerOutput = profileLogContent.indexOf(startSubstring)
    if (indexOfAutoTunerOutput > 0) {
      profileLogContent.substring(indexOfAutoTunerOutput + startSubstring.length).trim
    } else {
      ""
    }
  }

  /**
   * Helper method to build a worker info string with GPU properties
   */
  protected def buildGpuWorkerInfoAsString(
      customProps: Option[mutable.Map[String, String]] = None,
      numCores: Option[Int] = Some(32),
      systemMemory: Option[String] = Some("122880MiB"),
      numWorkers: Option[Int] = Some(4),
      gpuCount: Option[Int] = Some(2),
      gpuMemory: Option[String] = Some(GpuDevice.DEFAULT.getMemory),
      gpuDevice: Option[String] = Some(GpuDevice.DEFAULT.toString)): String = {
    buildWorkerInfoAsString(customProps, numCores, systemMemory, numWorkers,
      gpuCount, gpuMemory, gpuDevice)
  }

  private def getGpuAppMockInfoProvider: AppSummaryInfoBaseProvider = {
    getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some(testSparkVersion), Seq())
  }

  private def getGpuAppMockInfoWithJars(rapidsJars: Seq[String]): AppSummaryInfoBaseProvider = {
    getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some(testSparkVersion), rapidsJars)
  }

  /**
   * Helper method to return an instance of the Profiling AutoTuner with default properties
   * for Dataproc.
   */
  private def buildDefaultDataprocAutoTuner(
      logEventsProps: mutable.Map[String, String]): AutoTuner = {
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(None, Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some(testSparkVersion))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    ProfilingAutoTunerConfigsProvider.buildAutoTunerFromProps(
      dataprocWorkerInfo, infoProvider, platform)
  }

  test("verify 3.2.0+ auto conf setting") {
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(None, Some(32), Some("122880MiB"), Some(0))
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some("3.2.0"), Seq())
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=2
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark320.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.cores' was not set.
          |- 'spark.executor.instances' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- Number of workers is missing. Setting default to 1.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Load non-existing cluster properties") {
    val platform = PlatformFactory.createInstance(clusterProperties = None)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTuner(getGpuAppMockInfoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.locality.wait=0
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.files.maxPartitionBytes=512m
          |
          |Comments:
          |- 'spark.executor.cores' should be set to 16.
          |- 'spark.executor.instances' should be set to (cpuCoresPerNode * numWorkers) / 'spark.executor.cores'.
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Min(4, (gpuMemory / 7.5G)).
          |- 'spark.rapids.sql.enabled' should be true to enable SQL operations on the GPU.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' should be set to 0.001.
          |- Could not infer the cluster configuration, recommendations are generated using default values!
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Load cluster properties with CPU cores 0") {
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(None, Some(0))
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.locality.wait=0
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.files.maxPartitionBytes=512m
          |
          |Comments:
          |- 'spark.executor.cores' should be set to 16.
          |- 'spark.executor.instances' should be set to (cpuCoresPerNode * numWorkers) / 'spark.executor.cores'.
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Min(4, (gpuMemory / 7.5G)).
          |- 'spark.rapids.sql.enabled' should be true to enable SQL operations on the GPU.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' should be set to 0.001.
          |- Could not infer the cluster configuration, recommendations are generated using default values!
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Load cluster properties with memory to cores ratio to small") {
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(None, Some(8), Some("14000MiB"))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo,
        getGpuAppMockInfoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedComment =
      s"""This node/worker configuration is not ideal for using the Spark Rapids
Accelerator because it doesn't have enough memory for the executors.
We recommend using nodes/workers with more memory. Need at least 17496MB memory.""".stripMargin.replaceAll("\n", "")
    // scalastyle:on line.size.limit
    assert(autoTunerOutput.replaceAll("\n", "").contains(expectedComment))
  }

  test("Load cluster properties with CPU memory missing") {
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(None, Some(32), None)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.locality.wait=0
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.files.maxPartitionBytes=512m
          |
          |Comments:
          |- 'spark.executor.cores' should be set to 16.
          |- 'spark.executor.instances' should be set to (cpuCoresPerNode * numWorkers) / 'spark.executor.cores'.
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Min(4, (gpuMemory / 7.5G)).
          |- 'spark.rapids.sql.enabled' should be true to enable SQL operations on the GPU.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' should be set to 0.001.
          |- Could not infer the cluster configuration, recommendations are generated using default values!
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Load cluster properties with CPU memory 0") {
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(None, Some(32), Some("0m"))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=8
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.cores' was not set.
          |- 'spark.executor.instances' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Load cluster properties with number of workers 0") {
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(None, Some(32), Some("122880MiB"), Some(0))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=2
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.cores' was not set.
          |- 'spark.executor.instances' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- Number of workers is missing. Setting default to 1.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Load cluster properties with GPU count of 0") {
    // the gpuCount should default to 1
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.001")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(0))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=4
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- GPU count is missing. Setting default to 1.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Load cluster properties with GPU memory is missing") {
    // the gpu memory should be set to T4 memory settings
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "8396m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "16",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "16",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.001")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(2), None)
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- GPU memory is missing. Setting default to 15109m.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Load cluster properties with GPU memory 0") {
    // the gpu memory should be set to T4 memory settings
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "8396m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "16",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "16",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.001",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "64m",
      "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "4m")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(2), Some("0M"))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- GPU memory is missing. Setting default to 15109m.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Load cluster properties with GPU name missing") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "8396m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "16",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "16",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.001")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(2), Some("0MiB"), None)
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- GPU device is missing. Setting default to $T4Gpu.
          |- GPU memory is missing. Setting default to ${T4Gpu.getMemory}.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Load cluster properties with unknown GPU device") {
    // with unknown gpu device, this test should default to A100
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "8396m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "16",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "16",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.001")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(2), Some("0MiB"), Some("GPU-X"))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- GPU device is missing. Setting default to $T4Gpu.
          |- GPU memory is missing. Setting default to ${T4Gpu.getMemory}.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // This test should ignore executor memory specified in the properties as
  // CSP nodes have fixed memory configurations.
  test("Test executor memory on CSP where executor memory/cpu ratio is small") {
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(None, Some(8), Some("15360MiB"),
      Some(4), Some(1))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=2
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.cores' was not set.
          |- 'spark.executor.instances' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("test T4 dataproc cluster with dynamicAllocation enabled") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "8396m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "16",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "16",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.001")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(sparkProps))
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    compareOutput(expectedResults, autoTunerOutput)
  }

  // This mainly to test that the executorInstances will be calculated when the dynamic allocation
  // is missing.
  test("test T4 dataproc cluster with missing dynamic allocation") {
    val customProps = mutable.LinkedHashMap(
      "spark.dynamicAllocation.enabled" -> "false",
      "spark.executor.cores" -> "32",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "8396m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "16",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "16",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.001")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(sparkProps))
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=4
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("test AutoTuner with empty sparkProperties") {
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(None)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=8
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.cores' was not set.
          |- 'spark.executor.instances' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("AutoTuner detects non UTF-8 file-encoding") {
    // When system properties has an entry for file-encoding that is not supported by GPU for
    // certain expressions. Then the AutoTuner should generate a comment warning that the
    // file-encoding is not one of the supported ones.
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
      "spark.task.resource.gpu.amount" -> "0.001")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        // set the file-encoding to non UTF-8
        "file.encoding" -> "ISO-8859-1",
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |- file.encoding should be [UTF-8] because GPU only supports the charset when using some expressions.
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }
  // Test that the properties from the custom props will be used to calculate the recommendations.
  // For example, the output should use "spark.executor.cores" -> 4 when calculating the
  // recommendations.
  test("AutoTuner gives precedence to properties from custom props") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "4",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
      "spark.task.resource.gpu.amount" -> "0.001")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=5
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("plugin set to the wrong values") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.plugins" -> "com.nvidia.spark.WrongPlugin0, com.nvidia.spark.WrongPlugin1",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion))
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // Why this test to see if it carries over the sql.enabled false???? This is broken
  // if needed
  test("plugin not enabled") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "false",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // Changing the maxInput of tasks should reflect on the maxPartitions recommendations.
  // Values used in setting the properties are taken from sample eventlogs.
  test("Recommendation of maxPartitions is calculated based on maxInput of tasks") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "4m",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo,
        getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.01, 0.0), logEventsProps,
          Some(testSparkVersion)), platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // When GCFraction is higher AutoTuner.MAX_JVM_GCTIME_FRACTION, the output should contain
  // a comment recommending to consider different GC algorithms.
  // This test triggers that case by injecting a sequence of jvmGCFraction with average higher
  // than the static threshold of 0.3.
  test("Output contains GC comments when GC Fraction is higher than threshold") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "4m",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(testSparkVersion))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // Ensure handling of integer values for maxPartitionBytes is handled
  test("Handle integer value of maxPartitionBytes properly") {
    val customProps = mutable.LinkedHashMap(
      "spark.sql.files.maxPartitionBytes" -> "12345678")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "4m",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(testSparkVersion))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  def generateRecommendationsForRapidsJars(rapidsJars: Seq[String]): String = {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "8396m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "16",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "16",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.001")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(sparkProps))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo,
        getGpuAppMockInfoWithJars(rapidsJars), platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    Profiler.getAutoTunerResultsAsString(properties, comments)
  }

  test("Multiple RAPIDS jars trigger a comment") {
    // 1. The Autotuner should warn the users that they have multiple jars defined in the classPath
    // 2. Compare the output
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.multiple")} [23.06.0, 23.02.1]
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    val rapidsJarsArr = Seq("rapids-4-spark_2.12-23.06.0-SNAPSHOT.jar",
      "rapids-4-spark_2.12-23.02.1.jar")
    val autoTunerOutput = generateRecommendationsForRapidsJars(rapidsJarsArr)
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Recommend upgrading to the latest plugin release") {
    // 1. Pull the latest release from mvn.
    // 2. The Autotuner should warn the users that they are using an older release
    // 3. Compare the output
    val jarVer = "23.02.0"
    val latestRelease = WebCrawlerUtil.getLatestPluginRelease match {
      case Some(v) => v
      case None => fail("Could not find pull the latest release successfully")
    }
    val pluginJarMvnURl = "https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/" +
      s"$latestRelease/rapids-4-spark_2.12-$latestRelease.jar"
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- A newer RAPIDS Accelerator for Apache Spark plugin is available:
          |  $pluginJarMvnURl
          |  Version used in application is $jarVer.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    val rapidsJarsArr = Seq(s"rapids-4-spark_2.12-$jarVer.jar")
    val autoTunerOutput = generateRecommendationsForRapidsJars(rapidsJarsArr)
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("No recommendation when the jar pluginJar is up-to-date") {
    // 1. Pull the latest release from mvn.
    // 2. The Autotuner finds tha the jar version is latest. No comments should be added
    // 3. Compare the output
    val latestRelease = WebCrawlerUtil.getLatestPluginRelease match {
      case Some(v) => v
      case None => fail("Could not find pull the latest release successfully")
    }
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    val rapidsJarsArr = Seq(s"rapids-4-spark_2.12-$latestRelease.jar")
    val autoTunerOutput = generateRecommendationsForRapidsJars(rapidsJarsArr)
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Recommend file cache if parquet/orc and data thresholds are met") {
    val customProps = mutable.LinkedHashMap(
      "spark.sql.files.maxPartitionBytes" -> "12345678")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "4m",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" ->
          s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(testSparkVersion), Seq(), 40.0, 200000000000L)
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.filecache.enabled=true
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.filecache.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- Enable file cache only if Spark local disks bandwidth is > 1 GB/s and you have sufficient disk space available to fit both cache and normal Spark temporary data.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Do not recommend file cache if small redundant size") {
    val customProps = mutable.LinkedHashMap(
      "spark.sql.files.maxPartitionBytes" -> "12345678")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "1m",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" ->
          s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(testSparkVersion), Seq(), 40.0, 2000000L)
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("test recommendations for databricks-aws platform argument") {
    val databricksWorkerInfo = buildGpuWorkerInfoAsString()
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(databricksWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATABRICKS_AWS, clusterPropsOpt)
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(databricksWorkerInfo,
        getGpuAppMockInfoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()

    // Assert recommendations are excluded in properties
    assert(properties.map(_.name).forall(autoTuner.platform.isValidRecommendation))
    // Assert recommendations are skipped in comments
    assert(comments.map(_.comment).forall(autoTuner.platform.isValidComment))
  }

  // When spark is running as a standalone, the memoryOverhead should not be listed as a
  // recommendation: issue-553.
  test("memoryOverhead should not be recommended for Spark Standalone") {
    // This UT sets a custom spark-property "spark.master" pointing to a spark-standalone value
    // The Autotuner should detect that the spark-master is standalone and refrains from
    // recommending memoryOverhead value
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
      "spark.task.resource.gpu.amount" -> "0.001")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.master" -> "spark://HOST_NAME:7077",
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // Map of `Spark Version` -> `memoryOverheadLabel`
  private val MISSING_MEMORY_OVERHEAD_k8s_TEST_CASES = Seq(
    "3.5.0" -> "spark.executor.memoryOverheadFactor",
    "3.2.1" -> "spark.kubernetes.memoryOverheadFactor"
  )

  // This UT sets a custom spark-property "spark.master" pointing to a spark
  // k8s value. The Autotuner should detect that the spark-master is k8s and
  // comments on the missing memoryOverhead value since pinned pool is set.
  MISSING_MEMORY_OVERHEAD_k8s_TEST_CASES.foreach { case (sparkVersion, memoryOverheadLabel) =>
    test(s"missing memoryOverhead comment is included for k8s with pinned pool " +
      s"[sparkVersion=$sparkVersion]") {
      val logEventsProps: mutable.Map[String, String] =
        mutable.LinkedHashMap[String, String](
          "spark.master" -> "k8s://https://my-cluster-endpoint.example.com:6443",
          "spark.executor.cores" -> "16",
          "spark.executor.instances" -> "1",
          "spark.executor.memory" -> "80g",
          "spark.executor.resource.gpu.amount" -> "1",
          "spark.executor.instances" -> "1",
          "spark.sql.shuffle.partitions" -> "200",
          "spark.sql.files.maxPartitionBytes" -> "1g",
          "spark.task.resource.gpu.amount" -> "0.001",
          "spark.rapids.memory.pinnedPool.size" -> "5g",
          "spark.rapids.sql.enabled" -> "true",
          "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
          "spark.rapids.sql.concurrentGpuTasks" -> "4")
      val dataprocWorkerInfo = buildGpuWorkerInfoAsString()
      val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
        Some(sparkVersion))
      val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
        .loadClusterPropertiesFromContent(dataprocWorkerInfo)
      val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, clusterPropsOpt)
      val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
        .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
      val (_, comments) = autoTuner.getRecommendedProperties()
      val expectedComment =
        s"'$memoryOverheadLabel' must be set if using 'spark.rapids.memory.pinnedPool.size'."
      assert(comments.exists(_.comment == expectedComment))
    }
  }

  // This UT sets a custom spark-property "spark.master" pointing to a spark
  // k8s value. The Autotuner should detect that the spark-master is k8s and
  // should not comment on the missing memoryOverhead value since pinned pool is not set.
  test(s"missing memoryOverhead comment is not included for k8s without pinned pool " +
    s"[sparkVersion=$testSparkVersion]") {
    val sparkMaster = "k8s://https://my-cluster-endpoint.example.com:6443"
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.master" -> sparkMaster,
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString()
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val memoryOverheadLabel = ProfilingAutoTunerConfigsProvider.getMemoryOverheadLabel(
        SparkMaster(Some(sparkMaster)), Some(testSparkVersion))
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.executor.memory=32768m
          |--conf $memoryOverheadLabel=13516m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=24
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=24
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=32
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |
          |Comments:
          |- '$memoryOverheadLabel' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // This UT sets a custom spark-property "spark.master" pointing to a yarn
  // value. The Autotuner should detect that the spark-master is yarn and
  // should not comment on the missing memoryOverhead value even though pinned
  // pool is set.
  test("missing memoryOverhead comment is not included for yarn " +
    s"[sparkVersion=$testSparkVersion]") {
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.master" -> "yarn",
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString()
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=13516m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=24
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=24
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=32
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Recommendations generated for unsupported operators from driver logs only") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    val unsupportedDriverOperators = Seq(
      DriverLogUnsupportedOperators(
        "FromUnixTime", 1,
        "Only UTC zone id is supported. Actual default zone id: America/Los_Angeles; " +
          "CORRECTED format 'yyyyMMdd' on the GPU is not guaranteed to produce the same " +
          "results as Spark on CPU. Set spark.rapids.sql.incompatibleDateFormats.enabled=true " +
          "to force onto GPU.")
    )
    val driverInfoProvider = DriverInfoProviderMockTest(unsupportedDriverOperators)
    val workerInfo = buildGpuWorkerInfoAsString(Some(customProps))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(workerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DEFAULT, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(workerInfo,
        AppSummaryInfoBaseProvider.fromAppInfo(None),
        platform, driverInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.rapids.sql.incompatibleDateFormats.enabled=true
          |
          |Comments:
          |- 'spark.rapids.sql.incompatibleDateFormats.enabled' was not set.
          |- ${ProfilingAutoTunerConfigsProvider.commentForExperimentalConfig("spark.rapids.sql.incompatibleDateFormats.enabled")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Recommendations generated for unsupported operators from driver and event logs") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    val unsupportedDriverOperators = Seq(
      DriverLogUnsupportedOperators(
        "FromUnixTime", 1,
        "Only UTC zone id is supported. Actual default zone id: America/Los_Angeles; " +
          "CORRECTED format 'yyyyMMdd' on the GPU is not guaranteed to produce the same " +
          "results as Spark on CPU. Set spark.rapids.sql.incompatibleDateFormats.enabled=true " +
          "to force onto GPU.")
    )
    val driverInfoProvider = DriverInfoProviderMockTest(unsupportedDriverOperators)
    val workerInfo = buildGpuWorkerInfoAsString(Some(customProps))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(workerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DEFAULT, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(workerInfo,
        getGpuAppMockInfoProvider, platform, driverInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=4
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=13516m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=24
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=24
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.incompatibleDateFormats.enabled=true
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=32
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |
          |Comments:
          |- 'spark.executor.instances' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.incompatibleDateFormats.enabled' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |- ${ProfilingAutoTunerConfigsProvider.commentForExperimentalConfig("spark.rapids.sql.incompatibleDateFormats.enabled")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Recommendations generated for empty unsupported operators from driver logs only") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    val workerInfo = buildGpuWorkerInfoAsString(Some(customProps))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(workerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DEFAULT, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(workerInfo,
        AppSummaryInfoBaseProvider.fromAppInfo(None),
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Recommendations not generated for unsupported operators from driver logs") {
    // This test does not generate any recommendations for the unsupported operator 'Literal'
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    val unsupportedDriverOperators = Seq(
      DriverLogUnsupportedOperators(
        "Literal", 3,
        "expression Literal 1700518632630000 produces an unsupported type TimestampType")
    )
    val driverInfoProvider = DriverInfoProviderMockTest(unsupportedDriverOperators)
    val workerInfo = buildGpuWorkerInfoAsString(Some(customProps))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(workerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DEFAULT, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(workerInfo,
        AppSummaryInfoBaseProvider.fromAppInfo(None),
        platform, driverInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("AQE configuration autoBroadcastJoinThreshold should not be GTE 100mb") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.executor.memoryOverhead" -> "7372m",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4",
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "500mb")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- Setting 'spark.sql.adaptive.autoBroadcastJoinThreshold' > 100m could lead to performance\n  regression. Should be set to a lower number.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  private def testPartitionConfigurations(
                                           inputSize: Double,
                                           shuffleRead: Double,
                                           gpuDevice: GpuDevice,
                                           expectedLines: Seq[String]): Unit = {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(gpuDevice.getMemory), Some(gpuDevice.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion), meanInput = inputSize, meanShuffleRead = shuffleRead)
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    assert(expectedLines.forall(line => autoTunerOutput.contains(line)),
      s"Expected lines not found in AutoTuner output")
  }

  val testCases: TableFor4[Int, Int, GpuDevice, Seq[String]] = Table(
    ("inputSize", "shuffleRead", "gpuDevice", "expectedLines"),
    // small input, small shuffle read
    (1000, 1000, T4Gpu, Seq(
      "--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m")),
    // large input, small shuffle read
    (40000, 1000, T4Gpu, Seq()),
    // large input, large shuffle read
    (40000, 80000, T4Gpu, Seq(
      "--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=32m",
      "--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=800",
      "--conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false"
    )),
    // large input, large shuffle read, faster GPU
    (40000, 80000, A100Gpu, Seq(
      "--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=64m",
      "--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=400",
      "--conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false"
    ))
  )

  forAll(testCases) { (inputSize, shuffleRead, gpuDevice, expectedLines) =>
    test(s"AQE partition configs - input size: $inputSize," +
      s" shuffle read: $shuffleRead, gpu device: $gpuDevice") {
      testPartitionConfigurations(inputSize, shuffleRead, gpuDevice, expectedLines)
    }
  }

  test("Handle adaptive auto shuffle configuration setting properly") {
    val customProps = mutable.LinkedHashMap(
      "spark.databricks.adaptive.autoOptimizeShuffle.enabled" -> "true")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "4m",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" ->
        s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(testDatabricksVersion))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATABRICKS_AWS, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.databricks.adaptive.autoOptimizeShuffle.enabled=false
          |--conf spark.executor.memory=32768m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersionDatabricks.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  /**
   * Helper method to verify that the recommended shuffle manager version matches the
   * expected version.
   */
  private def verifyRecommendedShuffleManagerVersion(
      autoTuner: AutoTuner,
      expectedSmVersion: String): Unit = {
    autoTuner.getShuffleManagerClassName match {
      case Right(smClassName) =>
        assert(smClassName == ProfilingAutoTunerConfigsProvider
          .buildShuffleManagerClassName(expectedSmVersion))
      case Left(comment) =>
        fail(s"Expected valid RapidsShuffleManager but got comment: $comment")
    }
  }

  test("test shuffle manager version for supported databricks version") {
    val databricksVersion = "11.3.x-gpu-ml-scala2.12"
    val databricksWorkerInfo = buildGpuWorkerInfoAsString(None)
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin",
        DatabricksParseHelper.PROP_TAG_CLUSTER_SPARK_VERSION_KEY -> databricksVersion),
      Some(databricksVersion), Seq())
    // Do not set the platform as DB to see if it can work correctly irrespective
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(databricksWorkerInfo,
        infoProvider, PlatformFactory.createInstance(PlatformNames.DATABRICKS_AWS))
    // Assert shuffle manager string for DB 11.3 tag
    verifyRecommendedShuffleManagerVersion(autoTuner, expectedSmVersion = "330db")
  }

  test("test shuffle manager version for supported spark version") {
    val sparkVersion = "3.3.0"
    val workerInfo = buildGpuWorkerInfoAsString(None)
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some(sparkVersion), Seq())
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(workerInfo,
        infoProvider, PlatformFactory.createInstance())
    // Assert shuffle manager string for supported Spark v3.3.0
    verifyRecommendedShuffleManagerVersion(autoTuner, expectedSmVersion = "330")
  }

  test("test shuffle manager version for supported custom spark version") {
    val customSparkVersion = "3.3.0-custom"
    val workerInfo = buildGpuWorkerInfoAsString(None)
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some(customSparkVersion), Seq())
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(workerInfo,
        infoProvider, PlatformFactory.createInstance())
    // Assert shuffle manager string for supported custom Spark v3.3.0
    verifyRecommendedShuffleManagerVersion(autoTuner, expectedSmVersion = "330")
  }

  /**
   * Helper method to verify that the shuffle manager version is not recommended
   * for the unsupported Spark version.
   */
  private def verifyUnsupportedSparkVersionForShuffleManager(
      autoTuner: AutoTuner,
      sparkVersion: String): Unit = {
    autoTuner.getShuffleManagerClassName match {
      case Right(smClassName) =>
        fail(s"Expected error comment but got valid RapidsShuffleManager: $smClassName")
      case Left(comment) =>
        assert(comment == ProfilingAutoTunerConfigsProvider
          .shuffleManagerCommentForUnsupportedVersion(sparkVersion, autoTuner.platform))
    }
  }

  test("test shuffle manager version for unsupported databricks version") {
    val databricksVersion = "9.1.x-gpu-ml-scala2.12"
    val databricksWorkerInfo = buildGpuWorkerInfoAsString(None)
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin",
        DatabricksParseHelper.PROP_TAG_CLUSTER_SPARK_VERSION_KEY -> databricksVersion),
      Some(databricksVersion), Seq())
    // Do not set the platform as DB to see if it can work correctly irrespective
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(databricksWorkerInfo,
        infoProvider, PlatformFactory.createInstance(PlatformNames.DATABRICKS_AWS))
    verifyUnsupportedSparkVersionForShuffleManager(autoTuner, databricksVersion)
  }

  test("test shuffle manager version for unsupported spark version") {
    val sparkVersion = "3.1.2"
    val workerInfo = buildGpuWorkerInfoAsString(None)
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some(sparkVersion), Seq())
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(workerInfo,
        infoProvider, PlatformFactory.createInstance())
    verifyUnsupportedSparkVersionForShuffleManager(autoTuner, sparkVersion)
  }

  test("test shuffle manager version for unsupported custom spark version") {
    val customSparkVersion = "3.1.2-custom"
    val workerInfo = buildGpuWorkerInfoAsString(None)
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some(customSparkVersion), Seq())
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(workerInfo,
        infoProvider, PlatformFactory.createInstance())
    verifyUnsupportedSparkVersionForShuffleManager(autoTuner, customSparkVersion)
  }

  test("test shuffle manager version for missing spark version") {
    val workerInfo = buildGpuWorkerInfoAsString(None)
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      None, Seq())
    val autoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(workerInfo,
        infoProvider, PlatformFactory.createInstance())
    // Verify that the shuffle manager is not recommended for missing Spark version
    autoTuner.getShuffleManagerClassName match {
      case Right(smClassName) =>
        fail(s"Expected error comment but got valid RapidsShuffleManager: $smClassName")
      case Left(comment) =>
        assert(comment == ProfilingAutoTunerConfigsProvider.shuffleManagerCommentForMissingVersion)
    }
  }

  test("Test spilling occurred in shuffle stages") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "4m",
        "spark.shuffle.manager" ->
          s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(1000L, 1000L), Seq(0.4, 0.4),
      logEventsProps, Some(testSparkVersion), shuffleStagesWithPosSpilling = Set(1))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.sql.shuffle.partitions=400
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.shuffle.partitions' should be increased since spilling occurred in shuffle stages.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Test spilling occurred in shuffle stages with shuffle skew") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.001")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "4m",
        "spark.shuffle.manager" ->
          s"com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager")
    val dataprocWorkerInfo = buildGpuWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(1000L, 1000L), Seq(0.4, 0.4),
      logEventsProps, Some(testSparkVersion), shuffleStagesWithPosSpilling = Set(1, 5),
      shuffleSkewStages = Set(1))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- Shuffle skew exists (when task's Shuffle Read Size > 3 * Avg Stage-level size) in
          |  stages with spilling. Increasing shuffle partitions is not recommended in this
          |  case since keys will still hash to the same task.
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Test Kryo Serializer sets Registrar") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
      )
    val autoTuner = buildDefaultDataprocAutoTuner(logEventsProps)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.kryo.registrator' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Test Kryo Serializer sets Registrar when already set") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator" -> "org.apache.SomeRegistrator,org.apache.SomeOtherRegistrator"
      )
    val autoTuner = buildDefaultDataprocAutoTuner(logEventsProps)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.kryo.registrator=org.apache.SomeRegistrator,org.apache.SomeOtherRegistrator,com.nvidia.spark.rapids.GpuKryoRegistrator
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Test Kryo Serializer sets Registrar when already set but empty") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator" -> ""
      )
    val autoTuner = buildDefaultDataprocAutoTuner(logEventsProps)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Test EMR sets shuffle manager properly and doesn't need Spark RAPIDS jar") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "32",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1"
      )
    val emrWorkerInfo = buildGpuWorkerInfoAsString(None, Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some("3.4.1-amzn-1"))
    val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(emrWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.EMR, clusterPropsOpt)
    val autoTuner: AutoTuner = ProfilingAutoTunerConfigsProvider
      .buildAutoTunerFromProps(emrWorkerInfo, infoProvider,
        platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark341.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("test AutoTuner sets Dataproc Spark performance enhancements") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1"
      )
    val autoTuner = buildDefaultDataprocAutoTuner(logEventsProps)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("test AutoTuner skips Dataproc Spark performance enhancements if already set") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.dataproc.enhanced.execution.enabled" -> "false"
      )
    val autoTuner = buildDefaultDataprocAutoTuner(logEventsProps)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.instances=10
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
          |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  Seq(true, false).foreach { hasGpuOOm =>
    test(s"test AutoTuner recommends default max partition bytes when not set " +
      s"[hasScanStagesWithGpuOom = $hasGpuOOm]") {
      // mock the properties loaded from eventLog
      val logEventsProps: mutable.Map[String, String] =
        mutable.LinkedHashMap[String, String](
          "spark.executor.cores" -> "16",
          "spark.executor.instances" -> "1",
          "spark.executor.memory" -> "80g",
          "spark.executor.resource.gpu.amount" -> "1",
          "spark.executor.instances" -> "1",
          "spark.task.resource.gpu.amount" -> "0.001",
          "spark.rapids.memory.pinnedPool.size" -> "5g",
          "spark.rapids.sql.enabled" -> "true",
          "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
          "spark.rapids.sql.concurrentGpuTasks" -> "4")
      val dataprocWorkerInfo = buildGpuWorkerInfoAsString()
      val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
        logEventsProps, Some(testSparkVersion), scanStagesWithGpuOom = hasGpuOOm)
      val clusterPropsOpt = ProfilingAutoTunerConfigsProvider
        .loadClusterPropertiesFromContent(dataprocWorkerInfo)
      val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
      val autoTuner = ProfilingAutoTunerConfigsProvider
        .buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
      val (properties, comments) = autoTuner.getRecommendedProperties()
      val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
      // scalastyle:off line.size.limit
      val expectedResults =
        s"""|
            |Spark Properties:
            |--conf spark.dataproc.enhanced.execution.enabled=false
            |--conf spark.dataproc.enhanced.optimizer.enabled=false
            |--conf spark.executor.instances=8
            |--conf spark.executor.memory=32768m
            |--conf spark.executor.memoryOverhead=17612m
            |--conf spark.locality.wait=0
            |--conf spark.rapids.memory.pinnedPool.size=4096m
            |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
            |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
            |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
            |--conf spark.rapids.sql.batchSizeBytes=2147483647
            |--conf spark.rapids.sql.concurrentGpuTasks=2
            |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
            |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
            |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
            |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
            |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
            |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
            |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
            |--conf spark.sql.files.maxPartitionBytes=512m
            |
            |Comments:
            |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
            |- 'spark.dataproc.enhanced.execution.enabled' was not set.
            |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
            |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
            |- 'spark.executor.memoryOverhead' was not set.
            |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
            |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
            |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
            |- 'spark.rapids.sql.batchSizeBytes' was not set.
            |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
            |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
            |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
            |- 'spark.shuffle.manager' was not set.
            |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
            |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
            |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
            |- 'spark.sql.files.maxPartitionBytes' was not set.
            |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.jars.missing")}
            |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
            |""".stripMargin
      // scalastyle:on line.size.limit
      compareOutput(expectedResults, autoTunerOutput)
    }
  }

  test("test AutoTuner reduces maxPartitionBytes when scan stages have GPU OOM failures") {
    val eventLog = s"$profilingLogDir/gpu_oom_eventlog.zstd"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--csv",
        "--auto-tuner",
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)

      // Assert that the maxPartitionBytes was 10gb in the source GPU event log
      val sparkPropertiesFile = getOutputFilePath(tempDir, "spark_properties.csv")
      val sparkProperties = FSUtils.readFileContentAsUTF8(sparkPropertiesFile)
      assert(sparkProperties.contains("\"spark.sql.files.maxPartitionBytes\",\"10gb\""))

      // Compare the auto-tuner output to the expected results and assert that
      // the maxPartitionBytes is reduced.
      val logFile = getOutputFilePath(tempDir, "profile.log")
      val profileLogContent = FSUtils.readFileContentAsUTF8(logFile)
      val actualResults = extractAutoTunerResults(profileLogContent)

      // scalastyle:off line.size.limit
      val expectedResults =
        s"""|
            |Spark Properties:
            |--conf spark.rapids.sql.batchSizeBytes=2147483647
            |--conf spark.rapids.sql.enabled=true
            |--conf spark.sql.files.maxPartitionBytes=1851m
            |--conf spark.sql.shuffle.partitions=400
            |
            |Comments:
            |- 'spark.executor.cores' should be set to 16.
            |- 'spark.executor.instances' should be set to (cpuCoresPerNode * numWorkers) / 'spark.executor.cores'.
            |- 'spark.executor.memory' should be set to at least 2GB/core.
            |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
            |- 'spark.rapids.sql.batchSizeBytes' was not set.
            |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Min(4, (gpuMemory / 7.5G)).
            |- 'spark.rapids.sql.enabled' should be true to enable SQL operations on the GPU.
            |- 'spark.rapids.sql.enabled' was not set.
            |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
            |- 'spark.sql.shuffle.partitions' should be increased since spilling occurred in shuffle stages.
            |- 'spark.task.resource.gpu.amount' should be set to 0.001.
            |- Could not infer the cluster configuration, recommendations are generated using default values!
            |- ${ProfilingAutoTunerConfigsProvider.classPathComments("rapids.shuffle.jars")}
            |""".stripMargin.trim
      // scalastyle:on line.size.limit
      compareOutput(expectedResults, actualResults)
    }
  }
}
