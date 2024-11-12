/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.nvidia.spark.rapids.tool.{A100Gpu, AppSummaryInfoBaseProvider, GpuDevice, PlatformFactory, PlatformNames, T4Gpu}
import com.nvidia.spark.rapids.tool.planparser.DatabricksParseHelper
import com.nvidia.spark.rapids.tool.profiling.AutoTuner.loadClusterPropertiesFromContent
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.TableFor4
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.util.WebCrawlerUtil


case class DriverInfoProviderMockTest(unsupportedOps: Seq[DriverLogUnsupportedOperators])
  extends BaseDriverLogInfoProvider(None) {
  override def getUnsupportedOperators: Seq[DriverLogUnsupportedOperators] = unsupportedOps
}

class AppInfoProviderMockTest(val maxInput: Double,
    val spilledMetrics: Seq[Long],
    val jvmGCFractions: Seq[Double],
    val propsFromLog: mutable.Map[String, String],
    val sparkVersion: Option[String],
    val rapidsJars: Seq[String],
    val distinctLocationPct: Double,
    val redundantReadSize: Long,
    val meanInput: Double,
    val meanShuffleRead: Double,
    val shuffleStagesWithPosSpilling: Set[Long],
    val shuffleSkewStages: Set[Long]) extends AppSummaryInfoBaseProvider {
  override def isAppInfoAvailable = true
  override def getMaxInput: Double = maxInput
  override def getMeanInput: Double = meanInput
  override def getMeanShuffleRead: Double = meanShuffleRead
  override def getSpilledMetrics: Seq[Long] = spilledMetrics
  override def getJvmGCFractions: Seq[Double] = jvmGCFractions
  override def getRapidsProperty(propKey: String): Option[String] = propsFromLog.get(propKey)
  override def getSparkProperty(propKey: String): Option[String] = propsFromLog.get(propKey)
  override def getSystemProperty(propKey: String): Option[String] = propsFromLog.get(propKey)
  override def getSparkVersion: Option[String] = sparkVersion
  override def getRapidsJars: Seq[String] = rapidsJars
  override def getDistinctLocationPct: Double = distinctLocationPct
  override def getRedundantReadSize: Long = redundantReadSize
  override def getShuffleStagesWithPosSpilling: Set[Long] = shuffleStagesWithPosSpilling
  override def getShuffleSkewStages: Set[Long] = shuffleSkewStages
}

class AutoTunerSuite extends FunSuite with BeforeAndAfterEach with Logging {

  val defaultSparkVersion = "3.1.1"

  val defaultDataprocProps: mutable.Map[String, String] = {
    mutable.LinkedHashMap[String, String](
      "spark.dynamicAllocation.enabled" -> "true",
      "spark.driver.maxResultSize" -> "7680m",
      "spark.driver.memory" -> "15360m",
      "spark.executor.cores" -> "16",
      "spark.executor.instances" -> "2",
      "spark.executor.resource.gpu.amount" -> "1",
      "spark.executor.memory" -> "26742m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.executorEnv.OPENBLAS_NUM_THREADS" -> "1",
      "spark.extraListeners" -> "com.google.cloud.spark.performance.DataprocMetricsListener",
      "spark.rapids.memory.pinnedPool.size" -> "2048m",
      "spark.scheduler.mode" -> "FAIR",
      "spark.sql.cbo.enabled" -> "true",
      "spark.sql.adaptive.enabled" -> "true",
      "spark.ui.port" -> "0",
      "spark.yarn.am.memory" -> "640m"
    )
  }

  private def buildWorkerInfoAsString(
      customProps: Option[mutable.Map[String, String]] = None,
      numCores: Option[Int] = Some(32),
      systemMemory: Option[String] = Some("122880MiB"),
      numWorkers: Option[Int] = Some(4),
      gpuCount: Option[Int] = Some(2),
      gpuMemory: Option[String] = Some(GpuDevice.DEFAULT.getMemory),
      gpuDevice: Option[String] = Some(GpuDevice.DEFAULT.toString)): String = {
    val gpuWorkerProps = new GpuWorkerProps(
      gpuMemory.getOrElse(""), gpuCount.getOrElse(0), gpuDevice.getOrElse(""))
    val cpuSystem = new SystemClusterProps(
      numCores.getOrElse(0), systemMemory.getOrElse(""), numWorkers.getOrElse(0))
    val systemProperties = customProps match {
      case None => mutable.Map[String, String]()
      case Some(newProps) => newProps
    }
    val convertedMap = new util.LinkedHashMap[String, String](systemProperties.asJava)
    val clusterProps = new ClusterProperties(cpuSystem, gpuWorkerProps, convertedMap)
    // set the options to convert the object into formatted yaml content
    val options = new DumperOptions()
    options.setIndent(2)
    options.setPrettyFlow(true)
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(options)
    val rawString = yaml.dump(clusterProps)
    // Skip the first line as it contains "the name of the class"
    rawString.split("\n").drop(1).mkString("\n")
  }

  private def getGpuAppMockInfoProvider: AppSummaryInfoBaseProvider = {
    getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some(defaultSparkVersion), Seq())
  }

  private def getGpuAppMockInfoWithJars(rapidsJars: Seq[String]): AppSummaryInfoBaseProvider = {
    getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some(defaultSparkVersion), rapidsJars)
  }

  private def getMockInfoProvider(maxInput: Double,
      spilledMetrics: Seq[Long],
      jvmGCFractions: Seq[Double],
      propsFromLog: mutable.Map[String, String],
      sparkVersion: Option[String],
      rapidsJars: Seq[String] = Seq(),
      distinctLocationPct: Double = 0.0,
      redundantReadSize: Long = 0,
      meanInput: Double = 0.0,
      meanShuffleRead: Double = 0.0,
      shuffleStagesWithPosSpilling: Set[Long] = Set(),
      shuffleSkewStages: Set[Long] = Set()): AppSummaryInfoBaseProvider = {
    new AppInfoProviderMockTest(maxInput, spilledMetrics, jvmGCFractions, propsFromLog,
      sparkVersion, rapidsJars, distinctLocationPct, redundantReadSize, meanInput, meanShuffleRead,
      shuffleStagesWithPosSpilling, shuffleSkewStages)
  }

  test("verify 3.2.0+ auto conf setting") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32), Some("122880MiB"), Some(0))
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some("3.2.0"), Seq())
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=2
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
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
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.0625
          |
          |Comments:
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
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionSize' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- Number of workers is missing. Setting default to 1.
          |- RAPIDS Accelerator for Apache Spark plugin jar is missing
          |  from the classpath entries.
          |  If the Spark RAPIDS jar is being bundled with your
          |  Spark distribution, this step is not needed.
          |- The RAPIDS Shuffle Manager requires spark.driver.extraClassPath
          |  and spark.executor.extraClassPath settings to include the
          |  path to the Spark RAPIDS plugin jar.
          |  If the Spark RAPIDS jar is being bundled with your Spark
          |  distribution, this step is not needed.
          |""".stripMargin
    assert(autoTunerOutput == expectedResults)
  }

  test("Load non-existing cluster properties") {
    val autoTuner = AutoTuner.buildAutoTuner("non-existing.yaml", getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Min(4, (gpuMemory / 7.5G)).
          |- 'spark.rapids.sql.enabled' should be true to enable SQL operations on the GPU.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' should be set to Min(1, (gpuCount / numCores)).
          |- Could not infer the cluster configuration, recommendations are generated using default values!
          |- RAPIDS Accelerator for Apache Spark plugin jar is missing
          |  from the classpath entries.
          |  If the Spark RAPIDS jar is being bundled with your
          |  Spark distribution, this step is not needed.
          |- The RAPIDS Shuffle Manager requires spark.driver.extraClassPath
          |  and spark.executor.extraClassPath settings to include the
          |  path to the Spark RAPIDS plugin jar.
          |  If the Spark RAPIDS jar is being bundled with your Spark
          |  distribution, this step is not needed.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with CPU cores 0") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(0))
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Min(4, (gpuMemory / 7.5G)).
          |- 'spark.rapids.sql.enabled' should be true to enable SQL operations on the GPU.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' should be set to Min(1, (gpuCount / numCores)).
          |- Could not infer the cluster configuration, recommendations are generated using default values!
          |- RAPIDS Accelerator for Apache Spark plugin jar is missing
          |  from the classpath entries.
          |  If the Spark RAPIDS jar is being bundled with your
          |  Spark distribution, this step is not needed.
          |- The RAPIDS Shuffle Manager requires spark.driver.extraClassPath
          |  and spark.executor.extraClassPath settings to include the
          |  path to the Spark RAPIDS plugin jar.
          |  If the Spark RAPIDS jar is being bundled with your Spark
          |  distribution, this step is not needed.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with memory to cores ratio to small") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(8), Some("14000MiB"))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo,
      getGpuAppMockInfoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedComment =
      s"""This node/worker configuration is not ideal for using the Spark Rapids
 Accelerator because it doesn't have enough memory for the executors.
 We recommend using nodes/workers with more memory. Need at least 7796MB memory.""".stripMargin.replaceAll("\n", "")
    // scalastyle:on line.size.limit
    assert(autoTunerOutput.replaceAll("\n", "").contains(expectedComment))
  }

  test("Load cluster properties with CPU memory missing") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32), None)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Min(4, (gpuMemory / 7.5G)).
          |- 'spark.rapids.sql.enabled' should be true to enable SQL operations on the GPU.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' should be set to Min(1, (gpuCount / numCores)).
          |- Could not infer the cluster configuration, recommendations are generated using default values!
          |- RAPIDS Accelerator for Apache Spark plugin jar is missing
          |  from the classpath entries.
          |  If the Spark RAPIDS jar is being bundled with your
          |  Spark distribution, this step is not needed.
          |- The RAPIDS Shuffle Manager requires spark.driver.extraClassPath
          |  and spark.executor.extraClassPath settings to include the
          |  path to the Spark RAPIDS plugin jar.
          |  If the Spark RAPIDS jar is being bundled with your Spark
          |  distribution, this step is not needed.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with CPU memory 0") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32), Some("0m"))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=8
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.0625
          |
          |Comments:
          |- 'spark.executor.cores' was not set.
          |- 'spark.executor.instances' was not set.
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
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
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- RAPIDS Accelerator for Apache Spark plugin jar is missing
          |  from the classpath entries.
          |  If the Spark RAPIDS jar is being bundled with your
          |  Spark distribution, this step is not needed.
          |- The RAPIDS Shuffle Manager requires spark.driver.extraClassPath
          |  and spark.executor.extraClassPath settings to include the
          |  path to the Spark RAPIDS plugin jar.
          |  If the Spark RAPIDS jar is being bundled with your Spark
          |  distribution, this step is not needed.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with number of workers 0") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32), Some("122880MiB"), Some(0))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=2
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=32
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.0625
          |
          |Comments:
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
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- Number of workers is missing. Setting default to 1.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
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
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(0))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=32
          |--conf spark.executor.instances=4
          |--conf spark.executor.memory=65536m
          |--conf spark.executor.memoryOverhead=20889m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=48
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=48
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=64
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.03125
          |
          |Comments:
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
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- GPU count is missing. Setting default to 1.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
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
      "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo =
      buildWorkerInfoAsString(Some(sparkProps), Some(32), Some("122880MiB"), Some(4), Some(2), None)
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- GPU memory is missing. Setting default to 15109m.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
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
      "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "64m",
      "spark.sql.adaptive.coalescePartitions.minPartitionNum" -> "1")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(2), Some("0M"))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- GPU memory is missing. Setting default to 15109m.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
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
      "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(2), Some("0MiB"), None)
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- GPU device is missing. Setting default to $T4Gpu.
          |- GPU memory is missing. Setting default to ${T4Gpu.getMemory}.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
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
      "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(2), Some("0MiB"), Some("GPU-X"))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- GPU device is missing. Setting default to $T4Gpu.
          |- GPU memory is missing. Setting default to ${T4Gpu.getMemory}.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
  }

  test("Test executor memory on CSP where executor/cpu ratio is small") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(8), Some("15360MiB"),
      Some(4), Some(1))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=4
          |--conf spark.executor.memory=6000m
          |--conf spark.executor.memoryOverhead=11776m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=32
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.cores' was not set.
          |- 'spark.executor.instances' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
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
      "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps))
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    assert(expectedResults == autoTunerOutput)
  }

  // This mainly to test that the executorInstances will be calculated when the dynamic allocation
  // is missing.
  test("test T4 dataproc cluster with missing dynamic allocation") {
    val customProps = mutable.LinkedHashMap(
      "spark.dynamicAllocation.enabled" -> "false",
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "8396m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "16",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "16",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps))
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    assert(expectedResults == autoTunerOutput)
  }

  test("test AutoTuner with empty sparkProperties") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=8
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.0625
          |
          |Comments:
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
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    assert(expectedResults == autoTunerOutput)
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
      "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
      "spark.task.resource.gpu.amount" -> "0.0625")
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
        "spark.task.resource.gpu.amount" -> "0.0625",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=160
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |- file.encoding should be [UTF-8] because GPU only supports the charset when using some expressions.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }
  // Test that the properties from the eventlogs will be used to calculate the recommendations.
  // For example, the output should recommend "spark.executor.cores" -> 8. Although the cluster
  // "spark.executor.cores" is the same as the recommended value, the property is set to 16 in
  // the eventlogs.
  test("AutoTuner gives precedence to properties from eventlogs") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
      "spark.task.resource.gpu.amount" -> "0.0625")
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
        "spark.task.resource.gpu.amount" -> "0.0625",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=160
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("plugin set to the wrong values") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
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
        "spark.task.resource.gpu.amount" -> "0.0625",
        "spark.plugins" -> "com.nvidia.spark.WrongPlugin0, com.nvidia.spark.WrongPlugin1",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=160
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  // Why this test to see if it carries over the sql.enabled false???? This is broken
  // if needed
  test("plugin not enabled") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
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
        "spark.task.resource.gpu.amount" -> "0.0625",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "false",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=160
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  // Changing the maxInput of tasks should reflect on the maxPartitions recommendations.
  // Values used in setting the properties are taken from sample eventlogs.
  test("Recommendation of maxPartitions is calculated based on maxInput of tasks") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.adaptive.coalescePartitions.minPartitionNum" -> "1",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo,
      getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.01, 0.0), logEventsProps,
        Some(defaultSparkVersion)), platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(autoTunerOutput == expectedResults)
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
      "spark.task.resource.gpu.amount" -> "0.0625")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.adaptive.coalescePartitions.minPartitionNum" -> "1",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(defaultSparkVersion))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
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
        "spark.sql.adaptive.coalescePartitions.minPartitionNum" -> "1",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(defaultSparkVersion))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
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
      "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo,
      getGpuAppMockInfoWithJars(rapidsJars), platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    Profiler.getAutoTunerResultsAsString(properties, comments)
  }

  test("Multiple RAPIDS jars trigger a comment") {
    // 1. The Autotuner should warn the users that they have multiple jars defined in the classPath
    // 2. Compare the output
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- ${AutoTuner.classPathComments("rapids.jars.multiple")} [23.06.0, 23.02.1]
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val rapidsJarsArr = Seq("rapids-4-spark_2.12-23.06.0-SNAPSHOT.jar",
      "rapids-4-spark_2.12-23.02.1.jar")
    val autoTunerOutput = generateRecommendationsForRapidsJars(rapidsJarsArr)
    assert(expectedResults == autoTunerOutput)
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
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- A newer RAPIDS Accelerator for Apache Spark plugin is available:
          |  $pluginJarMvnURl
          |  Version used in application is $jarVer.
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val rapidsJarsArr = Seq(s"rapids-4-spark_2.12-$jarVer.jar")
    val autoTunerOutput = generateRecommendationsForRapidsJars(rapidsJarsArr)
    assert(expectedResults == autoTunerOutput)
  }

  test("No recommendation when the jar pluginJar is up-to-date") {
    // 1. Pull the latest release from mvn.
    // 2. The Autotuner finds tha the jar version is latest. No comments should be added
    // 3. Compare the output
    val latestRelease = WebCrawlerUtil.getLatestPluginRelease match {
      case Some(v) => v
      case None => fail("Could not find pull the latest release successfully")
    }
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.executor.memoryOverhead=17612m
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10485760
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val rapidsJarsArr = Seq(s"rapids-4-spark_2.12-$latestRelease.jar")
    val autoTunerOutput = generateRecommendationsForRapidsJars(rapidsJarsArr)
    assert(expectedResults == autoTunerOutput)
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
        "spark.sql.adaptive.coalescePartitions.minPartitionNum" -> "1",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(defaultSparkVersion), Seq(), 40.0, 200000000000L)
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.filecache.enabled=true
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.filecache.enabled' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- Enable file cache only if Spark local disks bandwidth is > 1 GB/s and you have sufficient disk space available to fit both cache and normal Spark temporary data.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
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
        "spark.sql.adaptive.coalescePartitions.minPartitionNum" -> "1",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(defaultSparkVersion), Seq(), 40.0, 2000000L)
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("test recommendations for databricks-aws platform argument") {
    val databricksWorkerInfo = buildWorkerInfoAsString()
    val clusterPropsOpt = loadClusterPropertiesFromContent(databricksWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATABRICKS_AWS, clusterPropsOpt)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(databricksWorkerInfo,
      getGpuAppMockInfoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()

    // Assert recommendations are excluded in properties
    assert(properties.map(_.property).forall(autoTuner.platform.isValidRecommendation))
    // Assert recommendations are skipped in comments
    assert(comments.map(_.comment).forall(autoTuner.platform.isValidComment))
  }

  // When spark is running as a standalone, the memoryOverhead should not be listed as a
  // recommendation: issue-553.
  test("memoryOverhead should not be recommended for Spark Standalone") {
    // This UT sets a custom spark-property "spark.master" pointing to a spark-standalone value
    // The Autotuner should detects that the spark-master is standalone and refrains from
    // recommending memoryOverhead value
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
      "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
      "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
      "spark.task.resource.gpu.amount" -> "0.0625")
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
        "spark.task.resource.gpu.amount" -> "0.0625",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
      |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=160
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("Recommendations generated for unsupported operators from driver logs only") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val unsupportedDriverOperators = Seq(
      DriverLogUnsupportedOperators(
        "FromUnixTime", 1,
        "Only UTC zone id is supported. Actual default zone id: America/Los_Angeles; " +
          "CORRECTED format 'yyyyMMdd' on the GPU is not guaranteed to produce the same " +
          "results as Spark on CPU. Set spark.rapids.sql.incompatibleDateFormats.enabled=true " +
          "to force onto GPU.")
    )
    val driverInfoProvider = DriverInfoProviderMockTest(unsupportedDriverOperators)
    val workerInfo = buildWorkerInfoAsString(Some(customProps))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(workerInfo,
      AppSummaryInfoBaseProvider.fromAppInfo(None),
      PlatformFactory.createInstance(), driverInfoProvider)
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
          |- ${AutoTuner.commentForExperimentalConfig("spark.rapids.sql.incompatibleDateFormats.enabled")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("Recommendations generated for unsupported operators from driver and event logs") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val unsupportedDriverOperators = Seq(
      DriverLogUnsupportedOperators(
        "FromUnixTime", 1,
        "Only UTC zone id is supported. Actual default zone id: America/Los_Angeles; " +
          "CORRECTED format 'yyyyMMdd' on the GPU is not guaranteed to produce the same " +
          "results as Spark on CPU. Set spark.rapids.sql.incompatibleDateFormats.enabled=true " +
          "to force onto GPU.")
    )
    val driverInfoProvider = DriverInfoProviderMockTest(unsupportedDriverOperators)
    val workerInfo = buildWorkerInfoAsString(Some(customProps))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(workerInfo,
      getGpuAppMockInfoProvider, PlatformFactory.createInstance(), driverInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.incompatibleDateFormats.enabled=true
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Min(4, (gpuMemory / 7.5G)).
          |- 'spark.rapids.sql.enabled' should be true to enable SQL operations on the GPU.
          |- 'spark.rapids.sql.incompatibleDateFormats.enabled' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' should be set to Min(1, (gpuCount / numCores)).
          |- Could not infer the cluster configuration, recommendations are generated using default values!
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |- ${AutoTuner.commentForExperimentalConfig("spark.rapids.sql.incompatibleDateFormats.enabled")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("Recommendations generated for empty unsupported operators from driver logs only") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val workerInfo = buildWorkerInfoAsString(Some(customProps))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(workerInfo,
      AppSummaryInfoBaseProvider.fromAppInfo(None),
      PlatformFactory.createInstance())
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("Recommendations not generated for unsupported operators from driver logs") {
    // This test does not generate any recommendations for the unsupported operator 'Literal'
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val unsupportedDriverOperators = Seq(
      DriverLogUnsupportedOperators(
        "Literal", 3,
        "expression Literal 1700518632630000 produces an unsupported type TimestampType")
    )
    val driverInfoProvider = DriverInfoProviderMockTest(unsupportedDriverOperators)
    val workerInfo = buildWorkerInfoAsString(Some(customProps))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(workerInfo,
      AppSummaryInfoBaseProvider.fromAppInfo(None),
      PlatformFactory.createInstance(), driverInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("AQE configuration autoBroadcastJoinThreshold should not be GTE 100mb") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
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
        "spark.task.resource.gpu.amount" -> "0.0625",
        "spark.executor.memoryOverhead" -> "7372m",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4",
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "500mb")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
      |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=160
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- Setting 'spark.sql.adaptive.autoBroadcastJoinThreshold' > 100m could lead to performance\n  regression. Should be set to a lower number.
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
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
      "spark.task.resource.gpu.amount" -> "0.0625")
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
        "spark.task.resource.gpu.amount" -> "0.0625",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(gpuDevice.getMemory), Some(gpuDevice.toString))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion), meanInput = inputSize, meanShuffleRead = shuffleRead)
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
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
        "spark.sql.adaptive.coalescePartitions.minPartitionNum" -> "1",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.rapids.sql.multiThreadedRead.numThreads" -> "20",
        "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(defaultSparkVersion))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.databricks.adaptive.autoOptimizeShuffle.enabled=false
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("test shuffle manager version for databricks") {
    val databricksWorkerInfo = buildWorkerInfoAsString(None)
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin",
        DatabricksParseHelper.PROP_TAG_CLUSTER_SPARK_VERSION_KEY -> "11.3.x-gpu-ml-scala2.12"),
      Some("3.3.0"), Seq())
    // Do not set the platform as DB to see if it can work correctly irrespective
    val autoTuner = AutoTuner.buildAutoTunerFromProps(databricksWorkerInfo,
      infoProvider, PlatformFactory.createInstance())
    val smVersion = autoTuner.getShuffleManagerClassName()
    // Assert shuffle manager string for DB 11.3 tag
    assert(smVersion.get == "com.nvidia.spark.rapids.spark330db.RapidsShuffleManager")
  }

  test("test shuffle manager version for non-databricks") {
    val databricksWorkerInfo = buildWorkerInfoAsString(None)
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some("3.3.0"), Seq())
    val autoTuner = AutoTuner.buildAutoTunerFromProps(databricksWorkerInfo,
      infoProvider, PlatformFactory.createInstance())
    val smVersion = autoTuner.getShuffleManagerClassName()
    assert(smVersion.get == "com.nvidia.spark.rapids.spark330.RapidsShuffleManager")
  }

  test("Test spilling occurred in shuffle stages") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.0625",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.sql.adaptive.coalescePartitions.minPartitionNum" -> "1",
        "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(1000L, 1000L), Seq(0.4, 0.4),
      logEventsProps, Some(defaultSparkVersion), shuffleStagesWithPosSpilling = Set(1))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.sql.shuffle.partitions=400
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.shuffle.partitions' should be increased since spilling occurred in shuffle stages.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("Test spilling occurred in shuffle stages with shuffle skew") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.0625",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "4",
        "spark.rapids.shuffle.multiThreaded.reader.threads" -> "8",
        "spark.rapids.shuffle.multiThreaded.writer.threads" -> "8",
        "spark.sql.adaptive.coalescePartitions.minPartitionNum" -> "1",
        "spark.shuffle.manager" -> "com.nvidia.spark.rapids.spark311.RapidsShuffleManager")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(1000L, 1000L), Seq(0.4, 0.4),
      logEventsProps, Some(defaultSparkVersion), shuffleStagesWithPosSpilling = Set(1, 5),
      shuffleSkewStages = Set(1))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- Shuffle skew exists (when task's Shuffle Read Size > 3 * Avg Stage-level size) in
          |  stages with spilling. Increasing shuffle partitions is not recommended in this
          |  case since keys will still hash to the same task.
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
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
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some(defaultSparkVersion))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=160
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.kryo.registrator' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
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
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some(defaultSparkVersion))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.kryo.registrator=org.apache.SomeRegistrator,org.apache.SomeOtherRegistrator,com.nvidia.spark.rapids.GpuKryoRegistrator
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=160
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
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
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32),
      Some("212992MiB"), Some(5), Some(4), Some(T4Gpu.getMemory), Some(T4Gpu.toString))
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some(defaultSparkVersion))
    val clusterPropsOpt = loadClusterPropertiesFromContent(dataprocWorkerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, clusterPropsOpt)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider,
      platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=11878m
          |--conf spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=160
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }
}
