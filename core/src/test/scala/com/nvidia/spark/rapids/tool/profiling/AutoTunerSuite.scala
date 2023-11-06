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

package com.nvidia.spark.rapids.tool.profiling

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.util.WebCrawlerUtil


class AppInfoProviderMockTest(val maxInput: Double,
    val spilledMetrics: Seq[Long],
    val jvmGCFractions: Seq[Double],
    val propsFromLog: mutable.Map[String, String],
    val sparkVersion: Option[String],
    val rapidsJars: Seq[String],
    val distinctLocationPct: Double,
    val redundantReadSize: Long) extends AppSummaryInfoBaseProvider {
  override def getMaxInput: Double = maxInput
  override def getSpilledMetrics: Seq[Long] = spilledMetrics
  override def getJvmGCFractions: Seq[Double] = jvmGCFractions
  override def getProperty(propKey: String): Option[String] = propsFromLog.get(propKey)
  override def getSparkVersion: Option[String] = sparkVersion
  override def getRapidsJars: Seq[String] = rapidsJars
  override def getDistinctLocationPct: Double = distinctLocationPct
  override def getRedundantReadSize: Long = redundantReadSize
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
      gpuMemory: Option[String] = Some("15109MiB"),
      gpuDevice: Option[String] = Some("T4")): String = {
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
      redundantReadSize: Long = 0): AppSummaryInfoBaseProvider = {
    new AppInfoProviderMockTest(maxInput, spilledMetrics, jvmGCFractions, propsFromLog,
      sparkVersion, rapidsJars, distinctLocationPct, redundantReadSize)
  }

  test("verify 3.2.0+ auto conf setting") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32), Some("122880MiB"), Some(0))
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      mutable.Map("spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.AnotherPlugin, com.nvidia.spark.SQLPlugin"),
      Some("3.2.0"), Seq())
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=2
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=8396m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=16
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=16
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=20
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
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
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
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Max(4, (gpuMemory / 8G)).
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).
          |- RAPIDS Accelerator for Apache Spark plugin jar is missing
          |  from the classpath entries.
          |  If the Spark RAPIDS jar is being bundled with your
          |  Spark distribution, this step is not needed.
          |- The RAPIDS Shuffle Manager requires spark.driver.extraClassPath
          |  and spark.executor.extraClassPath settings to include the
          |  path to the Spark RAPIDS plugin jar.
          |  If the Spark RAPIDS jar is being bundled with your Spark
          |  distribution, this step is not needed.
          |- java.io.FileNotFoundException: File non-existing.yaml does not exist
          |""".stripMargin
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
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Max(4, (gpuMemory / 8G)).
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).
          |- Incorrect values in worker system information: {numCores: 0, memory: 122880MiB, numWorkers: 4}.
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

  test("Load cluster properties with CPU memory missing") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32), None)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Max(4, (gpuMemory / 8G)).
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).
          |- Incorrect values in worker system information: {numCores: 32, memory: , numWorkers: 4}.
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
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Max(4, (gpuMemory / 8G)).
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).
          |- Incorrect values in worker system information: {numCores: 32, memory: 0m, numWorkers: 4}.
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
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=2
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=8396m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=16
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=16
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=20
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
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
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
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=32
          |--conf spark.executor.memory=65536m
          |--conf spark.executor.memoryOverhead=11673m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=32
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=32
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=32
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark311.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.03125
          |
          |Comments:
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
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
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
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
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
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
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- GPU device is missing. Setting default to T4.
          |- GPU memory is missing. Setting default to 15109m.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with unknown GPU device") {
    // with unknown gpu device, the memory won't be set correctly, then it should default to 16G
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
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- GPU memory is missing. Setting default to 15109m.
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
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
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
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    assert(expectedResults == autoTunerOutput)
  }

  test("test AutoTuner with empty sparkProperties" ) {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=8
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=8396m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=16
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=16
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=20
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
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getGpuAppMockInfoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
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
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=6758m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=160
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("No recommendations generated for CPU eventLogs when plugin set to the wrong values") {
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
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |
          |Comments:
          |- AutoTuner recommendations only support eventlogs generated by Spark applications utilizing RAPIDS Accelerator for Apache Spark
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://nvidia.github.io/spark-rapids/Getting-Started
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("No recommendations generated for CPU eventLogs when plugin not set") {
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
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |
          |Comments:
          |- AutoTuner recommendations only support eventlogs generated by Spark applications utilizing RAPIDS Accelerator for Apache Spark
          |- RAPIDS Accelerator for Apache Spark jar is missing in "spark.plugins". Please refer to https://nvidia.github.io/spark-rapids/Getting-Started
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("No recommendations generated for CPU eventLogs when plugin not enabled") {
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
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |
          |Comments:
          |- AutoTuner recommendations only support eventlogs generated by Spark applications utilizing RAPIDS Accelerator for Apache Spark
          |- Please enable Spark RAPIDS Accelerator for Apache Spark by setting spark.rapids.sql.enabled=true
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
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo,
      getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.01, 0.0), logEventsProps,
        Some(defaultSparkVersion)))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=6758m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
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
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(defaultSparkVersion))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=6758m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
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
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(defaultSparkVersion))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=6758m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
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
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo,
      getGpuAppMockInfoWithJars(rapidsJars))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    Profiler.getAutoTunerResultsAsString(properties, comments)
  }

  test("Multiple RAPIDS jars trigger a comment") {
    // 1. The Autotuner should warn the users that they have multiple jars defined in the classPath
    // 2. Compare the output
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- ${AutoTuner.classPathComments("rapids.jars.multiple")} [23.06.0, 23.02.1]
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val rapidsJarsArr = Seq("rapids-4-spark_2.12-23.06.0-SNAPSHOT.jar",
      "rapids-4-spark_2.12-23.02.1.jar")
    val autoTunerOutput = generateRecommendationsForRapidsJars(rapidsJarsArr)
    autoTunerOutput shouldBe expectedResults
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
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- A newer RAPIDS Accelerator for Apache Spark plugin is available:
          |  $pluginJarMvnURl
          |  Version used in application is $jarVer.
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val rapidsJarsArr = Seq(s"rapids-4-spark_2.12-$jarVer.jar")
    val autoTunerOutput = generateRecommendationsForRapidsJars(rapidsJarsArr)
    autoTunerOutput shouldBe expectedResults
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
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=128
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    val rapidsJarsArr = Seq(s"rapids-4-spark_2.12-$latestRelease.jar")
    val autoTunerOutput = generateRecommendationsForRapidsJars(rapidsJarsArr)
    autoTunerOutput shouldBe expectedResults
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
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(defaultSparkVersion), Seq(), 40.0, 200000000000L)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=6758m
          |--conf spark.rapids.filecache.enabled=true
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.filecache.enabled' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- Enable file cache only if Spark local disks bandwidth is > 1 GB/s
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
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some(defaultSparkVersion), Seq(), 40.0, 2000000L)
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=6758m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("test recommendations for databricks platform argument") {
    val databricksWorkerInfo = buildWorkerInfoAsString()
    val autoTuner = AutoTuner.buildAutoTunerFromProps(databricksWorkerInfo,
      getGpuAppMockInfoProvider, "databricks")
    val (properties, comments) = autoTuner.getRecommendedProperties()

    // Assert recommendations are excluded in properties
    assert(properties.map(_.property).forall(autoTuner.selectedPlatform.isValidRecommendation))
    // Assert recommendations are skipped in comments
    assert(comments.map(_.comment).forall(autoTuner.selectedPlatform.isValidComment))
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
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(defaultSparkVersion))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
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
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionNum=160
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.minPartitionNum' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- ${AutoTuner.classPathComments("rapids.jars.missing")}
          |- ${AutoTuner.classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }
}
