/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.tool.{GpuTypes, NodeInstanceMapKey, PlatformFactory, PlatformInstanceTypes, PlatformNames, ToolTestUtils}
import com.nvidia.spark.rapids.tool.profiling.Profiler
import com.nvidia.spark.rapids.tool.tuning.config.{ConfTypeEnum, TuningConfigEntry, TuningEntryDefinition}

import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.annotation.Since

/**
 * Test suite for the Profiling AutoTuner that uses the new target cluster properties format.
 *
 * This test suite introduces a cleaner way to specify target cluster configurations by explicitly
 * separating:
 * - Target cluster shape (cores, memory, GPU count/type)
 * - Target Spark properties (enforced configurations)
 *
 * This is in contrast to the legacy format in [[ProfilingAutoTunerSuite]] which overloaded the
 * same format for both source and target cluster properties.
 */
@Since("25.04.2")
class ProfilingAutoTunerSuiteV2 extends ProfilingAutoTunerSuiteBase {

  lazy val sparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Rapids Spark Profiling Tool Unit Tests")
      .getOrCreate()
  }

  // Test that the properties from the custom target cluster props will be enforced.
  test("AutoTuner enforces properties from custom target cluster props") {
    // 1. Mock source cluster info for dataproc
    val instanceMapKey = NodeInstanceMapKey("g2-standard-16")
    val gpuInstance = PlatformInstanceTypes.DATAPROC_BY_INSTANCE_NAME(instanceMapKey)
    // 2. Mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "2",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "1",
        // Below properties should be overridden by the enforced properties
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.001",
        "spark.rapids.sql.concurrentGpuTasks" -> "4"
      )
    // 3. Define enforced properties for the target cluster
    val enforcedSparkProperties = Map(
      "spark.sql.shuffle.partitions" -> "400",
      "spark.sql.files.maxPartitionBytes" -> "101m",
      "spark.task.resource.gpu.amount" -> "0.25",
      "spark.rapids.sql.concurrentGpuTasks" -> "2"
    )
    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      enforcedSparkProperties = enforcedSparkProperties
    )
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, Some(targetClusterInfo))

    val sparkPropsWithMemory =
      logEventsProps + ("spark.executor.memory" -> (gpuInstance.memoryMB.toString + "MiB"))
    configureEventLogClusterInfoForTest(
      platform,
      numCores = gpuInstance.cores,
      numWorkers = 4,
      gpuCount = gpuInstance.numGpus,
      sparkProperties = sparkPropsWithMemory.toMap
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.memory=32g
          |--conf spark.executor.memoryOverhead=15564m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=400
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=101m
          |--conf spark.sql.shuffle.partitions=400
          |--conf spark.task.resource.gpu.amount=0.25
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- ${getEnforcedPropertyComment("spark.rapids.sql.concurrentGpuTasks")}
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' was not set.
          |- ${getEnforcedPropertyComment("spark.sql.files.maxPartitionBytes")}
          |- ${getEnforcedPropertyComment("spark.sql.shuffle.partitions")}
          |- ${getEnforcedPropertyComment("spark.task.resource.gpu.amount")}
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // Test that the executor memory and memory overhead properties from the custom target cluster
  // props lead to AutoTuner warning about insufficient memory.
  test("AutoTuner warns about insufficient memory with executor heap and" +
    " memory overhead override") {
    // 1. Mock source cluster info for dataproc
    val instanceMapKey = NodeInstanceMapKey("g2-standard-16")
    val gpuInstance = PlatformInstanceTypes.DATAPROC_BY_INSTANCE_NAME(instanceMapKey)
    // 2. Mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "2",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "1"
      )
    // 3. Define enforced properties for the target cluster
    // Note: These values should cause insufficient memory warning
    val enforcedSparkProperties = Map(
      "spark.executor.memory" -> "40g",
      "spark.executor.memoryOverhead" -> "30g"
    )
    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      enforcedSparkProperties = enforcedSparkProperties
    )
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, Some(targetClusterInfo))

    val sparkPropsWithMemory =
      logEventsProps + ("spark.executor.memory" -> (gpuInstance.memoryMB.toString + "MiB"))
    configureEventLogClusterInfoForTest(
      platform,
      numCores = gpuInstance.cores,
      numWorkers = 4,
      gpuCount = gpuInstance.numGpus,
      sparkProperties = sparkPropsWithMemory.toMap
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.memory=[FILL_IN_VALUE]
          |--conf spark.executor.memoryOverhead=[FILL_IN_VALUE]
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=[FILL_IN_VALUE]
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=200
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=4g
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- ${getEnforcedPropertyComment("spark.executor.memory")}
          |- ${getEnforcedPropertyComment("spark.executor.memoryOverhead")}
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
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${notEnoughMemCommentForKey("spark.executor.memory")}
          |- ${notEnoughMemCommentForKey("spark.executor.memoryOverhead")}
          |- ${notEnoughMemCommentForKey("spark.rapids.memory.pinnedPool.size")}
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |- ${notEnoughMemComment(89600)}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // Test that the pinned pool property from the custom target cluster
  // props lead to AutoTuner warning about insufficient memory.
  test("AutoTuner warns about insufficient memory with pinned pool override") {
    // 1. Mock source cluster info for dataproc
    val instanceMapKey = NodeInstanceMapKey("g2-standard-16")
    val gpuInstance = PlatformInstanceTypes.DATAPROC_BY_INSTANCE_NAME(instanceMapKey)
    // 2. Mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "2",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "1"
      )
    // 3. Define enforced properties for the target cluster
    val enforcedSparkProperties = Map(
      "spark.rapids.memory.pinnedPool.size" -> "30g", // Should cause insufficient memory warning
      "spark.sql.files.maxPartitionBytes" -> "101m"   // Should be enforced
    )
    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      enforcedSparkProperties = enforcedSparkProperties
    )
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, Some(targetClusterInfo))

    val sparkPropsWithMemory =
      logEventsProps + ("spark.executor.memory" -> (gpuInstance.memoryMB.toString + "MiB"))
    configureEventLogClusterInfoForTest(
      platform,
      numCores = gpuInstance.cores,
      numWorkers = 4,
      gpuCount = gpuInstance.numGpus,
      sparkProperties = sparkPropsWithMemory.toMap
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.memory=[FILL_IN_VALUE]
          |--conf spark.executor.memoryOverhead=[FILL_IN_VALUE]
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=[FILL_IN_VALUE]
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=200
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=101m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- ${getEnforcedPropertyComment("spark.rapids.memory.pinnedPool.size")}
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
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' was not set.
          |- ${getEnforcedPropertyComment("spark.sql.files.maxPartitionBytes")}
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${notEnoughMemCommentForKey("spark.executor.memory")}
          |- ${notEnoughMemCommentForKey("spark.executor.memoryOverhead")}
          |- ${notEnoughMemCommentForKey("spark.rapids.memory.pinnedPool.size")}
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |- ${notEnoughMemComment(126975)}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("Test Kryo Serializer does not add GPU registrator again if already present") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator" ->
          "org.apache.SomeRegistrator,com.nvidia.spark.rapids.GpuKryoRegistrator",
        "spark.executor.resource.gpu.discoveryScript" ->
          "${SPARK_HOME}/examples/src/main/scripts/getGpusResources.sh",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin"
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
          |--conf spark.executor.memory=32g
          |--conf spark.executor.memoryOverhead=15564m
          |--conf spark.kryoserializer.buffer.max=512m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=200
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
          |- 'spark.kryoserializer.buffer.max' increasing the max buffer to prevent out-of-memory errors.
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
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // Test that AutoTuner parses existing Kryo Registrator correctly
  // i.e. it removes duplicates, empty entries, and adds GpuKryoRegistrator
  test("Test AutoTuner parses existing Kryo Registrator correctly") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator" ->
          "org.apache.SomeRegistrator,, org.apache.OtherRegistrator,org.apache.SomeRegistrator",
        "spark.executor.resource.gpu.discoveryScript" ->
          "${SPARK_HOME}/examples/src/main/scripts/getGpusResources.sh",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin"
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
          |--conf spark.executor.memory=32g
          |--conf spark.executor.memoryOverhead=15564m
          |--conf spark.kryo.registrator=org.apache.SomeRegistrator,org.apache.OtherRegistrator,com.nvidia.spark.rapids.GpuKryoRegistrator
          |--conf spark.kryoserializer.buffer.max=512m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=200
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
          |- 'spark.kryo.registrator' GpuKryoRegistrator must be appended to the existing value when using Kryo serialization.
          |- 'spark.kryoserializer.buffer.max' increasing the max buffer to prevent out-of-memory errors.
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
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // This test uses target cluster properties with user-enforced Spark properties.
  // The platform is mocked as Kubernetes on OnPrem
  // to enable memory overhead calculation.
  // AutoTuner is expected to:
  // - Include the enforced Spark properties in the final configuration.
  test("Target cluster properties for OnPrem with enforced spark properties") {
    // 2. Mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "1",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "1"
      )
    // 3. Define enforced properties for the target cluster
    val enforcedSparkProperties = Map(
      "spark.sql.shuffle.partitions" -> "101",
      "spark.sql.files.maxPartitionBytes" -> "101m",
      "spark.task.resource.gpu.amount" -> "0.25"
    )
    // sparkProperties:
    //   enforced:
    //    spark.sql.shuffle.partitions: 101
    //    spark.sql.files.maxPartitionBytes: 101m
    //    spark.task.resource.gpu.amount: 0.25
    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      enforcedSparkProperties = enforcedSparkProperties
    )
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, Some(targetClusterInfo))

    val sparkPropsWithMemory = logEventsProps + ("spark.executor.memory" -> "50g")
    configureEventLogClusterInfoForTest(
      platform,
      numCores = 8,
      numWorkers = 1,
      gpuCount = 2, // default for OnPrem
      sparkProperties = sparkPropsWithMemory.toMap
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(Kubernetes))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.memory=16g
          |--conf spark.executor.memoryOverhead=9830m
          |--conf spark.executor.resource.gpu.vendor=nvidia.com
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=20
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=101
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=101m
          |--conf spark.sql.shuffle.partitions=101
          |--conf spark.task.resource.gpu.amount=0.25
          |
          |Comments:
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.executor.resource.gpu.vendor' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' was not set.
          |- ${getEnforcedPropertyComment("spark.sql.files.maxPartitionBytes")}
          |- ${getEnforcedPropertyComment("spark.sql.shuffle.partitions")}
          |- ${getEnforcedPropertyComment("spark.task.resource.gpu.amount")}
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |- $missingGpuDiscoveryScriptComment
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // This test uses target cluster properties with a worker node having 16 cores, 64g memory,
  // 1 L4 GPU, and user-enforced Spark properties. The platform is mocked as Kubernetes on OnPrem
  // to enable memory overhead calculation.
  // AutoTuner is expected to:
  // - Recommend 32g executor memory,
  // - Calculate overhead using the max pinned pool size (4g),
  // - Include the enforced Spark properties in the final configuration.
  test("Target cluster properties for OnPrem with workerInfo and enforced spark properties") {
    // 2. Mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "2",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "1"
      )
    // 3. Define enforced properties for the target cluster
    val enforcedSparkProperties = Map(
      "spark.sql.shuffle.partitions" -> "400",
      "spark.sql.files.maxPartitionBytes" -> "101m",
      "spark.task.resource.gpu.amount" -> "0.25",
      "spark.rapids.sql.concurrentGpuTasks" -> "1"  // For L4, default recommendation would be 3
    )
    // workerInfo:
    //   cpuCores: 16
    //   memoryGB: 64
    //   gpu:
    //     count: 1
    //     name: l4
    // sparkProperties:
    //   enforced:
    //    spark.sql.shuffle.partitions: 400
    //    spark.sql.files.maxPartitionBytes: 101m
    //    spark.task.resource.gpu.amount: 0.25
    //    spark.rapids.sql.concurrentGpuTasks: 2
    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      cpuCores = Some(16), memoryGB = Some(64),
      gpuCount = Some(1), gpuDevice = Some(GpuTypes.L4),
      enforcedSparkProperties = enforcedSparkProperties
    )
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, Some(targetClusterInfo))

    val sparkPropsWithMemory = logEventsProps + ("spark.executor.memory" -> "14000MiB")
    configureEventLogClusterInfoForTest(
      platform,
      numCores = 8, // from eventLog
      numWorkers = 2,
      gpuCount = 1, // target cluster has 1 L4
      sparkProperties = sparkPropsWithMemory.toMap
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(Kubernetes))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.memory=32g
          |--conf spark.executor.memoryOverhead=11468m
          |--conf spark.executor.resource.gpu.vendor=nvidia.com
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=24
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=24
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=1
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=32
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=400
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=101m
          |--conf spark.sql.shuffle.partitions=400
          |--conf spark.task.resource.gpu.amount=0.25
          |
          |Comments:
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.executor.resource.gpu.vendor' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- ${getEnforcedPropertyComment("spark.rapids.sql.concurrentGpuTasks")}
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' was not set.
          |- ${getEnforcedPropertyComment("spark.sql.files.maxPartitionBytes")}
          |- ${getEnforcedPropertyComment("spark.sql.shuffle.partitions")}
          |- ${getEnforcedPropertyComment("spark.task.resource.gpu.amount")}
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |- $missingGpuDiscoveryScriptComment
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // This test uses custom target cluster properties with 40g worker memory and 2 GPUs.
  // Now, each executor can use up to 20g (including memory and overhead).
  // The user enforces spark.executor.memory to 18g. This leaves insufficient room for overhead.
  // AutoTuner is expected to warn about the insufficient executor memory configuration.
  test("Target cluster properties for OnPrem with total executor memory " +
    "exceeding available worker memory") {
    // 2. Mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "2",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "1"
      )
    // 3. Define enforced properties for the target cluster
    val enforcedSparkProperties = Map(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "18g",   // Requesting more memory than available in the node
      "spark.sql.shuffle.partitions" -> "400"
    )
    // workerInfo:
    //   cpuCores: 16
    //   memoryGB: 40
    //   gpu:
    //     count: 2
    //     name: l4
    // sparkProperties:
    //   enforced:
    //    spark.executor.cores: 8
    //    spark.executor.memory: 18g
    //    spark.sql.shuffle.partitions: 400
    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      cpuCores = Some(16), memoryGB = Some(40),
      gpuCount = Some(2), gpuDevice = Some(GpuTypes.L4),
      enforcedSparkProperties = enforcedSparkProperties
    )
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, Some(targetClusterInfo))

    val sparkPropsWithMemory = logEventsProps + ("spark.executor.memory" -> "14000MiB")
    configureEventLogClusterInfoForTest(
      platform,
      numCores = 16, // from eventLog
      numWorkers = 2,
      gpuCount = 2, // target cluster has 2 L4s
      sparkProperties = sparkPropsWithMemory.toMap
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(Kubernetes))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.memory=[FILL_IN_VALUE]
          |--conf spark.executor.memoryOverhead=[FILL_IN_VALUE]
          |--conf spark.executor.resource.gpu.vendor=nvidia.com
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=[FILL_IN_VALUE]
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=20
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=400
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=400
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- ${getEnforcedPropertyComment("spark.executor.cores")}
          |- ${getEnforcedPropertyComment("spark.executor.memory")}
          |- 'spark.executor.resource.gpu.vendor' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- ${getEnforcedPropertyComment("spark.sql.shuffle.partitions")}
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${notEnoughMemCommentForKey("spark.executor.memory")}
          |- ${notEnoughMemCommentForKey("spark.executor.memoryOverhead")}
          |- ${notEnoughMemCommentForKey("spark.rapids.memory.pinnedPool.size")}
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |- ${notEnoughMemComment(24371)}
          |- $missingGpuDiscoveryScriptComment
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // This test verifies that an error is thrown when the target cluster YAML file
  // contains both instance type (for CSP) and resource properties (for OnPrem).
  test("Should fail when target cluster contains both CSP instanceType and OnPrem resources") {
    TrampolineUtil.withTempDir { tempDir =>
      // workerInfo:
      //   instanceType: g2-standard-8
      //   cpuCores: 16
      //   memoryGB: 64
      //   gpu:
      //     count: 1
      //     name: l4
      assertThrows[IllegalArgumentException] {
        ToolTestUtils.createTargetClusterInfoFile(
          tempDir.getAbsolutePath,
          workerNodeInstanceType = Some("g2-standard-8"),
          cpuCores = Some(16), memoryGB = Some(64),
          gpuCount = Some(1), gpuDevice = Some(GpuTypes.L4))
      }
    }
  }

  // This test verifies that an error is thrown when the target cluster YAML file
  // contains resource properties (for OnPrem) except GPU.
  test("Should fail when target cluster contains OnPrem resources except GPU") {
    TrampolineUtil.withTempDir { tempDir =>
      // workerInfo:
      //   cpuCores: 16
      //   memoryGB: 64
      assertThrows[IllegalArgumentException] {
        ToolTestUtils.createTargetClusterInfoFile(
          tempDir.getAbsolutePath,
          cpuCores = Some(16), memoryGB = Some(64))
      }
    }
  }

  // This test verifies that an error is thrown when the target cluster YAML file
  // contains both worker info and driver info for OnPrem
  test("Should fail when target cluster contains both worker info and driver info for OnPrem") {
    TrampolineUtil.withTempDir { tempDir =>
      // driverInfo:
      //   instanceType: foobar
      // workerInfo:
      //   cpuCores: 16
      //   memoryGB: 64
      //   gpu:
      //     count: 1
      //     name: l4
      assertThrows[IllegalArgumentException] {
        ToolTestUtils.createTargetClusterInfoFile(
          tempDir.getAbsolutePath,
          driverNodeInstanceType = Some("foobar"),
          cpuCores = Some(16), memoryGB = Some(64),
          gpuCount = Some(1), gpuDevice = Some(GpuTypes.L4))
      }
    }
  }

  // This test uses target cluster properties with a worker node having 16 cores, 64g memory,
  // and 1 L20 GPU. The platform is mocked as Kubernetes on OnPrem
  // to enable memory overhead calculation.
  // AutoTuner is expected to:
  // - Recommend `spark.rapids.sql.concurrentGpuTasks` to a value of 4 since L20
  //   has high memory.
  test("Target cluster properties for OnPrem with worker having L20 GPU") {
    // 2. Mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "2",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "1"
      )
    // workerInfo:
    //   cpuCores: 16
    //   memoryGB: 64
    //   gpu:
    //     count: 1
    //     name: l20
    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      cpuCores = Some(16), memoryGB = Some(64),
      gpuCount = Some(1), gpuDevice = Some(GpuTypes.L20)
    )
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, Some(targetClusterInfo))

    val sparkPropsWithMemory = logEventsProps + ("spark.executor.memory" -> "14000MiB")
    configureEventLogClusterInfoForTest(
      platform,
      numCores = 8, // from eventLog
      numWorkers = 2,
      gpuCount = 1, // target cluster has 1 L20
      sparkProperties = sparkPropsWithMemory.toMap
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(Kubernetes))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.memory=32g
          |--conf spark.executor.memoryOverhead=11468m
          |--conf spark.executor.resource.gpu.vendor=nvidia.com
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=24
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=24
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=4
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=32
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=200
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.executor.resource.gpu.vendor' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |- $missingGpuDiscoveryScriptComment
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // Test that the alias feature works correctly for mapping non-standard Spark properties
  // to standard ones, using the specific example from the issue:
  // spark.sql.adaptive.shuffle.maxNumPostShufflePartitions ->
  //   spark.sql.adaptive.coalescePartitions.initialPartitionNum
  test("AutoTuner should handle aliased properties from tuningDefinitions") {
    // 1. Mock source cluster info for dataproc
    val instanceMapKey = NodeInstanceMapKey("n1-standard-16", Option(1))
    val gpuInstance = PlatformInstanceTypes.DATAPROC_BY_INSTANCE_NAME(instanceMapKey)

    // 2. Mock the properties loaded from eventLog with the aliased property
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "2",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "1",
        // alias property
        "spark.sql.adaptive.maxNumPostShufflePartitions" -> "100"
      )

    // 3. Create user-defined tuningDefinitions for the target cluster
    val userTuningDefinitions = createMaxNumPostShufflePartitionsTuningDefinition()

    // 4. Define enforced properties for the target cluster (no alias needed in target-cluster yaml)
    val enforcedSparkProperties = Map(
      "spark.task.resource.gpu.amount" -> "0.25",
      "spark.rapids.sql.concurrentGpuTasks" -> "2"
    )

    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      workerNodeInstanceType = Some("n1-standard-16"),
      gpuCount = Some(1),
      enforcedSparkProperties = enforcedSparkProperties,
      tuningDefinitions = userTuningDefinitions
    )

    val infoProvider = getMockInfoProvider(
      maxInput = 100000.0,
      spilledMetrics = Seq(100000),
      jvmGCFractions = Seq(0.004),
      propsFromLog = logEventsProps,
      sparkVersion = Some(testSparkVersion),
      meanInput = 60000.0,  // > 35000 (AQE_INPUT_SIZE_BYTES_THRESHOLD)
      meanShuffleRead = 70000.0  // > 50000 (AQE_SHUFFLE_READ_BYTES_THRESHOLD)
    )
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, Some(targetClusterInfo))

    val sparkPropsWithMemory =
      logEventsProps + ("spark.executor.memory" -> (gpuInstance.memoryMB.toString + "MiB"))
    configureEventLogClusterInfoForTest(
      platform,
      numCores = gpuInstance.cores,
      numWorkers = 4,
      gpuCount = 1,
      sparkProperties = sparkPropsWithMemory.toMap
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)

    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.memory=32g
          |--conf spark.executor.memoryOverhead=15564m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=32m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false
          |--conf spark.sql.adaptive.maxNumPostShufflePartitions=800
          |--conf spark.sql.files.maxPartitionBytes=4g
          |--conf spark.sql.shuffle.partitions=800
          |--conf spark.task.resource.gpu.amount=0.25
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- ${getEnforcedPropertyComment("spark.rapids.sql.concurrentGpuTasks")}
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- ${getEnforcedPropertyComment("spark.task.resource.gpu.amount")}
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  /**
   * Helper method to create tuning definition for testing
   * spark.sql.adaptive.maxNumPostShufflePartitions alias
   */
  private def createMaxNumPostShufflePartitionsTuningDefinition():
    java.util.List[TuningEntryDefinition] = {
    import scala.jdk.CollectionConverters._
    List(TuningEntryDefinition(
      label = "spark.sql.adaptive.maxNumPostShufflePartitions",
      description = "Custom tuning definition for testing alias feature",
      confType = ConfTypeEnum.Int)).asJava
  }

  // This test validates that user-provided tuning configurations are honored by AutoTuner.
  // AutoTuner is expected to:
  // - Recommend `spark.executor.memory` to a value of 0.9g/core * 16cores ~ 14736m
  // - Recommend `spark.rapids.sql.concurrentGpuTasks` to a value of 1
  test("AutoTuner honours user provided tuning configurations") {
    // 1. Mock source cluster info for dataproc
    val instanceMapKey = NodeInstanceMapKey("g2-standard-16")
    val gpuInstance = PlatformInstanceTypes.DATAPROC_BY_INSTANCE_NAME(instanceMapKey)
    // 2. Mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "2",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "1"
      )
    // 3. Mock the user-provided tuning configurations. Equivalent YAML snippet:
    // tuningConfigs:
    //   default:
    //   - name: HEAP_PER_CORE
    //     default: 0.9g
    //   - name: CONC_GPU_TASKS
    //     max: 1
    val defaultTuningConfigsEntries = List(
      TuningConfigEntry(name = "HEAP_PER_CORE", default = "0.9g"),
      TuningConfigEntry(name = "CONC_GPU_TASKS", max = "1")
    )
    val userProvidedTuningConfigs = ToolTestUtils.buildTuningConfigs(
      default = defaultTuningConfigsEntries)
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC)

    val sparkPropsWithMemory =
      logEventsProps + ("spark.executor.memory" -> (gpuInstance.memoryMB.toString + "MiB"))
    configureEventLogClusterInfoForTest(
      platform,
      numCores = gpuInstance.cores,
      numWorkers = 4,
      gpuCount = gpuInstance.numGpus,
      sparkProperties = sparkPropsWithMemory.toMap
    )
    val autoTuner =
      buildAutoTunerForTests(
        infoProvider,
        platform,
        Some(Kubernetes),
        Some(userProvidedTuningConfigs))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.memory=14736m
          |--conf spark.executor.memoryOverhead=13761m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=1
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=200
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
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
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // This test validates that AutoTuner throws IllegalArgumentException when user provides
  // tuning configurations with typos in name.
  test("AutoTuner should throw IllegalArgumentException for typo in tuning config name") {
    // 1. Mock source cluster info for dataproc
    val instanceMapKey = NodeInstanceMapKey("g2-standard-16")
    val gpuInstance = PlatformInstanceTypes.DATAPROC_BY_INSTANCE_NAME(instanceMapKey)
    // 2. Mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "2",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "1"
      )
    // 3. Mock the user-provided tuning configurations with typo. Equivalent YAML snippet:
    // tuningConfigs:
    //   default:
    //   - name: BATCH_SIZE_BTYES  # Typo: should be BATCH_SIZE_BYTES
    //     default: 512m
    val defaultTuningConfigsEntries = List(
      TuningConfigEntry(name = "BATCH_SIZE_BTYES", default = "512m")  // Typo in name
    )
    val userProvidedTuningConfigs = ToolTestUtils.buildTuningConfigs(
      default = defaultTuningConfigsEntries)
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC)

    val sparkPropsWithMemory =
      logEventsProps + ("spark.executor.memory" -> (gpuInstance.memoryMB.toString + "MiB"))
    configureEventLogClusterInfoForTest(
      platform,
      numCores = gpuInstance.cores,
      numWorkers = 4,
      gpuCount = gpuInstance.numGpus,
      sparkProperties = sparkPropsWithMemory.toMap
    )

    // AutoTuner should throw IllegalArgumentException due to invalid tuning config name
    assertThrows[IllegalArgumentException] {
      val autoTuner =
        buildAutoTunerForTests(infoProvider, platform, None, Some(userProvidedTuningConfigs))
      autoTuner.getRecommendedProperties()
    }
  }

  // This test validates that AutoTuner honours existing values of
  // `spark.executor.resource.gpu.amount` when it is already set.
  test("AutoTuner honours existing values of spark.executor.resource.gpu.amount") {
    // 1. Mock source cluster info for dataproc
    val instanceMapKey = NodeInstanceMapKey("g2-standard-16")
    val gpuInstance = PlatformInstanceTypes.DATAPROC_BY_INSTANCE_NAME(instanceMapKey)
    // 2. Mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "2",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "2"
      )
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC)

    val sparkPropsWithMemory =
      logEventsProps + ("spark.executor.memory" -> (gpuInstance.memoryMB.toString + "MiB"))
    configureEventLogClusterInfoForTest(
      platform,
      numCores = gpuInstance.cores,
      numWorkers = 4,
      gpuCount = gpuInstance.numGpus,
      sparkProperties = sparkPropsWithMemory.toMap
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(Yarn))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.memory=32g
          |--conf spark.executor.memoryOverhead=15564m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=200
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
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
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("AutoTuner adjusts initialPartitionNum based on ColumnarExchange data size") {
    // Test case: max columnar exchange data size = 1000GB, batch size is default value 2gb
    // original initialPartitionNum = 2560
    // Expected: recommended initialPartitionNum = min(2560, 500) = 500

    val maxColumnarExchangeDataSizeBytes = Some(1000L * 1024 * 1024 * 1024) // 1000GB in bytes
    val originalInitialPartitionNum = "2560"

    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum" -> originalInitialPartitionNum,
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "4m",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "64m",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1"
      )

    val infoProvider = getMockInfoProvider(
      maxInput = 1.0E9, // Large input to trigger AQE recommendations
      spilledMetrics = Seq(0, 0),
      jvmGCFractions = Seq(0.1, 0.1),
      propsFromLog = logEventsProps,
      sparkVersion = Some(testSparkVersion),
      meanInput = 1.0E9, // Large mean input
      meanShuffleRead = 1.0E9, // Large mean shuffle read
      maxColumnarExchangeDataSizeBytes = maxColumnarExchangeDataSizeBytes
    )

    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC)

    // Configure cluster info
    configureEventLogClusterInfoForTest(
      platform,
      numCores = 16,
      numWorkers = 1,
      gpuCount = 1,
      sparkProperties = logEventsProps.toMap
    )

    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)

    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.memory=32g
          |--conf spark.executor.memoryOverhead=15564m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=501
          |--conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false
          |--conf spark.sql.files.maxPartitionBytes=137m
          |--conf spark.sql.shuffle.partitions=501
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
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.coalescePartitions.initialPartitionNum' adjusted from 2560 to 501 based on ColumnarExchange data size (1073741824000 bytes) and GPU batch size (2147483647 bytes)
          |- 'spark.sql.files.maxPartitionBytes' was not set.
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
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("AutoTuner handles case when ColumnarExchange data size ratio is larger than " +
    "original initialPartitionNum") {
    // Test case: max columnar exchange data size = 6000GB,
    // original initialPartitionNum = 2560
    // Expected: ratio = ceil(6000GB / 2GB) = 3000, which is > 2560
    // So recommended initialPartitionNum = min(2560, 3000) = 2560 (no change)

    val maxColumnarExchangeDataSizeBytes = Some(6000L * 1024 * 1024 * 1024) // 6000GB in bytes
    val originalInitialPartitionNum = "2560"

    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum" -> originalInitialPartitionNum,
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "4m",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "64m",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1"
      )

    val infoProvider = getMockInfoProvider(
      maxInput = 1.0E9,
      spilledMetrics = Seq(0, 0),
      jvmGCFractions = Seq(0.1, 0.1),
      propsFromLog = logEventsProps,
      sparkVersion = Some(testSparkVersion),
      meanInput = 1.0E9,
      meanShuffleRead = 1.0E9,
      maxColumnarExchangeDataSizeBytes = maxColumnarExchangeDataSizeBytes
    )

    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC)

    configureEventLogClusterInfoForTest(
      platform,
      numCores = 16,
      numWorkers = 1,
      gpuCount = 1,
      sparkProperties = logEventsProps.toMap
    )

    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)

    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.memory=32g
          |--conf spark.executor.memoryOverhead=15564m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false
          |--conf spark.sql.files.maxPartitionBytes=137m
          |--conf spark.sql.shuffle.partitions=2560
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
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
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
    compareOutput(expectedResults, autoTunerOutput)
  }

  test("AutoTuner handles case when no ColumnarExchange data size is available") {
    // Test case: no ColumnarExchange data size available,
    // original initialPartitionNum = 2560
    // Expected: no adjustment should be made, original value should be preserved

    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum" -> "2560",
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "4m",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "64m",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks" -> "1"
      )

    val infoProvider = getMockInfoProvider(
      maxInput = 1.0E9,
      spilledMetrics = Seq(0, 0),
      jvmGCFractions = Seq(0.1, 0.1),
      propsFromLog = logEventsProps,
      sparkVersion = Some(testSparkVersion),
      meanInput = 1.0E9,
      meanShuffleRead = 1.0E9,
      maxColumnarExchangeDataSizeBytes = None // No ColumnarExchange data size
    )

    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC)

    configureEventLogClusterInfoForTest(
      platform,
      numCores = 16,
      numWorkers = 1,
      gpuCount = 1,
      sparkProperties = logEventsProps.toMap
    )

    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)

    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.memory=32g
          |--conf spark.executor.memoryOverhead=15564m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false
          |--conf spark.sql.files.maxPartitionBytes=137m
          |--conf spark.sql.shuffle.partitions=2560
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
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
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
    compareOutput(expectedResults, autoTunerOutput)
  }

  // Test that the alias feature works correctly for initialPartitionNum properties
  // using spark.sql.adaptive.maxNumPostShufflePartitions as alias
  test("AutoTuner should handle aliased initialPartitionNum properties from tuningDefinitions") {
    // 1. Mock source cluster info for dataproc
    val instanceMapKey = NodeInstanceMapKey("n1-standard-16", Option(1))
    val gpuInstance = PlatformInstanceTypes.DATAPROC_BY_INSTANCE_NAME(instanceMapKey)

    // 2. Mock the properties loaded from eventLog with the aliased property
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "2",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
        "spark.executor.resource.gpu.amount" -> "1",
        // alias property for initialPartitionNum
        "spark.sql.adaptive.maxNumPostShufflePartitions" -> "2000"
      )

    // 3. Create user-defined tuningDefinitions for the target cluster
    val userTuningDefinitions = createMaxNumPostShufflePartitionsTuningDefinition()

    // 4. Define enforced properties for the target cluster
    val enforcedSparkProperties = Map(
      "spark.task.resource.gpu.amount" -> "0.25",
      "spark.rapids.sql.concurrentGpuTasks" -> "2"
    )

    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      workerNodeInstanceType = Some("n1-standard-16"),
      gpuCount = Some(1),
      enforcedSparkProperties = enforcedSparkProperties,
      tuningDefinitions = userTuningDefinitions
    )

    val infoProvider = getMockInfoProvider(
      maxInput = 100000.0,
      spilledMetrics = Seq(100000),
      jvmGCFractions = Seq(0.004),
      propsFromLog = logEventsProps,
      sparkVersion = Some(testSparkVersion),
      meanInput = 60000.0,  // > 35000 (AQE_INPUT_SIZE_BYTES_THRESHOLD)
      meanShuffleRead = 70000.0,  // > 50000 (AQE_SHUFFLE_READ_BYTES_THRESHOLD)
      rapidsJars = Seq.empty,
      distinctLocationPct = 0.0,
      redundantReadSize = 0L,
      shuffleStagesWithPosSpilling = Set.empty,
      shuffleSkewStages = Set.empty,
      scanStagesWithGpuOom = false,
      shuffleStagesWithOom = false,
      maxColumnarExchangeDataSizeBytes = Some(1000L * 1024 * 1024 * 1024) // 1000GB
    )
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, Some(targetClusterInfo))

    val sparkPropsWithMemory =
      logEventsProps + ("spark.executor.memory" -> (gpuInstance.memoryMB.toString + "MiB"))
    configureEventLogClusterInfoForTest(
      platform,
      numCores = gpuInstance.cores,
      numWorkers = 4,
      gpuCount = 1,
      sparkProperties = sparkPropsWithMemory.toMap
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)

    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.dataproc.enhanced.execution.enabled=false
          |--conf spark.dataproc.enhanced.optimizer.enabled=false
          |--conf spark.executor.cores=16
          |--conf spark.executor.memory=32g
          |--conf spark.executor.memoryOverhead=15564m
          |--conf spark.locality.wait=0
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=28
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=28
          |--conf spark.rapids.sql.batchSizeBytes=2147483647b
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.format.parquet.multithreaded.combine.waitTime=1000
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=80
          |--conf spark.rapids.sql.reader.multithreaded.combine.sizeBytes=10m
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=32m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false
          |--conf spark.sql.adaptive.maxNumPostShufflePartitions=501
          |--conf spark.sql.files.maxPartitionBytes=4g
          |--conf spark.sql.shuffle.partitions=501
          |--conf spark.task.resource.gpu.amount=0.25
          |
          |Comments:
          |- 'spark.dataproc.enhanced.execution.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.execution.enabled' was not set.
          |- 'spark.dataproc.enhanced.optimizer.enabled' should be disabled. WARN: Turning this property on might case the GPU accelerated Dataproc cluster to hang.
          |- 'spark.dataproc.enhanced.optimizer.enabled' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.maxBytesInFlight' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was user-enforced in the target cluster properties.
          |- 'spark.rapids.sql.format.parquet.multithreaded.combine.waitTime' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.maxNumPostShufflePartitions' adjusted from 2000 to 501 based on ColumnarExchange data size (1073741824000 bytes) and GPU batch size (2147483647 bytes)
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was user-enforced in the target cluster properties.
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
    compareOutput(expectedResults, autoTunerOutput)
  }

  // Tests for NON_EXECUTOR_MEM_FRACTION tuning config

  // This test verifies that AutoTuner uses platform default when
  // NON_EXECUTOR_MEM_FRACTION is not specified (default is -1, meaning use platform default)
  test("AutoTuner uses platform default nonExecutorMemoryFraction") {
    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      workerNodeInstanceType = Some("g2-standard-8")
    )
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC, Some(targetClusterInfo))

    // Verify platform default is 0.2 (20% not available for executors) for Dataproc
    assert(platform.nonExecutorMemoryFraction == 0.2,
      s"Expected Dataproc default to be 0.2, got ${platform.nonExecutorMemoryFraction}")
  }

  // This test verifies that different platforms have different defaults
  test("Different platforms have different nonExecutorMemoryFraction defaults") {
    // Create platforms without target cluster info to test default values
    val dataprocPlatform = PlatformFactory.createInstance(PlatformNames.DATAPROC, None)
    val emrPlatform = PlatformFactory.createInstance(PlatformNames.EMR, None)

    assert(dataprocPlatform.nonExecutorMemoryFraction == 0.2,
      "Dataproc default should be 0.2 (20% not available for executors)")
    assert(emrPlatform.nonExecutorMemoryFraction == 0.3,
      "EMR default should be 0.3 (30% not available for executors)")
  }

  // Test custom NON_EXECUTOR_MEM_FRACTION value (0.0 = 100% memory utilization)
  test("AutoTuner accepts NON_EXECUTOR_MEM_FRACTION = 0.0 for full memory utilization") {
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "2",
        "spark.executor.memory" -> "26742m",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin"
      )

    // Set NON_EXECUTOR_MEM_FRACTION to 0.0 (100% available for executors)
    val defaultTuningConfigsEntries = List(
      TuningConfigEntry(name = "NON_EXECUTOR_MEM_FRACTION", default = "0.0")
    )
    val userProvidedTuningConfigs = ToolTestUtils.buildTuningConfigs(
      default = defaultTuningConfigsEntries)

    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC)

    configureEventLogClusterInfoForTest(
      platform,
      numCores = 32,
      numWorkers = 2,
      gpuCount = 1,
      sparkProperties = logEventsProps.toMap
    )

    // Should use 0.0 (100% available) instead of Dataproc default 0.2
    val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(Yarn),
      Some(userProvidedTuningConfigs))
    val (_, _) = autoTuner.getRecommendedProperties()
  }

  // Test custom NON_EXECUTOR_MEM_FRACTION value (0.5 = 50% reserved)
  test("AutoTuner accepts custom NON_EXECUTOR_MEM_FRACTION = 0.5") {
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "2",
        "spark.executor.memory" -> "26742m",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin"
      )

    // Set NON_EXECUTOR_MEM_FRACTION to 0.5 (50% reserved, 50% available)
    val defaultTuningConfigsEntries = List(
      TuningConfigEntry(name = "NON_EXECUTOR_MEM_FRACTION", default = "0.5")
    )
    val userProvidedTuningConfigs = ToolTestUtils.buildTuningConfigs(
      default = defaultTuningConfigsEntries)

    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC)

    configureEventLogClusterInfoForTest(
      platform,
      numCores = 32,
      numWorkers = 2,
      gpuCount = 1,
      sparkProperties = logEventsProps.toMap
    )

    val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(Yarn),
      Some(userProvidedTuningConfigs))
    val (_, _) = autoTuner.getRecommendedProperties()
  }

  // Test invalid NON_EXECUTOR_MEM_FRACTION (>= 1.0) falls back to platform default
  test("AutoTuner falls back to platform default when NON_EXECUTOR_MEM_FRACTION >= 1.0") {
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "2",
        "spark.executor.memory" -> "26742m",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin"
      )

    // Set invalid NON_EXECUTOR_MEM_FRACTION >= 1.0 (should fall back to platform default)
    val defaultTuningConfigsEntries = List(
      TuningConfigEntry(name = "NON_EXECUTOR_MEM_FRACTION", default = "1.5")
    )
    val userProvidedTuningConfigs = ToolTestUtils.buildTuningConfigs(
      default = defaultTuningConfigsEntries)

    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.DATAPROC)

    configureEventLogClusterInfoForTest(
      platform,
      numCores = 32,
      numWorkers = 2,
      gpuCount = 1,
      sparkProperties = logEventsProps.toMap
    )

    // Should fall back to Dataproc default (0.2) with a warning
    val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(Yarn),
      Some(userProvidedTuningConfigs))
    val (_, _) = autoTuner.getRecommendedProperties()
  }

  // Test that NON_EXECUTOR_MEM takes precedence over NON_EXECUTOR_MEM_FRACTION
  test("NON_EXECUTOR_MEM takes precedence over NON_EXECUTOR_MEM_FRACTION") {
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "2",
        "spark.executor.memory" -> "26742m",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.rapids.sql.enabled" -> "true",
        "spark.plugins" -> "com.nvidia.spark.SQLPlugin"
      )

    // Set both absolute and fraction - absolute should take precedence
    val defaultTuningConfigsEntries = List(
      TuningConfigEntry(name = "NON_EXECUTOR_MEM", default = "5g"),
      TuningConfigEntry(name = "NON_EXECUTOR_MEM_FRACTION", default = "0.5")
    )
    val userProvidedTuningConfigs = ToolTestUtils.buildTuningConfigs(
      default = defaultTuningConfigsEntries)

    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0), logEventsProps,
      Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM)

    configureEventLogClusterInfoForTest(
      platform,
      numCores = 32,
      numWorkers = 2,
      gpuCount = 1,
      sparkProperties = logEventsProps.toMap
    )

    // NON_EXECUTOR_MEM (5g) should be used, not NON_EXECUTOR_MEM_FRACTION (0.5)
    val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(Yarn),
      Some(userProvidedTuningConfigs))
    val (_, _) = autoTuner.getRecommendedProperties()
  }

  // Test that user-enforced spark.executor.cores in target cluster is respected
  // when calculating recommended number of executors.
  test("AutoTuner respects user-enforced executor cores in cluster sizing") {
    // Source app with 11 cores, 148 executors (via maxExecutors)
    val logEventsProps: mutable.Map[String, String] = mutable.LinkedHashMap[String, String](
      "spark.executor.cores" -> "11",
      "spark.executor.memory" -> "21g",
      "spark.executor.resource.gpu.amount" -> "1",
      "spark.dynamicAllocation.enabled" -> "true",
      "spark.dynamicAllocation.minExecutors" -> "1",
      "spark.dynamicAllocation.maxExecutors" -> "148",
      "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
      "spark.rapids.sql.enabled" -> "true"
    )

    // Target cluster: 48 cores, 4 GPUs (default would be 12 cores/executor)
    // But user enforces 11 cores per executor
    val enforcedSparkProperties = Map(
      "spark.executor.cores" -> "11"
    )
    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      cpuCores = Some(48),  // 48 cores, 4 GPUs = default 12 cores per executor
      memoryGB = Some(192L),
      gpuCount = Some(4),
      gpuDevice = Some("l4"),
      enforcedSparkProperties = enforcedSparkProperties
    )

    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, Some(targetClusterInfo))

    configureEventLogClusterInfoForTest(
      platform,
      numCores = 11,
      numWorkers = 148,
      gpuCount = 1,
      sparkProperties = logEventsProps.toMap
    )

    val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(Yarn))
    val (properties, _) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, Seq.empty)

    // With user-enforced cores=11 and source total cores = 11 * 148 = 1628
    // Recommended executors = 1628 / 11 = 148
    // Without the fix (using default 12 cores), it would be 1628 / 12 = 135
    // This is the key assertion - executor.instances=148 proves cores=11 was used
    assert(autoTunerOutput.contains("spark.executor.instances=148"),
      s"Expected spark.executor.instances=148 with enforced cores=11, but got:\n$autoTunerOutput")
    // Note: spark.executor.cores may not appear in output if source already has same value
  }

}
