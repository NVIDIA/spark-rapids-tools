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

import java.nio.file.Paths

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.{GpuTypes, PlatformFactory, PlatformNames, ToolTestUtils}
import com.nvidia.spark.rapids.tool.profiling.Profiler
import com.nvidia.spark.rapids.tool.qualification.{QualificationArgs, QualificationMain}
import com.nvidia.spark.rapids.tool.views.CLUSTER_INFORMATION_LABEL
import com.nvidia.spark.rapids.tool.views.qualification.QualReportGenConfProvider
import org.scalatest.exceptions.TestFailedException
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.TableFor3

import org.apache.spark.sql.TrampolineUtil
import org.apache.spark.sql.rapids.tool.RecommendedClusterInfo
import org.apache.spark.sql.rapids.tool.util.FSUtils

/**
 * Suite to test the Qualification Tool's AutoTuner
 */
class QualificationAutoTunerSuite extends BaseAutoTunerSuite {

  val qualLogDir: String = ToolTestUtils.getTestResourcePath("spark-events-qualification")
  val autoTunerHelper: AutoTunerHelper = QualificationAutoTunerHelper

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
   * Helper method to return an instance of the Qualification AutoTuner with default properties
   */
  private def buildDefaultAutoTuner(
      logEventsProps: mutable.Map[String, String] = defaultSparkProps): AutoTuner = {
    val sparkPropsWithMemory = logEventsProps + ("spark.executor.memory" -> "212992MiB")
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      sparkPropsWithMemory, Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.EMR)

    // Configure cluster info: 32 cores, 5 workers, 4 GPUs per worker = 20 total executors
    platform.configureClusterInfoFromEventLog(
      coresPerExecutor = 32,
      execsPerNode = 4,
      numExecs = 20, // 5 workers * 4 GPUs per worker
      numExecutorNodes = 5,
      sparkProperties = sparkPropsWithMemory.toMap,
      systemProperties = Map.empty
    )

    buildAutoTunerForTests(infoProvider, platform)
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
        s"- ${notEnoughMemCommentForKey("spark.executor.memory")}",
        s"- ${notEnoughMemCommentForKey("spark.executor.memoryOverhead")}",
        s"- ${notEnoughMemCommentForKey("spark.rapids.memory.pinnedPool.size")}",
        s"- ${notEnoughMemComment(40140)}"
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
      val logEventsProps: mutable.Map[String, String] =
        mutable.LinkedHashMap[String, String](
          "spark.executor.cores" -> "8",
          "spark.executor.instances" -> "4",
          "spark.executor.memory" -> "8g",
          "spark.executor.memoryOverhead" -> "2g"
        )
      val sparkPropsWithMemory = logEventsProps + ("spark.executor.memory" -> workerMemory)
      val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
        logEventsProps, Some(testSparkVersion))
      val platform = PlatformFactory.createInstance(PlatformNames.ONPREM)
      // Configure cluster info: 16 cores, 2 workers, 2 GPUs per worker = 4 total executors
      platform.configureClusterInfoFromEventLog(
        coresPerExecutor = 16,
        execsPerNode = 2,
        numExecs = 4, // 2 workers * 2 GPUs per worker
        numExecutorNodes = 2,
        sparkProperties = sparkPropsWithMemory.toMap,
        systemProperties = Map.empty
      )
      val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(Yarn))
      val (properties, comments) = autoTuner.getRecommendedProperties(showOnlyUpdatedProps =
        QualificationAutoTunerRunner.filterByUpdatedPropsEnabled)
      val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
      assertExpectedLinesExist(expectedResults, autoTunerOutput)
    }
  }

  /**
   * Test to validate the cluster shape recommendation with enforced spark properties.
   * This tests that if the user has enforced `spark.executor.instances`, this will
   * affect the recommended cluster shape.
   *
   * Target Cluster YAML file:
   * {{{
   * driverInfo:
   *  instanceType: n1-standard-8
   * workerInfo:
   *  instanceType: g2-standard-8
   * sparkProperties:
   *  enforced:
   *    spark.executor.cores: 8
   *    spark.executor.instances: 4
   *    spark.executor.memory: 12g
   * }}}
   */
  test(s"test valid cluster shape recommendation with enforced spark properties on dataproc " +
    s"affecting the cluster shape") {
    val testEventLog = s"$qualLogDir/nds_q72_dataproc_2_2.zstd"
    val testEnforcedSparkProperties = Map(
      "spark.executor.cores" -> "8",
      "spark.executor.instances" -> "4",
      "spark.rapids.sql.batchSizeBytes" -> "3g"
    )
    val expectedClusterInfo = RecommendedClusterInfo(
      vendor = PlatformNames.DATAPROC,
      coresPerExecutor = 8,
      numWorkerNodes = 4,
      numGpusPerNode = 1,
      numExecutors = 4,
      gpuDevice = GpuTypes.L4,
      dynamicAllocationEnabled = false,
      dynamicAllocationMaxExecutors = "N/A",
      dynamicAllocationMinExecutors = "N/A",
      dynamicAllocationInitialExecutors = "N/A",
      driverNodeType = Some("n1-standard-8"),
      workerNodeType = Some("g2-standard-8")
    )
    TrampolineUtil.withTempDir { tempDir =>
      val targetClusterInfoFile = ToolTestUtils.createTargetClusterInfoFile(
        tempDir.getAbsolutePath,
        driverNodeInstanceType = expectedClusterInfo.driverNodeType,
        workerNodeInstanceType = expectedClusterInfo.workerNodeType,
        enforcedSparkProperties = testEnforcedSparkProperties)

      val appArgs = new QualificationArgs(Array(
        "--platform",
        PlatformNames.DATAPROC,
        "--target-cluster-info",
        targetClusterInfoFile.toString,
        "--output-directory",
        tempDir.getAbsolutePath,
        "--auto-tuner",
        testEventLog
        ))

      val result = QualificationMain.mainInternal(appArgs)
      assert(!result.isFailed)
      val appId = result.appSummaries.headOption.map(_.appId)
        .getOrElse(throw new TestFailedException("No appId found in the result", 0))

      // 1. Verify the recommended cluster info
      val clusterInfoFileName = s"${CLUSTER_INFORMATION_LABEL.replace(" ", "_").toLowerCase}.json"
      val actualClusterInfoFile = Paths.get(
        QualReportGenConfProvider.getPerAppReportPath(tempDir.getAbsolutePath),
        appId, clusterInfoFileName
      ).toFile
      assertRecommendedClusterInfo(actualClusterInfoFile, expectedClusterInfo)

      // 2. Verify the enforced spark properties
      val tuningResultPath = Paths.get(
        QualReportGenConfProvider.getTuningReportPath(tempDir.getAbsolutePath),
        s"$appId.log"
      ).toString
      val actualTuningResults = FSUtils.readFileContentAsUTF8(tuningResultPath)

      // scalastyle:off line.size.limit
      val expectedResults =
        s"""|
            |### Recommended SPARK Configuration on GPU Cluster for App: $appId ###
            |
            |Spark Properties:
            |--conf spark.dataproc.enhanced.execution.enabled=true
            |--conf spark.dataproc.enhanced.optimizer.enabled=true
            |--conf spark.executor.cores=8
            |--conf spark.executor.instances=4
            |--conf spark.executor.memory=16g
            |--conf spark.executor.memoryOverhead=9830m
            |--conf spark.executor.resource.gpu.amount=1
            |--conf spark.locality.wait=0
            |--conf spark.plugins=com.nvidia.spark.SQLPlugin
            |--conf spark.rapids.memory.pinnedPool.size=4g
            |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
            |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
            |--conf spark.rapids.sql.batchSizeBytes=3g
            |--conf spark.rapids.sql.concurrentGpuTasks=3
            |--conf spark.rapids.sql.enabled=true
            |--conf spark.rapids.sql.multiThreadedRead.numThreads=40
            |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
            |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
            |--conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false
            |--conf spark.sql.adaptive.enabled=true
            |--conf spark.sql.files.maxPartitionBytes=1644m
            |--conf spark.sql.shuffle.partitions=128
            |--conf spark.task.resource.gpu.amount=0.001
            |
            |Comments:
            |- ${getEnforcedPropertyComment("spark.executor.cores")}
            |- ${getEnforcedPropertyComment("spark.executor.instances")}
            |- 'spark.executor.resource.gpu.amount' should be set to allow Spark to schedule GPU resources.
            |- 'spark.plugins' should be set to the class name required for the RAPIDS Accelerator for Apache Spark.
            |  Refer to: https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
            |- 'spark.rapids.memory.pinnedPool.size' was not set.
            |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
            |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
            |- ${getEnforcedPropertyComment("spark.rapids.sql.batchSizeBytes")}
            |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
            |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
            |- 'spark.shuffle.manager' was not set.
            |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
            |- 'spark.sql.files.maxPartitionBytes' was not set.
            |- 'spark.task.resource.gpu.amount' was not set.
            |- ${classPathComments("rapids.jars.missing")}
            |- ${classPathComments("rapids.shuffle.jars")}
            |- $additionalSparkPluginsComment
            |""".stripMargin.trim
      // scalastyle:on line.size.limit
      compareOutput(expectedResults, actualTuningResults)
    }
  }

  // This test validates that user-provided tuning configurations specific to Qualification
  // are honored by the AutoTuner.
  // AutoTuner is expected to:
  // - Recommend `spark.executor.memory` to a value:
  //     1.2g/core * 16cores = 19648m
  // - Recommend `spark.rapids.sql.concurrentGpuTasks` to a value:
  //     max(CONC_GPU_TASKS (8), gpuMemory (24g) / GPU_MEM_PER_TASK (4g) = 6
  test("AutoTuner honours user provided tuning configurations specific to Qualification") {
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "4",
        "spark.executor.memory" -> "8g",
        "spark.executor.memoryOverhead" -> "2g"
      )
    // 2. Mock the user-provided tuning configurations. Equivalent YAML snippet:
    // tuningConfigs:
    //   default:
    //   - name: GPU_MEM_PER_TASK
    //     default: 4g
    //   - name: CONC_GPU_TASKS
    //     max: 8
    //   qualification:
    //   - name: HEAP_PER_CORE
    //     default: 1.2g
    val defaultTuningConfigsEntries = List(
      TuningConfigEntry(name = "GPU_MEM_PER_TASK", default = "4g"),
      TuningConfigEntry(name = "CONC_GPU_TASKS", max = "8")
    )
    val qualificationTuningConfigEntries = List(
      TuningConfigEntry(name = "HEAP_PER_CORE", default = "1.2g")
    )
    val userProvidedTuningConfigs = ToolTestUtils.buildTuningConfigs(
      default = defaultTuningConfigsEntries, qualification = qualificationTuningConfigEntries)
    val sparkPropsWithMemory = logEventsProps + ("spark.executor.memory" -> "32g")
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM)

    // Configure cluster info: 8 cores, 2 workers, 2 GPUs per worker = 4 total executors
    platform.configureClusterInfoFromEventLog(
      coresPerExecutor = 8,
      execsPerNode = 2,
      numExecs = 4, // 2 workers * 2 GPUs per worker
      numExecutorNodes = 2,
      sparkProperties = sparkPropsWithMemory.toMap,
      systemProperties = Map.empty
    )

    val autoTuner =
      buildAutoTunerForTests(infoProvider, platform, Some(Yarn), Some(userProvidedTuningConfigs))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=2
          |--conf spark.executor.memory=19648m
          |--conf spark.executor.memoryOverhead=10156m
          |--conf spark.executor.resource.gpu.amount=1
          |--conf spark.locality.wait=0
          |--conf spark.plugins=com.nvidia.spark.SQLPlugin
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=24
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=24
          |--conf spark.rapids.sql.batchSizeBytes=1g
          |--conf spark.rapids.sql.concurrentGpuTasks=6
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=32
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.executor.resource.gpu.amount' should be set to allow Spark to schedule GPU resources.
          |- 'spark.plugins' should be set to the class name required for the RAPIDS Accelerator for Apache Spark.
          |  Refer to: https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |- $missingGpuDiscoveryScriptComment
          |- $additionalSparkPluginsComment
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // Test to validate that Bootstrap sets the appropriate GPU resource properties (i.e. amount,
  // discovery script and vendor) based on the Spark master type.
  // scalastyle:off line.size.limit
  val gpuResourcePropertiesTestData: TableFor3[String, SparkMaster, Seq[String]] = Table(
    ("testName", "sparkMaster", "expectedResults"),
    ("Standalone",
      Standalone,
      Seq(
        "--conf spark.executor.resource.gpu.amount=1",
        "- 'spark.executor.resource.gpu.amount' should be set to allow Spark to schedule GPU resources.",
        s"- $missingGpuDiscoveryScriptComment"
      )),
    ("Yarn",
      Yarn,
      Seq(
        "--conf spark.executor.resource.gpu.amount=1",
        "- 'spark.executor.resource.gpu.amount' should be set to allow Spark to schedule GPU resources.",
        s"- $missingGpuDiscoveryScriptComment"
      )),
    ("Kubernetes",
      Kubernetes,
      Seq(
        "--conf spark.executor.resource.gpu.amount=1",
        "--conf spark.executor.resource.gpu.vendor=nvidia.com",
        "- 'spark.executor.resource.gpu.amount' should be set to allow Spark to schedule GPU resources.",
        "- 'spark.executor.resource.gpu.vendor' was not set.",
        s"- $missingGpuDiscoveryScriptComment"
      ))
  )
  // scalastyle:on line.size.limit

  forAll(gpuResourcePropertiesTestData) {
    (testName: String, sparkMaster: SparkMaster, expectedResults: Seq[String]) =>
      test(s"test AutoTuner for Qualification sets GPU resource properties for $testName") {
        val sparkPropsWithMemory = defaultSparkProps + ("spark.executor.memory" -> "122880MiB")
        val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
          defaultSparkProps, Some(testSparkVersion))
        val platform = PlatformFactory.createInstance(PlatformNames.ONPREM)

        // Configure cluster info: 32 cores, 4 workers, 2 GPUs per worker = 8 total executors
        platform.configureClusterInfoFromEventLog(
          coresPerExecutor = 32,
          execsPerNode = 2,
          numExecs = 8, // 4 workers * 2 GPUs per worker
          numExecutorNodes = 4,
          sparkProperties = sparkPropsWithMemory.toMap,
          systemProperties = Map.empty
        )

        val autoTuner = buildAutoTunerForTests(infoProvider, platform, Some(sparkMaster))
        val (properties, comments) = autoTuner.getRecommendedProperties(showOnlyUpdatedProps =
          QualificationAutoTunerRunner.filterByUpdatedPropsEnabled)
        val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
        assertExpectedLinesExist(expectedResults, autoTunerOutput)
      }
  }

  // This test ensures that AutoTuner honours enforced values for the GPU discovery script
  // and does not include a missing discovery script comment when spark master
  // is YARN
  test("test AutoTuner honours enforced gpu discovery script and" +
    " skips the missing comment when spark master is YARN") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "32g",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g")
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some(testSparkVersion))
    // Define 'spark.executor.resource.gpu.discoveryScript' as an enforced property
    val enforcedSparkProperties = Map(
      "spark.executor.resource.gpu.discoveryScript" -> "/opt/sparkPlugin/gpuDiscoveryScript.sh"
    )

    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      enforcedSparkProperties = enforcedSparkProperties
    )
    val sparkPropsWithMemory = defaultSparkProps + ("spark.executor.memory" -> "122880MiB")
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, Some(targetClusterInfo))
    platform.configureClusterInfoFromEventLog(
      coresPerExecutor = 32,
      execsPerNode = 2,
      numExecs = 8,
      numExecutorNodes = 4,
      sparkProperties = sparkPropsWithMemory.toMap,
      systemProperties = Map.empty
    )

    val autoTuner = buildAutoTunerForTests(infoProvider, platform,
      sparkMaster = Some(Yarn))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.memory=16g
          |--conf spark.executor.memoryOverhead=9830m
          |--conf spark.executor.resource.gpu.amount=1
          |--conf spark.executor.resource.gpu.discoveryScript=/opt/sparkPlugin/gpuDiscoveryScript.sh
          |--conf spark.locality.wait=0
          |--conf spark.plugins=com.nvidia.spark.SQLPlugin
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=1g
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=20
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.executor.resource.gpu.amount' should be set to allow Spark to schedule GPU resources.
          |- 'spark.executor.resource.gpu.discoveryScript' was user-enforced in the target cluster properties.
          |- 'spark.plugins' should be set to the class name required for the RAPIDS Accelerator for Apache Spark.
          |  Refer to: https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |- $additionalSparkPluginsComment
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  /**
   * Test to validate that enforced properties from target cluster info are included in bootstrap.
   * This tests that properties specified in sparkProperties.enforced section appear in both
   * the .log file and the -bootstrap.conf file (regardless of whether they are in tuning table).
   */
  test("test enforced properties are included in bootstrap config") {
    val testEventLog = s"$qualLogDir/nds_q72_dataproc_2_2.zstd"
    // Include both tuning table properties and non-tuning table properties
    val testEnforcedSparkProperties = Map(
      "spark.executor.cores" -> "8",              // In tuning table
      "spark.sql.shuffle.partitions" -> "400",    // In tuning table
      "spark.custom.property" -> "customValue",   // Not in tuning table
      "spark.app.name" -> "TestApp"               // Not in tuning table
    )

    TrampolineUtil.withTempDir { tempDir =>
      val targetClusterInfoFile = ToolTestUtils.createTargetClusterInfoFile(
        tempDir.getAbsolutePath,
        driverNodeInstanceType = Some("n1-standard-8"),
        workerNodeInstanceType = Some("g2-standard-8"),
        enforcedSparkProperties = testEnforcedSparkProperties)

      val appArgs = new QualificationArgs(Array(
        "--platform",
        PlatformNames.DATAPROC,
        "--target-cluster-info",
        targetClusterInfoFile.toString,
        "--output-directory",
        tempDir.getAbsolutePath,
        "--auto-tuner",
        testEventLog
      ))

      val result = QualificationMain.mainInternal(appArgs)
      assert(!result.isFailed)
      val appId = result.appSummaries.headOption.map(_.appId)
        .getOrElse(throw new TestFailedException("No appId found in the result", 0))

      // 1. Verify that enforced properties appear in the main tuning log
      val tuningResultPath = Paths.get(
        QualReportGenConfProvider.getTuningReportPath(tempDir.getAbsolutePath),
        s"$appId.log"
      ).toString
      val actualTuningResults = FSUtils.readFileContentAsUTF8(tuningResultPath)

      testEnforcedSparkProperties.keys.foreach { propertyName =>
        assert(actualTuningResults.contains(s"--conf $propertyName="),
          s"Property $propertyName should appear in tuning log")
        assert(actualTuningResults.contains(getEnforcedPropertyComment(propertyName)),
          s"Enforced property comment for $propertyName should appear in tuning log")
      }

      // 2. Verify that ALL enforced properties also appear in the bootstrap config
      val bootstrapConfigPath = Paths.get(
        QualReportGenConfProvider.getTuningReportPath(tempDir.getAbsolutePath),
        s"$appId-bootstrap.conf"
      ).toString
      val bootstrapConfigContent = FSUtils.readFileContentAsUTF8(bootstrapConfigPath)

      testEnforcedSparkProperties.foreach { case (propertyName, propertyValue) =>
        assert(bootstrapConfigContent.contains(s"--conf $propertyName=$propertyValue"),
          s"Enforced property $propertyName=$propertyValue should appear in bootstrap config")
      }
    }
  }

  /**
   * Test to validate onPrem platform with offHeapLimit enabled.
   * This tests the new memory calculation logic with OS_RESERVED_MEM and offHeapLimit features.
   */
  test("test onPrem platform with offHeapLimit enabled") {
    // Log events properties
    val logEventsProps: mutable.Map[String, String] = mutable.LinkedHashMap[String, String](
      "spark.executor.cores" -> "4",
      "spark.executor.memory" -> "6144M",
      "spark.executor.instances" -> "20"
    )

    // Enforced Spark properties
    val enforcedSparkProps = Map(
      "spark.executor.cores" -> "20",
      "spark.shuffle.manager" -> "org.apache.spark.shuffle.celeborn.SparkShuffleManager",
      "spark.rapids.sql.multiThreadedRead.numThreads" -> "250",
      "spark.vcore.boost.ratio" -> "4",
      "spark.memory.offHeap.enabled" -> "true",
      "spark.memory.offHeap.size" -> "45g",
      "spark.executor.resource.gpu.amount" -> "1",
      "spark.plugins" -> "com.nvidia.spark.SQLPlugin",
      "spark.rapids.memory.host.offHeapLimit.enabled" -> "true",
      "spark.rapids.memory.host.offHeapLimit.size" -> "80g",
      "spark.sql.adaptive.enabled" -> "true"
    )

      // Build target cluster info with worker configuration and enforced properties
      val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
        cpuCores = Some(20),
        memoryGB = Some(120L), // 120g = 120GB
        gpuCount = Some(1),
        gpuMemory = Some("48g"),
        gpuDevice = Some("l20"),
        enforcedSparkProperties = enforcedSparkProps
      )

      val infoProvider = getMockInfoProvider(
        maxInput = 0.0,
        spilledMetrics = Seq(0),
        jvmGCFractions = Seq(0.0),
        propsFromLog = logEventsProps,
        sparkVersion = Some(testSparkVersion)
      )

    // tuningConfigs:
    //   default:
    //   - name: HEAP_PER_CORE
    //     default: 1g
    //   - name: CONC_GPU_TASKS
    //     max: 2
    val defaultTuningConfigsEntries = List(
      TuningConfigEntry(name = "HEAP_PER_CORE", default = "1g"),
      TuningConfigEntry(name = "CONC_GPU_TASKS", max = "2"),
      TuningConfigEntry(name = "OS_RESERVED_MEM", default = "5g")
    )
    val userProvidedTuningConfigs = ToolTestUtils.buildTuningConfigs(
      default = defaultTuningConfigsEntries)

    // Create platform with target cluster info
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, Some(targetClusterInfo))
    configureEventLogClusterInfoForTest(
      platform = platform,
      numCores = 20,
      numWorkers = 4,
      gpuCount = 1,
      sparkProperties = logEventsProps.toMap
    )

    // Build AutoTuner
    val autoTuner = buildAutoTunerForTests(infoProvider, platform,
      sparkMaster = Some(Kubernetes), userProvidedTuningConfigs = Some(userProvidedTuningConfigs))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)

    // Per memory tune logic when offHeapLimit is enabled:
    // totalMemoryMinusReserved = 120(enforced) - 5 = 115g
    // sparkOffHeapMemMB = 45g(enforced)
    // overhead = 115g - 20g - 45g = 50g
    // pinned = min( (45(sparkOffHeapMemMB) + 50(overhead) / 4), 20 * 2(OFFHEAP_PER_CORE))
    //        = 24320m
    // Expected results for onPrem with offHeapLimit enabled
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=20
          |--conf spark.executor.instances=4
          |--conf spark.executor.memory=20g
          |--conf spark.executor.memoryOverhead=50g
          |--conf spark.executor.resource.gpu.amount=1
          |--conf spark.executor.resource.gpu.vendor=nvidia.com
          |--conf spark.locality.wait=0
          |--conf spark.memory.offHeap.enabled=true
          |--conf spark.memory.offHeap.size=45g
          |--conf spark.plugins=com.nvidia.spark.SQLPlugin
          |--conf spark.rapids.memory.host.offHeapLimit.enabled=true
          |--conf spark.rapids.memory.host.offHeapLimit.size=80g
          |--conf spark.rapids.memory.pinnedPool.size=24320m
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=30
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=30
          |--conf spark.rapids.sql.batchSizeBytes=1g
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=250
          |--conf spark.shuffle.manager=org.apache.spark.shuffle.celeborn.SparkShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.task.resource.gpu.amount=0.001
          |--conf spark.vcore.boost.ratio=4
          |
          |Comments:
          |- ${getEnforcedPropertyComment("spark.executor.cores")}
          |- 'spark.executor.memoryOverhead' was not set.
          |- ${getEnforcedPropertyComment("spark.executor.resource.gpu.amount")}
          |- 'spark.executor.resource.gpu.vendor' was not set.
          |- ${getEnforcedPropertyComment("spark.memory.offHeap.enabled")}
          |- ${getEnforcedPropertyComment("spark.memory.offHeap.size")}
          |- ${getEnforcedPropertyComment("spark.plugins")}
          |- ${getEnforcedPropertyComment("spark.rapids.memory.host.offHeapLimit.enabled")}
          |- ${getEnforcedPropertyComment("spark.rapids.memory.host.offHeapLimit.size")}
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- ${getEnforcedPropertyComment("spark.rapids.sql.multiThreadedRead.numThreads")}
          |- ${getEnforcedPropertyComment("spark.shuffle.manager")}
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- ${getEnforcedPropertyComment("spark.sql.adaptive.enabled")}
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${getEnforcedPropertyComment("spark.vcore.boost.ratio")}
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |- $missingGpuDiscoveryScriptComment
          |""".stripMargin
    // Verify expected results match output
    compareOutput(expectedResults, autoTunerOutput)
  }

  // This test verifies that AutoTuner recommends the correct value for
  // "spark.plugins" property.
  test("test 'spark.plugins' is recommended correctly") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "32g",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g")
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some(testSparkVersion))
    val sparkPropsWithMemory = logEventsProps + ("spark.executor.memory" -> "122880MiB")
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM)
    platform.configureClusterInfoFromEventLog(
      coresPerExecutor = 32,
      execsPerNode = 2,
      numExecs = 8,
      numExecutorNodes = 4,
      sparkProperties = sparkPropsWithMemory.toMap,
      systemProperties = Map.empty
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.memory=16g
          |--conf spark.executor.resource.gpu.amount=1
          |--conf spark.locality.wait=0
          |--conf spark.plugins=com.nvidia.spark.SQLPlugin
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=1g
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=20
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.executor.resource.gpu.amount' should be set to allow Spark to schedule GPU resources.
          |- 'spark.plugins' should be set to the class name required for the RAPIDS Accelerator for Apache Spark.
          |  Refer to: https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |- $additionalSparkPluginsComment
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // This test verifies that Qualification Bootstrap ignores existing
  // "spark.plugins" property and RAPIDS plugin is added.
  test("test existing 'spark.plugins' are ignored and RAPIDS plugin is added") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "32g",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.plugins" -> "com.existing.plugin1,com.existing.plugin2")
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM)
    platform.configureClusterInfoFromEventLog(
      coresPerExecutor = 32,
      execsPerNode = 2,
      numExecs = 8,
      numExecutorNodes = 4,
      sparkProperties = logEventsProps.toMap,
      systemProperties = Map.empty
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.memory=16g
          |--conf spark.executor.resource.gpu.amount=1
          |--conf spark.locality.wait=0
          |--conf spark.plugins=com.nvidia.spark.SQLPlugin
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=1g
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=20
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.executor.resource.gpu.amount' should be set to allow Spark to schedule GPU resources.
          |- 'spark.plugins' should include the class name required for the RAPIDS Accelerator for Apache Spark.
          |  Refer to: https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |- $additionalSparkPluginsComment
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }

  // This test verifies that AutoTuner honours enforced values of spark.plugins
  test("test enforced values of 'spark.plugins' are honoured by AutoTuner") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "32g",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g")
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps, Some(testSparkVersion))

    // Define 'spark.plugins' as an enforced property
    val enforcedSparkProperties = Map(
      "spark.plugins" -> "com.existing.plugin1,com.existing.plugin2"
    )

    val targetClusterInfo = ToolTestUtils.buildTargetClusterInfo(
      enforcedSparkProperties = enforcedSparkProperties
    )

    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, Some(targetClusterInfo))
    platform.configureClusterInfoFromEventLog(
      coresPerExecutor = 32,
      execsPerNode = 2,
      numExecs = 8,
      numExecutorNodes = 4,
      sparkProperties = logEventsProps.toMap,
      systemProperties = Map.empty
    )
    val autoTuner = buildAutoTunerForTests(infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.memory=16g
          |--conf spark.executor.resource.gpu.amount=1
          |--conf spark.locality.wait=0
          |--conf spark.plugins=com.existing.plugin1,com.existing.plugin2
          |--conf spark.rapids.memory.pinnedPool.size=4g
          |--conf spark.rapids.shuffle.multiThreaded.reader.threads=20
          |--conf spark.rapids.shuffle.multiThreaded.writer.threads=20
          |--conf spark.rapids.sql.batchSizeBytes=1g
          |--conf spark.rapids.sql.concurrentGpuTasks=3
          |--conf spark.rapids.sql.enabled=true
          |--conf spark.rapids.sql.multiThreadedRead.numThreads=20
          |--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$testSmVersion.RapidsShuffleManager
          |--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
          |--conf spark.sql.adaptive.autoBroadcastJoinThreshold=[FILL_IN_VALUE]
          |--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=4m
          |--conf spark.task.resource.gpu.amount=0.001
          |
          |Comments:
          |- 'spark.executor.resource.gpu.amount' should be set to allow Spark to schedule GPU resources.
          |- ${getEnforcedPropertyComment("spark.plugins")}
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.reader.threads' was not set.
          |- 'spark.rapids.shuffle.multiThreaded.writer.threads' was not set.
          |- 'spark.rapids.sql.batchSizeBytes' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.rapids.sql.enabled' was not set.
          |- 'spark.rapids.sql.multiThreadedRead.numThreads' was not set.
          |- 'spark.shuffle.manager' was not set.
          |- 'spark.sql.adaptive.advisoryPartitionSizeInBytes' was not set.
          |- 'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- ${classPathComments("rapids.jars.missing")}
          |- ${classPathComments("rapids.shuffle.jars")}
          |""".stripMargin
    // scalastyle:on line.size.limit
    compareOutput(expectedResults, autoTunerOutput)
  }
}
