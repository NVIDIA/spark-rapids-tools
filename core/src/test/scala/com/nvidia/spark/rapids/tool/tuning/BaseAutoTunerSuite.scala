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

import java.io.File

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.{Platform, PlatformFactory, PlatformNames, ToolTestUtils}
import com.nvidia.spark.rapids.tool.profiling._
import com.nvidia.spark.rapids.tool.tuning.config.TuningConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.exceptions.TestFailedException
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.{RecommendedClusterInfo, ToolUtils}


case class DriverInfoProviderMockTest(unsupportedOps: Seq[DriverLogUnsupportedOperators])
  extends BaseDriverLogInfoProvider() {
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
    val shuffleSkewStages: Set[Long],
    val scanStagesWithGpuOom: Boolean,
    val shuffleStagesWithOom: Boolean,
    val maxColumnarExchangeDataSizeBytes: Option[Long] = None)
    extends BaseProfilingAppSummaryInfoProvider {
  override def isAppInfoAvailable = true
  override def getMaxInput: Double = maxInput
  override def getMeanInput: Double = meanInput
  override def getMeanShuffleRead: Double = meanShuffleRead
  override def getSpilledMetrics: Seq[Long] = spilledMetrics
  override def getJvmGCFractions: Seq[Double] = jvmGCFractions
  override def getAllProperties: Map[String, String] = propsFromLog.toMap
  override def getRapidsProperty(propKey: String): Option[String] = propsFromLog.get(propKey)
  override def getSparkProperty(propKey: String): Option[String] = propsFromLog.get(propKey)
  override def getSystemProperty(propKey: String): Option[String] = propsFromLog.get(propKey)
  override def getSparkVersion: Option[String] = sparkVersion
  override def getRapidsJars: Seq[String] = rapidsJars
  override def getDistinctLocationPct: Double = distinctLocationPct
  override def getRedundantReadSize: Long = redundantReadSize
  override def getShuffleStagesWithPosSpilling: Set[Long] = shuffleStagesWithPosSpilling
  override def getShuffleSkewStages: Set[Long] = shuffleSkewStages
  override def hasScanStagesWithGpuOom: Boolean = scanStagesWithGpuOom
  override def hasShuffleStagesWithOom: Boolean = shuffleStagesWithOom
  override def getMaxColumnarExchangeDataSizeBytes: Option[Long] = maxColumnarExchangeDataSizeBytes

  /**
   * Sets the spark master property in the properties map.
   * This method guarantees that the spark master property is set only once.
   */
  final def setSparkMaster(sparkMaster: String): Unit = {
    require(!propsFromLog.contains("spark.master"),
      "'spark.master' is already set in the properties map. " +
        "Remove it before before setting it again.")
    propsFromLog.put("spark.master", sparkMaster)
  }
}

/**
 * Base class for AutoTuner test suites
 */
abstract class BaseAutoTunerSuite extends AnyFunSuite with BeforeAndAfterEach
  with Logging with AutoTunerStaticComments {

  // Spark runtime version used for testing
  def testSparkVersion: String = ToolUtils.sparkRuntimeVersion
  // Databricks version used for testing
  def testDatabricksVersion: String = "12.2.x-aarch64-scala2.12"
  // RapidsShuffleManager version used for testing
  def testSmVersion: String = testSparkVersion.filterNot(_ == '.')
  // RapidsShuffleManager version used for testing Databricks
  def testSmVersionDatabricks: String = "332db"
  //  Subclasses to provide the AutoTuner configuration to use
  val autoTunerHelper: AutoTunerHelper

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

  protected def getMockInfoProvider(maxInput: Double,
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
      shuffleSkewStages: Set[Long] = Set(),
      scanStagesWithGpuOom: Boolean = false,
      shuffleStagesWithOom: Boolean = false,
      maxColumnarExchangeDataSizeBytes: Option[Long] = None): AppInfoProviderMockTest = {
    new AppInfoProviderMockTest(maxInput, spilledMetrics, jvmGCFractions, propsFromLog,
      sparkVersion, rapidsJars, distinctLocationPct, redundantReadSize, meanInput, meanShuffleRead,
      shuffleStagesWithPosSpilling, shuffleSkewStages, scanStagesWithGpuOom,
      shuffleStagesWithOom, maxColumnarExchangeDataSizeBytes)
  }

  /**
   * Helper method to compare the expected results with the actual output from the AutoTuner.
   *
   * In case of a mismatch, displays the complete output, which is useful for updating the
   * expected results.
   */
  protected def compareOutput(expectedResults: String, autoTunerOutput: String): Unit = {
    val outputsMatch = expectedResults == autoTunerOutput
    assert(outputsMatch,
      s"""|=== Expected ===
          |$expectedResults
          |
          |=== Actual ===
          |$autoTunerOutput
          |""".stripMargin)
  }

  // Define a mapping of platform names to their default SparkMaster types
  private lazy val platformToDefaultMasterMap: Map[String, SparkMaster] = Map(
    PlatformNames.EMR -> Yarn,
    PlatformNames.DATAPROC -> Yarn,
    PlatformNames.DATAPROC_GKE -> Kubernetes,
    PlatformNames.DATABRICKS_AWS -> Standalone,
    PlatformNames.DATABRICKS_AZURE -> Standalone,
    PlatformNames.DATAPROC_SL -> Standalone,
    PlatformNames.ONPREM -> Local
  )

  /**
   * Helper method to configure cluster info from event log for testing purposes.
   */
  protected def configureEventLogClusterInfoForTest(
      platform: Platform,
      numCores: Int = 32,
      numWorkers: Int = 4,
      gpuCount: Int = 1,
      sparkProperties: Map[String, String] = Map.empty): Unit = {
    val coresPerExecutor = numCores / gpuCount
    val execsPerNode = gpuCount
    val numExecutors = numWorkers * execsPerNode
    platform.configureClusterInfoFromEventLog(
      coresPerExecutor, execsPerNode, numExecutors, numWorkers,
      sparkProperties, Map.empty
    )
  }

  /**
   * Helper method to create an instance of the AutoTuner from the cluster properties.
   * It also sets the appropriate 'spark.master' configuration if provided or uses
   * the default based on the platform.
   */
  final def buildAutoTunerForTests(
    mockInfoProvider: AppInfoProviderMockTest,
    platform: Platform = PlatformFactory.createInstance(),
    sparkMaster: Option[SparkMaster] = None,
    userProvidedTuningConfigs: Option[TuningConfiguration] = None
  ): AutoTuner = {

    // Determine the SparkMaster using provided value or platform-based default
    val resolvedSparkMaster = sparkMaster.getOrElse {
      platformToDefaultMasterMap.getOrElse(
        platform.platformName,
        throw new IllegalArgumentException(s"Unsupported platform: ${platform.platformName}")
      )
    }

    // Convert SparkMaster enum to a mock string representation
    val mockSparkMasterStr = resolvedSparkMaster match {
      case Local => "local"
      case Standalone => "spark://localhost:7077"
      case Yarn => "yarn"
      case Kubernetes => "k8s://https://my-cluster-endpoint.example.com:6443"
    }

    // Set the spark master in the mock info provider
    mockInfoProvider.setSparkMaster(mockSparkMasterStr)

    // Build and return the AutoTuner
    autoTunerHelper.buildAutoTunerFromProps(mockInfoProvider, platform,
      userProvidedTuningConfigs = userProvidedTuningConfigs)
  }

  /**
   * Helper method to assert that the recommended cluster info matches the expected
   */
  def assertRecommendedClusterInfo(
      actualClusterInfoFile: File,
      expectedClusterInfo: RecommendedClusterInfo): Unit = {
    val recommendedClusterInfo =
      ToolTestUtils.loadClusterSummaryFromJson(actualClusterInfoFile).recommendedClusterInfo
        .getOrElse {
          throw new TestFailedException(
            s"Failed to load recommended cluster info from $actualClusterInfoFile", 0)
        }

    val clusterInfoMatches = recommendedClusterInfo == expectedClusterInfo
    assert(clusterInfoMatches,
      s"""
         |Actual cluster info does not match the expected cluster info.
         |Actual: $recommendedClusterInfo
         |Expected: $expectedClusterInfo
         |""".stripMargin)
  }
}
