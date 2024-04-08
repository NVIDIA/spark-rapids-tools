/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.tool

import scala.annotation.tailrec

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.ClusterInfo

/**
 *  Utility object containing constants for various platform names.
 */
object PlatformNames {
  val DATABRICKS_AWS = "databricks-aws"
  val DATABRICKS_AZURE = "databricks-azure"
  val DATAPROC = "dataproc"
  val DATAPROC_GKE = "dataproc-gke"
  val DATAPROC_SL = "dataproc-serverless"
  val EMR = "emr"
  val ONPREM = "onprem"
  val DEFAULT: String = ONPREM

  /**
   * Return a list of all supported platform names.
   */
  def getAllNames: List[String] = List(
    DATABRICKS_AWS, DATABRICKS_AZURE, DATAPROC, EMR, ONPREM,
    s"$DATAPROC-$L4Gpu", s"$DATAPROC-$T4Gpu",
    s"$DATAPROC_GKE-$L4Gpu", s"$DATAPROC_GKE-$T4Gpu",
    s"$DATAPROC_SL-$L4Gpu", s"$EMR-$A10Gpu", s"$EMR-$T4Gpu"
  )
}

/**
 * Represents a platform and its associated recommendations.
 *
 * @param gpuDevice Gpu Device present in the platform
 */
abstract class Platform(var gpuDevice: Option[GpuDevice]) {
  val platformName: String
  val defaultGpuDevice: GpuDevice

  /**
   * Recommendations to be excluded from the list of recommendations.
   * These have the highest priority.
   */
  val recommendationsToExclude: Set[String] = Set.empty
  /**
   * Recommendations to be included in the final list of recommendations.
   * These properties should be specific to the platform and not general Spark properties.
   * For example: "spark.databricks.optimizer.dynamicFilePruning" for the Databricks platform.
   *
   * Represented as a tuple of (propertyKey, propertyValue).
   */
  val recommendationsToInclude: Seq[(String, String)] = Seq.empty
  /**
   * Dynamically calculates the recommendation for a specific Spark property by invoking
   * the appropriate function based on `sparkProperty`.
   * TODO: Implement this function and integrate with existing code in AutoTuner
   *
   * @param sparkProperty The Spark property for which the recommendation is calculated.
   * @param args Variable list of arguments passed to the calculation function for dynamic
   *             processing.
   * @return Optional string containing the recommendation, or `None` if unavailable.
   */
  def getRecommendation(sparkProperty: String, args: Any*): Option[String] = None

  /**
   * Checks if the `property` is valid:
   * 1. It should not be in exclusion list
   *   OR
   * 2. It should be in the inclusion list
   */
  def isValidRecommendation(property: String): Boolean = {
    !recommendationsToExclude.contains(property) ||
      recommendationsToInclude.map(_._1).contains(property)
  }

  /**
   * Checks if the `comment` is valid:
   * 1. It should not have any property from the exclusion list
   */
  def isValidComment(comment: String): Boolean = {
    recommendationsToExclude.forall(excluded => !comment.contains(excluded))
  }

  def getOperatorScoreFile: String = {
    s"operatorsScore-$platformName-$getGpuOrDefault.csv"
  }

  final def getGpuOrDefault: GpuDevice = gpuDevice.getOrElse(defaultGpuDevice)

  final def setGpuDevice(gpuDevice: GpuDevice): Unit = {
    this.gpuDevice = Some(gpuDevice)
  }

  /**
   * Important system properties that should be retained based on platform.
   */
  def getRetainedSystemProps: Set[String] = Set.empty

  def createClusterInfo(coresPerExecutor: Int, numExecutorNodes: Int,
      sparkProperties: Map[String, String], systemProperties: Map[String, String]): ClusterInfo = {
    val driverHost = sparkProperties.get("spark.driver.host")
    ClusterInfo(coresPerExecutor, numExecutorNodes, driverHost = driverHost)
  }

  override def toString: String = {
    val gpuStr = gpuDevice.fold("")(gpu => s"-$gpu")
    s"$platformName$gpuStr"
  }
}

abstract class DatabricksPlatform(gpuDevice: Option[GpuDevice]) extends Platform(gpuDevice) {
  override val defaultGpuDevice: GpuDevice = T4Gpu

  override val recommendationsToExclude: Set[String] = Set(
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.executor.memory",
    "spark.executor.memoryOverhead"
  )
  override val recommendationsToInclude: Seq[(String, String)] = Seq(
    ("spark.databricks.optimizer.dynamicFilePruning", "false")
  )

  override def createClusterInfo(coresPerExecutor: Int, numExecutorNodes: Int,
      sparkProperties: Map[String, String], systemProperties: Map[String, String]): ClusterInfo = {
    val executorInstance = sparkProperties.get("spark.databricks.workerNodeTypeId")
    val driverInstance = sparkProperties.get("spark.databricks.driverNodeTypeId")
    val clusterId = sparkProperties.get("spark.databricks.clusterUsageTags.clusterId")
    val driverHost = sparkProperties.get("spark.driver.host")
    val clusterName = sparkProperties.get("spark.databricks.clusterUsageTags.clusterName")
    ClusterInfo(coresPerExecutor, numExecutorNodes, executorInstance,
      driverInstance, driverHost, clusterId, clusterName)
  }
}

class DatabricksAwsPlatform(gpuDevice: Option[GpuDevice]) extends DatabricksPlatform(gpuDevice) {
  override val platformName: String =  PlatformNames.DATABRICKS_AWS
  override val defaultGpuDevice: GpuDevice = A10GGpu
}

class DatabricksAzurePlatform(gpuDevice: Option[GpuDevice]) extends DatabricksPlatform(gpuDevice) {
  override val platformName: String =  PlatformNames.DATABRICKS_AZURE
}

class DataprocPlatform(gpuDevice: Option[GpuDevice]) extends Platform(gpuDevice) {
  override val platformName: String =  PlatformNames.DATAPROC
  override val defaultGpuDevice: GpuDevice = T4Gpu
}

class DataprocServerlessPlatform(gpuDevice: Option[GpuDevice]) extends DataprocPlatform(gpuDevice) {
  override val platformName: String =  PlatformNames.DATAPROC_SL
  override val defaultGpuDevice: GpuDevice = L4Gpu
}

class DataprocGkePlatform(gpuDevice: Option[GpuDevice]) extends DataprocPlatform(gpuDevice) {
  override val platformName: String =  PlatformNames.DATAPROC_GKE
}

class EmrPlatform(gpuDevice: Option[GpuDevice]) extends Platform(gpuDevice) {
  override val platformName: String =  PlatformNames.EMR
  override val defaultGpuDevice: GpuDevice = A10GGpu

  override def getRetainedSystemProps: Set[String] = Set("EMR_CLUSTER_ID")

  override def createClusterInfo(coresPerExecutor: Int, numExecutorNodes: Int,
      sparkProperties: Map[String, String], systemProperties: Map[String, String]): ClusterInfo = {
    val clusterId = systemProperties.get("EMR_CLUSTER_ID")
    val driverHost = sparkProperties.get("spark.driver.host")
    ClusterInfo(coresPerExecutor, numExecutorNodes, clusterId = clusterId, driverHost = driverHost)
  }
}

class OnPremPlatform(gpuDevice: Option[GpuDevice]) extends Platform(gpuDevice) {
  override val platformName: String =  PlatformNames.ONPREM
  override val defaultGpuDevice: GpuDevice = A100Gpu
}

/**
 * Factory for creating instances of different platforms.
 * This factory supports various platforms and provides methods for creating
 * corresponding platform instances.
 */
object PlatformFactory extends Logging {
  /**
   * Extracts platform and GPU names from the provided platform key.
   * Assumption: If the last part contains a number, we assume it is GPU name
   *
   * E.g.,
   * - 'emr-t4': Platform: emr, GPU: t4
   * - 'dataproc-gke-l4': Platform dataproc-gke, GPU: l4
   * - 'databricks-aws': Platform databricks-aws, GPU: None
   */
  private def extractPlatformGpuName(platformKey: String): (String, Option[String]) = {
    val parts = platformKey.split('-')
    val numberPattern = ".*\\d.*".r
    // If the last part contains a number, we assume it is GPU name
    if (numberPattern.findFirstIn(parts.last).isDefined) {
      (parts.init.toList.mkString("-"), Some(parts.last))
    } else {
      // If no GPU information is present, return the entire platform key as the
      // platform name and None for GPU
      (parts.toList.mkString("-"), None)
    }
  }

  @throws[IllegalArgumentException]
  @tailrec
  private def createPlatformInstance(platformName: String,
      gpuDevice: Option[GpuDevice]): Platform = platformName match {
    case PlatformNames.DATABRICKS_AWS => new DatabricksAwsPlatform(gpuDevice)
    case PlatformNames.DATABRICKS_AZURE => new DatabricksAzurePlatform(gpuDevice)
    case PlatformNames.DATAPROC => new DataprocPlatform(gpuDevice)
    case PlatformNames.DATAPROC_GKE => new DataprocGkePlatform(gpuDevice)
    case PlatformNames.DATAPROC_SL => new DataprocServerlessPlatform(gpuDevice)
    case PlatformNames.EMR => new EmrPlatform(gpuDevice)
    case PlatformNames.ONPREM => new OnPremPlatform(gpuDevice)
    case p if p.isEmpty =>
      logInfo(s"Platform is not specified. Using ${PlatformNames.DEFAULT} as default.")
      createPlatformInstance(PlatformNames.DEFAULT, gpuDevice)
    case _ =>
      throw new IllegalArgumentException(s"Unsupported platform: $platformName. " +
        s"Options include ${PlatformNames.getAllNames.mkString(", ")}.")
  }

  /**
   * Creates an instance of `Platform` based on the specified platform key.
   *
   * @param platformKey The key identifying the platform. Defaults to `PlatformNames.DEFAULT`.
   */
  def createInstance(platformKey: String = PlatformNames.DEFAULT): Platform = {
    val (platformName, gpuName) = extractPlatformGpuName(platformKey)
    val gpuDevice = gpuName.flatMap(GpuDevice.createInstance)
    val platform = createPlatformInstance(platformName, gpuDevice)
    logInfo(s"Using platform: $platform")
    platform
  }
}
