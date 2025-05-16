/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.rapids.tool.SourceClusterInfo
import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * Configuration for the recommended cluster to run the application with Spark RAPIDS.
 */
case class RecommendedClusterConfig(
    numExecutors: Int,
    coresPerExec: Int,
    memoryPerNodeMb: Long, // For onprem or cases where a matching CSP instance type is unavailable
    gpuDevice: GpuDevice,
    numGpusPerNode: Int
) {
  def coresPerNode: Int = {
    coresPerExec * numGpusPerNode
  }
}

/**
 * Base trait for different cluster configuration strategies.
 */
abstract class ClusterConfigurationStrategy(
    platform: Platform,
    sparkProperties: Map[String, String]) {

  /**
   * Calculates the initial number of executors based on the strategy.
   */
  protected def calculateInitialNumExecutors: Int

  private def getInitialNumExecutors: Int = {
    val dynamicAllocationEnabled = Platform.isDynamicAllocationEnabled(sparkProperties)
    val execInstFromProps = sparkProperties.get("spark.executor.instances")
    // If dynamic allocation is disabled, use spark.executor.instances in precedence
    if (execInstFromProps.isDefined && !dynamicAllocationEnabled) {
      // Spark Properties are in order:
      // P0. User defined software properties in Cluster Properties
      // P1. Spark Properties defined in the application information
      execInstFromProps.get.toInt
    } else {
      calculateInitialNumExecutors
    }
  }

  /**
   * Calculates the initial number of cores per executor based on the strategy.
   */
  protected def calculateInitialCoresPerExec: Int

  private def getInitialCoresPerExec: Int = {
    val coresFromProps = sparkProperties.get("spark.executor.cores")
    // Use spark.executor.cores in precedence
    if (coresFromProps.isDefined) {
      coresFromProps.get.toInt
    } else {
      calculateInitialCoresPerExec
    }
  }

  protected def getMemoryPerNodeMb: Long

  protected def getNumGpus: Int

  protected def getGpuDevice: GpuDevice

  /**
   * Generates the recommended cluster configuration based on the strategy.
   *
   * Logic:
   * 1. Calculate the initial number of executors and cores per executor.
   * 2. Calculate the total core count by multiplying initial cores per executor
   *    by the initial number of executors.
   * 3. Retrieve the recommended cores per executor from the platform (default is 16),
   *    for onprem, limit the recommended cores per executor to the total core count.
   * 4. Calculate the recommended number of executors by dividing the total core count
   *    by the recommended cores per executor.
   */
  final def getRecommendedConfig: Option[RecommendedClusterConfig] = {
    val initialNumExecutors = getInitialNumExecutors
    if (initialNumExecutors <= 0) {
      None
    } else {
      val initialCoresPerExec = getInitialCoresPerExec
      val totalCoresCount = initialCoresPerExec * initialNumExecutors
      val recommendedCoresPerExec = if (platform.isPlatformCSP) {
        platform.recommendedCoresPerExec
      } else {
        // For onprem, recommended cores per executor should not exceed total core count
        math.min(platform.recommendedCoresPerExec, totalCoresCount)
      }
      val recommendedNumExecutors =
        math.ceil(totalCoresCount.toDouble / recommendedCoresPerExec).toInt
      Some(RecommendedClusterConfig(
        numExecutors = recommendedNumExecutors,
        coresPerExec = recommendedCoresPerExec,
        memoryPerNodeMb = getMemoryPerNodeMb,
        gpuDevice = getGpuDevice,
        numGpusPerNode = getNumGpus))
    }
  }
}

/**
 * Strategy for cluster configuration based on user specified cluster properties.
 */
class ClusterPropertyBasedStrategy(
    platform: Platform,
    sparkProperties: Map[String, String])
  extends ClusterConfigurationStrategy(platform, sparkProperties) {

  private val clusterProperties = platform.clusterProperties.getOrElse(
      throw new IllegalArgumentException("Cluster properties must be defined"))

  // Calculate the number of GPUs per node based on the cluster properties
  private lazy val numGpusFromProps: Int = {
    // User provided num GPUs, fall back to platform default
    val userProvidedNumGpus = clusterProperties.getGpu.getCount match {
      case count if count > 0 => count
      case _ => platform.defaultNumGpus
    }

    // Apply platform-specific GPU limits for CSP, no limits for on-prem
    if (platform.isPlatformCSP) {
      math.min(userProvidedNumGpus, platform.maxGpusSupported)
    } else {
      userProvidedNumGpus
    }
  }

  override protected def calculateInitialNumExecutors: Int = {
    val numWorkers = math.max(1, clusterProperties.system.numWorkers)
    numGpusFromProps * numWorkers
  }

  override protected def calculateInitialCoresPerExec: Int = {
    val coresPerGpu = clusterProperties.system.getNumCores.toDouble / numGpusFromProps
    math.ceil(coresPerGpu).toInt
  }

  override protected def getMemoryPerNodeMb: Long = {
    StringUtils.convertToMB(clusterProperties.system.getMemory, Some(ByteUnit.BYTE))
  }

  override def getGpuDevice: GpuDevice = {
    GpuDevice.createInstance(clusterProperties.getGpu.name)
      .getOrElse(platform.defaultGpuDevice)
  }

  override def getNumGpus: Int = {
    numGpusFromProps
  }
}

/**
 * Strategy for cluster configuration based on cluster information from event log.
 */
class EventLogBasedStrategy(
    platform: Platform,
    sparkProperties: Map[String, String]
  ) extends ClusterConfigurationStrategy(platform, sparkProperties) {

  private val clusterInfoFromEventLog: SourceClusterInfo = {
    platform.clusterInfoFromEventLog.getOrElse(
      throw new IllegalArgumentException("Cluster information from event log must be defined"))
  }

  // For onprem or cases where a matching CSP instance type is unavailable,
  // Returns the memory per node
  override def getMemoryPerNodeMb: Long = {
    val heapMemMB = clusterInfoFromEventLog.executorHeapMemory
    val overheadMemMB = platform.getExecutorOverheadMemoryMB(sparkProperties)
    heapMemMB + overheadMemMB
  }

  override def calculateInitialNumExecutors: Int = {
    clusterInfoFromEventLog.numExecutors
  }

  override def calculateInitialCoresPerExec: Int = {
    clusterInfoFromEventLog.coresPerExecutor
  }

  // TODO: Add information about the existing GPU on the node
  override def getGpuDevice: GpuDevice = {
    platform.getGpuOrDefault
  }

  // TODO: Add information about the existing GPU count on the node
  override def getNumGpus: Int = {
    platform.defaultNumGpus
  }
}

/**
 * Companion object to create appropriate cluster configuration strategy.
 *
 * Strategy Precedence:
 * 1. Cluster Properties based strategy
 * 2. Event Log based strategy
 */
object ClusterConfigurationStrategy {
  def getStrategy(
      platform: Platform,
      sparkProperties: Map[String, String]): Option[ClusterConfigurationStrategy] = {
    if (platform.clusterProperties.isDefined) {
      // Use strategy based on cluster properties
      Some(new ClusterPropertyBasedStrategy(platform, sparkProperties))
    } else if (platform.clusterInfoFromEventLog.isDefined) {
      // Use strategy based on cluster information from event log
      Some(new EventLogBasedStrategy(platform, sparkProperties))
    } else {
      // Neither cluster properties are defined nor cluster information from event log is available
      None
    }
  }
}
