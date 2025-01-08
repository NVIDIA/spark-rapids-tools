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

import com.nvidia.spark.rapids.tool.tuning.ClusterProperties

import org.apache.spark.sql.rapids.tool.ExistingClusterInfo
import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * Represents the configuration for a recommended cluster.
 */
case class RecommendedClusterConfig(
  numExecutors: Int,
  numExecsPerNode: Int,
  coresPerExec: Int,
  memoryPerNodeMb: Long // For onprem or cases where a matching CSP instance type is unavailable
) {

  def coresPerNode: Int = {
    coresPerExec * numExecsPerNode
  }

  def numGpusPerNode: Int = {
    numExecsPerNode
  }
}

/**
 * Base trait for different cluster configuration strategies.
 */
abstract class ClusterConfigurationStrategy(maxGpusSupported: Int) {
  private val MIN_CORES_PER_EXEC = 16

  // Returns the initial number of executors to be used for the app.
  protected def getInitialNumExecutors: Int
  // Returns the initial number of cores per executor to be used for the app.
  protected def getInitialCoresPerExec: Int
  // For onprem or cases where a matching CSP instance type is unavailable,
  // Returns the memory per node
  protected def getMemoryPerNodeMb(numExecPerNode: Int): Long

  /**
   * Helper function to limit the value within the specified range
   */
  private final def limit(value: Int, min: Int, max: Int): Int = {
    Math.max(min, Math.min(max, value))
  }

  /**
   * Generates the recommended cluster configuration based on the strategy.
   *
   * Logic:
   * 1. Calculate the initial number of executors and cores per executor.
   * 2. Calculate the total core count by multiplying initial cores per executor
   *    by the initial number of executors.
   * 3. Determine the recommended cores per executor, limit it within the
   *    bounds of `MIN_CORES_PER_EXEC` and `totalCoresCount`.
   * 4. Calculate the recommended number of executors by dividing the total core count
   *    by the recommended cores per executor.
   * 5. Limit the number of executors per node to the maximum supported GPUs.
   */
  final def getRecommendedConfig: Option[RecommendedClusterConfig] = {
    val initialNumExecutors = getInitialNumExecutors
    if (initialNumExecutors <= 0) {
      None
    } else {
      val initialCoresPerExec = getInitialCoresPerExec
      val totalCoresCount = initialCoresPerExec * initialNumExecutors
      val recommendedCoresPerExec = limit(initialCoresPerExec, MIN_CORES_PER_EXEC, totalCoresCount)
      val recommendedNumExecutors =
        Math.ceil(totalCoresCount.toDouble / recommendedCoresPerExec).toInt
      val recommendedNumExecPerNode = Math.min(maxGpusSupported, recommendedNumExecutors)
      // Memory per node is needed for onprem or cases where a matching CSP instance type
      // is unavailable
      val recommendedMemoryPerNodeMb = getMemoryPerNodeMb(recommendedNumExecPerNode)

      Some(RecommendedClusterConfig(
        numExecutors = recommendedNumExecutors,
        numExecsPerNode = recommendedNumExecPerNode,
        coresPerExec = recommendedCoresPerExec,
        memoryPerNodeMb = recommendedMemoryPerNodeMb))
    }
  }
}

/**
 * Strategy for cluster configuration based on user specified cluster properties.
 */
class ClusterPropertyBasedStrategy(
    clusterProperties: ClusterProperties,
    maxGpusSupported: Int) extends ClusterConfigurationStrategy(maxGpusSupported) {

  // Calculate the number of GPUs per node based on the cluster properties
  private val numGpusPerNode: Int = {
    val gpuOption = clusterProperties.getGpu
    val numGpusPerNode = if (!gpuOption.isEmpty && gpuOption.getCount > 0) {
      gpuOption.getCount
    } else {
      // if the cluster properties do not specify GPU count, assume 1 GPU per node
      1
    }
    Math.min(numGpusPerNode, maxGpusSupported)
  }

  override protected def getInitialNumExecutors: Int = {
    val numWorkers = Math.max(1, clusterProperties.system.numWorkers)
    numGpusPerNode * numWorkers
  }

  override protected def getInitialCoresPerExec: Int = {
    val coresPerGpu = clusterProperties.system.getNumCores.toDouble / numGpusPerNode
    Math.ceil(coresPerGpu).toInt
  }

  override protected def getMemoryPerNodeMb(numExecPerNode: Int): Long = {
    StringUtils.convertToMB(clusterProperties.system.getMemory)
  }
}

/**
 * Strategy for cluster configuration based on cluster information from event log.
 */
class EventLogBasedStrategy(
  clusterInfoFromEventLog: ExistingClusterInfo,
  sparkProperties: Map[String, String],
  maxGpusSupported: Int) extends ClusterConfigurationStrategy(maxGpusSupported) {

  override protected def getInitialNumExecutors: Int = {
    val dynamicAllocationEnabled = Platform.isDynamicAllocationEnabled(sparkProperties)
    val execInstFromProps = sparkProperties.get("spark.executor.instances")

    if (execInstFromProps.isDefined && !dynamicAllocationEnabled) {
      execInstFromProps.get.toInt
    } else {
      clusterInfoFromEventLog.numExecutors
    }
  }

  override protected def getInitialCoresPerExec: Int = {
    clusterInfoFromEventLog.coresPerExecutor
  }

  override protected def getMemoryPerNodeMb(numExecPerNode: Int): Long = {
    val heapMemMB = clusterInfoFromEventLog.executorHeapMemory
    val overheadMemMB = Platform.getExecutorOverheadMemoryMB(sparkProperties)
    (heapMemMB + overheadMemMB) * numExecPerNode
  }
}

/**
 * Companion object to create appropriate cluster configuration strategy.
 */
object ClusterConfigurationStrategy {
  def getStrategy(
      clusterProperties: Option[ClusterProperties],
      clusterInfoFromEventLog: Option[ExistingClusterInfo],
      maxGpusSupported: Int,
      sparkProperties: Map[String, String]): Option[ClusterConfigurationStrategy] = {
    if (clusterProperties.isDefined) {
      // Use strategy based on cluster properties
      Some(new ClusterPropertyBasedStrategy(clusterProperties.get, maxGpusSupported))
    } else if (clusterInfoFromEventLog.isDefined) {
      // Use strategy based on cluster information from event log
      Some(new EventLogBasedStrategy(clusterInfoFromEventLog.get, sparkProperties,
        maxGpusSupported))
    } else {
      // Neither cluster properties are defined nor cluster information from event log is available
      None
    }
  }
}
