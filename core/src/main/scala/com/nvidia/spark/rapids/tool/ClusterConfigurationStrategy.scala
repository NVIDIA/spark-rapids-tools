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
    memoryPerNodeMb: Long) {

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
trait ClusterConfigurationStrategy {
  protected val numGpus: Int
  protected val maxGpusSupported: Int
  protected val sparkProperties: Map[String, String]

  private val MIN_CORES_PER_EXEC = 16

  protected def getNumExecutors: Int
  protected def getNumExecsPerNode: Int
  protected def getMemoryPerNodeMb: Long
  protected def getNumGpusPerNode: Int
  protected def getCoresPerExec: Int

  /**
   * Generates the recommended cluster configuration based on the strategy.
   */
  final def getRecommendedConfig: Option[RecommendedClusterConfig] = {
    val numExecutors = getNumExecutors
    if (numExecutors <= 0) {
      None
    } else {
      val coresPerExec = getCoresPerExec
      val totalCoreCount = coresPerExec * numExecutors
      val minCoresPerExec = Math.max(coresPerExec, MIN_CORES_PER_EXEC)
      val recommendedCoresPerExec = Math.min(minCoresPerExec, totalCoreCount)
      val recommendedNumExecutors =
        Math.ceil(totalCoreCount.toDouble / recommendedCoresPerExec).toInt
      val recommendedNumExecPerNode = Math.min(maxGpusSupported, recommendedNumExecutors)

      Some(RecommendedClusterConfig(
        numExecutors = recommendedNumExecutors,
        numExecsPerNode = recommendedNumExecPerNode,
        coresPerExec = recommendedCoresPerExec,
        memoryPerNodeMb = getMemoryPerNodeMb))
    }
  }
}

/**
 * Strategy for cluster configuration based on user specified cluster properties.
 */
class ClusterPropertyBasedStrategy(
    clusterProperties: ClusterProperties,
    val numGpus: Int,
    val maxGpusSupported: Int,
    val sparkProperties: Map[String, String]) extends ClusterConfigurationStrategy {

  override protected def getNumExecutors: Int = {
    val numWorkers = Math.max(1, clusterProperties.system.numWorkers)
    numGpus * numWorkers
  }

  override protected def getNumExecsPerNode: Int = {
    1
  }

  override protected def getMemoryPerNodeMb: Long = {
    StringUtils.convertToMB(clusterProperties.system.getMemory)
  }

  override protected def getNumGpusPerNode: Int = {
    Math.min(1, maxGpusSupported)
  }

  override protected def getCoresPerExec: Int = {
    clusterProperties.system.getNumCores / getNumGpusPerNode
  }
}

/**
 * Strategy for cluster configuration based on cluster information from event log.
 */
class EventLogBasedStrategy(
    clusterInfoFromEventLog: ExistingClusterInfo,
    val numGpus: Int,
    val maxGpusSupported: Int,
    val sparkProperties: Map[String, String]) extends ClusterConfigurationStrategy {

  override protected def getNumExecutors: Int = {
    val dynamicAllocationEnabled = Platform.isDynamicAllocationEnabled(sparkProperties)
    val execInstFromProps = sparkProperties.get("spark.executor.instances")

    if (execInstFromProps.isDefined && !dynamicAllocationEnabled) {
      execInstFromProps.get.toInt
    } else {
      clusterInfoFromEventLog.numExecutors
    }
  }

  override protected def getNumExecsPerNode: Int = {
    if (clusterInfoFromEventLog.numExecsPerNode == -1) {
      maxGpusSupported
    }
    else {
      clusterInfoFromEventLog.numExecsPerNode
    }
  }

  override protected def getMemoryPerNodeMb: Long = {
    val heapMemMB = clusterInfoFromEventLog.executorHeapMemory
    val overheadMemMB = Platform.getExecutorOverheadMemoryMB(sparkProperties)
    (heapMemMB + overheadMemMB) * getNumExecsPerNode
  }

  override protected def getNumGpusPerNode: Int = {
    Math.max(numGpus, Math.min(getNumExecsPerNode, maxGpusSupported))
  }

  override protected def getCoresPerExec: Int = {
    clusterInfoFromEventLog.coresPerExecutor
  }
}

/**
 * Companion object to create appropriate cluster configuration strategy.
 */
object ClusterConfigurationStrategy {
  def getStrategy(
      clusterProperties: Option[ClusterProperties],
      clusterInfoFromEventLog: Option[ExistingClusterInfo],
      numGpus: Int,
      maxGpusSupported: Int,
      sparkProperties: Map[String, String]): Option[ClusterConfigurationStrategy] = {
    if (clusterProperties.isDefined) {
      // Use strategy based on cluster properties
      Some(new ClusterPropertyBasedStrategy(
        clusterProperties.get, numGpus, maxGpusSupported, sparkProperties))
    } else if (clusterInfoFromEventLog.isDefined) {
      // Use strategy based on cluster information from event log
      Some(new EventLogBasedStrategy(
        clusterInfoFromEventLog.get, numGpus, maxGpusSupported, sparkProperties))
    } else {
      // Neither cluster properties are defined nor cluster information from event log is available
      None
    }
  }
}
