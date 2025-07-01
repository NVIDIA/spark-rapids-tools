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

package com.nvidia.spark.rapids.tool.tuning

import java.util

import scala.beans.BeanProperty

import com.nvidia.spark.rapids.tool.NodeInstanceMapKey

import org.apache.spark.sql.rapids.tool.util.ValidatableProperties

/**
 * Base class to hold the worker information for the target cluster
 * See subclasses below for concrete implementations.
 */
class WorkerInfo (
  @BeanProperty var instanceType: String,
  @BeanProperty var cpuCores: Int,
  @BeanProperty var memoryGB: Long,
  @BeanProperty var gpu: GpuWorkerProps) extends ValidatableProperties {

  def this() = this("", 0, 0L, new GpuWorkerProps())

  /** Returns true if all resource fields are unset or empty. */
  def isEmpty: Boolean = {
    (instanceType == null || instanceType.isEmpty) &&
      (cpuCores == 0 && memoryGB == 0L) && gpu.isEmpty
  }

  /** Returns true if this represents a CSP Info (instance type is set). */
  def isCspInfo: Boolean = {
    instanceType != null && instanceType.nonEmpty
  }

  /** Returns true if this represents an OnPrem Info (CPU or memory is set). */
  def isOnpremInfo: Boolean = {
    cpuCores > 0 || memoryGB > 0
  }

  override def validate(): Unit = {
    if (!isEmpty && isCspInfo && isOnpremInfo) {
      throw new IllegalArgumentException(
        "Both instance type and OnPrem resource values are provided. Please specify either one")
    }
    if (isOnpremInfo && gpu.isEmpty) {
      throw new IllegalArgumentException("GPU information is required for OnPrem " +
        "target cluster configuration.")
    }
    gpu.validate()
  }

  /**
   * Helper method to get the NodeInstanceMapKey for this worker. This
   * will be used as the key in the following map:
   * [[com.nvidia.spark.rapids.tool.Platform.getInstanceMapByName]]
   * @return
   */
  def getNodeInstanceMapKey: Option[NodeInstanceMapKey] = {
    if (this.getInstanceType == null || this.getInstanceType.isEmpty) {
      return None
    }
    val gpuCount = this.getGpu.getCount
    if (gpuCount > 0) {
      Some(NodeInstanceMapKey(instanceType = this.getInstanceType, gpuCount = Some(gpuCount)))
    } else {
      Some(NodeInstanceMapKey(instanceType = this.getInstanceType))
    }
  }
}

/**
 * Class to hold the Spark properties specified for the target cluster.
 * This will be extended in future to include preserved and removed categories.
 */
class SparkProperties(
  @BeanProperty var enforced: util.LinkedHashMap[String, String]) {
  def this() = this(new util.LinkedHashMap[String, String]())

  lazy val enforcedPropertiesMap: Map[String, String] = {
    if (enforced == null || enforced.isEmpty) {
      Map.empty
    } else {
      import scala.collection.JavaConverters.mapAsScalaMapConverter
      enforced.asScala.toMap
    }
  }
}

/**
 * Class to hold the properties for the target cluster.
 * This class will instantiate from the `--target-cluster-info` YAML file
 * using SnakeYAML.
 *
 * @see [[org.apache.spark.sql.rapids.tool.util.PropertiesLoader]]
 */
class TargetClusterProps (
  @BeanProperty var workerInfo: WorkerInfo,
  @BeanProperty var sparkProperties: SparkProperties) extends ValidatableProperties {

  def this() = this(new WorkerInfo(), new SparkProperties())

  // Validating only the worker info for now.
  override def validate(): Unit = {
    workerInfo.validate()
  }

  override def toString: String = {
    s"${getClass.getSimpleName}(workerInfo=$getWorkerInfo, sparkProperties=$getSparkProperties)"
  }
}
