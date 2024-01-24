/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.tool.profiling.ClusterProperties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.GpuTypes
import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * Abstract class representing a GPU device
 */
abstract class GpuDevice {
  def getMemory: String
  def getAdvisoryPartitionSizeInBytes: Option[String] = None
  def getInitialPartitionNum: Option[Int] = None
  // TODO: Improve logic for concurrent tasks
  final def getGpuConcTasks: Long =
    StringUtils.convertToMB(getMemory) / GpuDevice.DEF_GPU_MEM_PER_TASK_MB
}

case object A100Gpu extends GpuDevice {
  override def getMemory: String = GpuTypes.getGpuMem(GpuTypes.A100)
  override def getAdvisoryPartitionSizeInBytes: Option[String] = Some("64m")
  override def getInitialPartitionNum: Option[Int] = Some(400)
  override def toString = "Nvidia A100"
}

case object T4Gpu extends GpuDevice {
  override def getMemory: String = GpuTypes.getGpuMem(GpuTypes.T4)
  override def toString = "Nvidia T4"
}

case object L4Gpu extends GpuDevice {
  override def getMemory: String = GpuTypes.getGpuMem(GpuTypes.L4)
  override def toString = "Nvidia L4"
}

object GpuDevice extends Logging {
  // Amount of GPU memory to use per concurrent task in megabytes.
  // Current estimate is based on T4 GPUs with 14.75 GB and we want
  // to run with 2 concurrent by default on T4s.
  private val DEF_GPU_MEM_PER_TASK_MB = 7500L

  /**
   * Creates a specific GPU device based on cluster properties and platform.
   *
   * @param clusterProps Cluster properties containing GPU information.
   * @param platform Platform providing GPU type fallback.
   * @return Instance of the appropriate GpuDevice subclass.
   */
  def from(clusterProps: ClusterProperties, platform: Platform): GpuDevice = {
    val propsGpuType = clusterProps.getGpu.getName
    val selectedGpuType = platform.getGpuType.getOrElse(propsGpuType)
    selectedGpuType match {
      case GpuTypes.T4   => T4Gpu
      case GpuTypes.L4   => L4Gpu
      case GpuTypes.A100 => A100Gpu
      case gpuType =>
        logWarning(s"Unrecognized GPU type: $gpuType. Using ${GpuTypes.T4} as GPU type.")
        T4Gpu
    }
  }
}
