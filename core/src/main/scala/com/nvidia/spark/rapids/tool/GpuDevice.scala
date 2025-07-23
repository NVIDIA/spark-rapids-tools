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
package com.nvidia.spark.rapids.tool

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.rapids.tool.util.StringUtils

object GpuTypes {
  val A100 = "a100"
  val T4 = "t4"
  val V100 = "v100"
  val K80 = "k80"
  val P100 = "p100"
  val P4 = "p4"
  val L4 = "l4"
  val L20 = "l20"
  val A10 = "a10"
  val A10G = "a10G"
}

/**
 * Abstract class representing a GPU device
 */
abstract class GpuDevice {
  def getMemory: String
  def getAdvisoryPartitionSizeInBytes: Option[String] = None
  def getInitialPartitionNum: Option[Int] = None
  // TODO: Improve logic for concurrent tasks
  final def getGpuConcTasks(gpuMemPerTaskMB: Long): Long = {
    StringUtils.convertToMB(getMemory, Some(ByteUnit.BYTE)) / gpuMemPerTaskMB
  }
}

case object A100Gpu extends GpuDevice {
  override def getMemory: String = "40960m"
  override def getAdvisoryPartitionSizeInBytes: Option[String] = Some("64m")
  override def getInitialPartitionNum: Option[Int] = Some(400)
  override def toString: String = GpuTypes.A100
}

case object T4Gpu extends GpuDevice {
  override def getMemory: String = "15109m"

  override def getAdvisoryPartitionSizeInBytes: Option[String] = Some("32m")

  // TODO - what is this based off of?
  override def getInitialPartitionNum: Option[Int] = Some(800)
  override def toString: String = GpuTypes.T4
}

case object L4Gpu extends GpuDevice {
  override def getMemory: String = "24576m"
  override def toString: String = GpuTypes.L4
}

case object L20Gpu extends GpuDevice {
  // TODO: Add recommended values for advisory partition size
  override def getMemory: String = "49152m"
  override def toString: String = GpuTypes.L20
}

case object V100Gpu extends GpuDevice {
  override def getMemory: String = "16384m"
  override def toString: String = GpuTypes.V100
}

case object K80Gpu extends GpuDevice {
  override def getMemory: String = "12288m"
  override def toString: String = GpuTypes.K80
}

case object P100Gpu extends GpuDevice {
  override def getMemory: String = "16384m"
  override def toString: String = GpuTypes.P100
}

case object P4Gpu extends GpuDevice {
  override def getMemory: String = "8192m"
  override def toString: String = GpuTypes.P4
}

case object A10Gpu extends GpuDevice {
  override def getMemory: String = "24576m"
  override def toString: String = GpuTypes.A10
}

case object A10GGpu extends GpuDevice {
  override def getMemory: String = "24576m"
  override def toString: String = GpuTypes.A10G
}

object GpuDevice extends Logging {
  val DEFAULT: GpuDevice = T4Gpu

  lazy val deviceMap: Map[String, GpuDevice] = Map(
    GpuTypes.A100-> A100Gpu,
    GpuTypes.T4 -> T4Gpu,
    GpuTypes.L4 -> L4Gpu,
    GpuTypes.L20 -> L20Gpu,
    GpuTypes.V100 -> V100Gpu,
    GpuTypes.K80 -> K80Gpu,
    GpuTypes.P100 -> P100Gpu,
    GpuTypes.P4 -> P4Gpu,
    GpuTypes.A10 -> A10Gpu,
    GpuTypes.A10G -> A10GGpu
  )

  def createInstance(gpuName: String): Option[GpuDevice] = deviceMap.get(gpuName)
}
