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

import com.nvidia.spark.rapids.tool.NodeInstanceMapKey
import java.util
import scala.beans.BeanProperty

class WorkerInfo(
  @BeanProperty var instanceType: String,
  @BeanProperty var gpu: GpuWorkerProps) {

  def this() = this("", new GpuWorkerProps())

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

class TargetClusterProps(
  @BeanProperty var workerInfo: WorkerInfo,
  @BeanProperty var sparkProperties: SparkProperties) {

  def this() = this(new WorkerInfo(), new SparkProperties())

  override def toString: String = {
    s"${getClass.getSimpleName}(workerInfo=$workerInfo, sparkProperties=$sparkProperties)"
  }
}
