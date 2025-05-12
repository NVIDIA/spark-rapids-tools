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

import java.io.{BufferedReader, InputStreamReader, IOException}

import scala.beans.BeanProperty

import com.nvidia.spark.rapids.tool.NodeInstanceMapKey
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.yaml.snakeyaml.{DumperOptions, LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.representer.Representer

class WorkerInfo(
  @BeanProperty var instanceType: String,
  @BeanProperty var gpu: GpuWorkerProps) {

  def this() = this("", new GpuWorkerProps())

  /**
   * Helper method to get the NodeInstanceMapKey for this worker. This
   * will be used as the key in the following map:
   * [[com.nvidia.spark.rapids.tool.Platform.getInstanceByResourcesMap]]
   * @return
   */
  def getNodeInstanceMapKey: NodeInstanceMapKey = {
    val gpuCount = this.getGpu.getCount
    if (gpuCount > 0) {
      NodeInstanceMapKey(instanceType = this.getInstanceType, gpuCount = Some(gpuCount))
    } else {
      NodeInstanceMapKey(instanceType = this.getInstanceType)
    }
  }

  override def toString: String = {
    s"WorkerInfo(instanceType=$instanceType, gpu=$gpu)"
  }
}

class TargetClusterInfo(
  @BeanProperty var workerInfo: WorkerInfo) {
  def this() = this(new WorkerInfo())

  override def toString: String = {
    s"TargetClusterInfo(workerInfo=$workerInfo)"
  }
}

object TargetClusterInfo {
  def loadFromFile(filePathOpt: Option[String]): Option[TargetClusterInfo] = {
    filePathOpt.flatMap { filePath =>
      val path = new Path(filePath)
      var fsIs: FSDataInputStream = null
      try {
        val fs = FileSystem.get(path.toUri, new Configuration())
        fsIs = fs.open(path)
        val reader = new BufferedReader(new InputStreamReader(fsIs))
        val fileContent = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")

        val representer = new Representer(new DumperOptions())
        representer.getPropertyUtils.setSkipMissingProperties(true)
        val constructor = new Constructor(classOf[TargetClusterInfo], new LoaderOptions())
        val yamlObjNested = new Yaml(constructor, representer)
        Option(yamlObjNested.load(fileContent).asInstanceOf[TargetClusterInfo])
      } catch {
        case _: IOException =>
          None
      } finally {
        if (fsIs != null) {
          fsIs.close()
        }
      }
    }
  }
}
