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

package org.apache.spark.sql.rapids.tool.util

import java.io.{BufferedReader, InputStreamReader, IOException}

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.yaml.snakeyaml.{DumperOptions, LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.representer.Representer

import org.apache.spark.internal.Logging


/**
 * To be used by any Property classes for validating the property fields.
 */
trait ValidatableProperties {
  def validate(): Unit
  validate()
}

/**
 * Helper class to load YAML properties files into a specific type.
 *
 * @tparam T The type of configuration to load. Must be a subclass of [[ValidatableProperties]].
 */
class PropertiesLoader[T <: ValidatableProperties](implicit classTag: ClassTag[T]) extends Logging {
  def loadFromContent(content: String): Option[T] = {
    val representer = new Representer(new DumperOptions())
    representer.getPropertyUtils.setSkipMissingProperties(true)
    val constructor = new Constructor(classTag.runtimeClass.asInstanceOf[Class[T]],
      new LoaderOptions())
    val yamlObjNested = new Yaml(constructor, representer)
    val propertiesObj = yamlObjNested.load(content).asInstanceOf[T]
    // After loading from YAML, perform validation of arguments
    propertiesObj.validate()
    Option(propertiesObj)
  }

  def loadFromFile(filePath: String): Option[T] = {
    val path = new Path(filePath)
    var fsIs: FSDataInputStream = null
    try {
      val fs = FileSystem.get(path.toUri, new Configuration())
      fsIs = fs.open(path)
      val reader = new BufferedReader(new InputStreamReader(fsIs))
      val fileContent = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
      loadFromContent(fileContent)
    } catch {
      case _: IOException =>
        logWarning(s"No file found for input path: $filePath")
        None
    } finally {
      if (fsIs != null) {
        fsIs.close()
      }
    }
  }
}

object PropertiesLoader {
  def apply[T <: ValidatableProperties](implicit classTag: ClassTag[T]): PropertiesLoader[T] = {
    new PropertiesLoader[T]
  }
}
