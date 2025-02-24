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
import scala.collection.JavaConverters._
import scala.collection.breakOut

import org.yaml.snakeyaml.{DumperOptions, LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.representer.Representer

import org.apache.spark.sql.rapids.tool.util.UTF8Source

/**
 * A wrapper to the hold the tuning entry information.
 * @param label the property name
 * @param description used to explain the importance of that property and how it is used
 * @param enabled global flag to enable/disable the tuning entry. This is used to turn off a
 *                tuning entry
 * @param level This is used to group the tuning entries (job/cluster)
 * @param category Indicates the purpose of that property for RAPIDS.
 *                 "functionality": required to enable RAPIDS
 *                 "tuning": required to tune the runtime.
 * @param bootstrapEntry When true, the property should be added to the bootstrap configuration.
 *                       Default is true.
 * @param defaultSpark The default value of the property in Spark. This is used to set the
 *                     originalValue of the property in case it is not set by the eventlog.
 * @param comments The defaults comments to be loaded for the entry. It is a map to represent
 *                 three different types of comments:
 *                 1. "missing" to represent the default comment to be appended to the AutoTuner's
 *                    comment when the property is missing.
 *                 2. "persistent" to represent a comment that always shows up in the AutoTuner's
 *                    output.
 *                 3. "updated" to represent a comment that shows when a property is being set by
 *                    the Autotuner.
 */
class TuningEntryDefinition(
    @BeanProperty var label: String,
    @BeanProperty var description: String,
    @BeanProperty var enabled: Boolean,
    @BeanProperty var level: String,
    @BeanProperty var category: String,
    @BeanProperty var bootstrapEntry: Boolean,
    @BeanProperty var defaultSpark: String,
    @BeanProperty var comments: util.LinkedHashMap[String, String]) {
  def this() = {
    this(label = "", description = "", enabled = true, level = "", category = "",
      bootstrapEntry = true, defaultSpark = null,
      comments = new util.LinkedHashMap[String, String]())
  }

  def isEnabled(): Boolean = {
    enabled
  }
  def isBootstrap(): Boolean = {
    bootstrapEntry || label.startsWith("spark.rapids.")
  }

  /**
   * Indicates if the property has a default value in Spark. This implies that the default value
   * can be used to set the original value of the property.
   * @return true if the property has a default value in Spark.
   */
  def hasDefaultSpark(): Boolean = {
    defaultSpark != null
  }

  def getMissingComment(): Option[String] = {
    Option(comments.get("missing"))
  }

  def getPersistentComment(): Option[String] = {
    Option(comments.get("persistent"))
  }

  def getUpdatedComment(): Option[String] = {
    Option(comments.get("updated"))
  }
}

class TuningEntries(
  @BeanProperty var tuningDefinitions: java.util.List[TuningEntryDefinition]) {
  def this() = {
    this(new java.util.ArrayList[TuningEntryDefinition]())
  }
}


object TuningEntryDefinition {
  // A static Map between the propertyName and the TuningEntryDefinition
  lazy val TUNING_TABLE: Map[String, TuningEntryDefinition] = loadTable()

  /**
   * Load the tuning table from the yaml file.
   * @return a map between property name and the TuningEntryDefinition
   */
  private def loadTable(): Map[String, TuningEntryDefinition] = {
    val yamlSource =
      UTF8Source.fromResource("bootstrap/tuningTable.yaml").mkString
    val representer = new Representer(new DumperOptions())
    representer.getPropertyUtils.setSkipMissingProperties(true)
    val constructor = new Constructor(classOf[TuningEntries], new LoaderOptions())
    val yamlObjNested = new Yaml(constructor, representer)
    val entryTable: TuningEntries = yamlObjNested.load(yamlSource).asInstanceOf[TuningEntries]
    // load the enabled entries.
    entryTable.tuningDefinitions.asScala.collect {
      case e if e.isEnabled() => (e.label, e)
    }(breakOut)
  }
}
