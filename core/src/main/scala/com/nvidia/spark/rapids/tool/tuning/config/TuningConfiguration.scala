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

package com.nvidia.spark.rapids.tool.tuning.config

import java.util

import scala.beans.BeanProperty
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.rapids.tool.util.{PropertiesLoader, UTF8Source, ValidatableProperties}

/**
 * Represents the tuning configurations loaded from the tuning table YAML file.
 * This class provides dynamic access to configs with description, default, min, and max values.
 */
class TuningConfiguration(
    @BeanProperty var default: util.List[TuningConfigEntry] =
      new util.ArrayList[TuningConfigEntry](),
    @BeanProperty var qualification: util.List[TuningConfigEntry] =
      new util.ArrayList[TuningConfigEntry](),
    @BeanProperty var profiling: util.List[TuningConfigEntry] =
      new util.ArrayList[TuningConfigEntry](),
    @BeanProperty var pluginConfigs: TuningPluginsConfig = new TuningPluginsConfig()
) extends ValidatableProperties with MergeableConfigTrait[TuningConfiguration] {

  override def configKey: String = "tuningConfiguration"

  override def validate(): Unit = {
    default.asScala.foreach(_.validate())
    qualification.asScala.foreach(_.validate())
    profiling.asScala.foreach(_.validate())
    pluginConfigs.validate()
  }

  def this() = this(
    default = new util.ArrayList[TuningConfigEntry](),
    qualification = new util.ArrayList[TuningConfigEntry](),
    profiling = new util.ArrayList[TuningConfigEntry](),
    pluginConfigs = new TuningPluginsConfig())

  /**
   * Creates a deep copy of this TuningConfiguration.
   *
   * This method creates new ArrayList instances and copies each TuningConfigEntry,
   * ensuring that modifications to the copy do not affect the original.
   *
   * @return A new TuningConfiguration instance with deep copies of all configuration lists
   */
  override protected def copyConfig(): TuningConfiguration = {
    new TuningConfiguration(
      default = deepCopyList(default),
      qualification = deepCopyList(qualification),
      profiling = deepCopyList(profiling),
      pluginConfigs = pluginConfigs.copy()
    )
  }

  /**
   * Creates a deep copy of a list of TuningConfigEntry objects.
   *
   * @param list The list to copy
   * @return A new ArrayList with copied entries
   */
  private def deepCopyList(
      list: util.List[TuningConfigEntry]): util.ArrayList[TuningConfigEntry] = {
    val copy = new util.ArrayList[TuningConfigEntry]()
    list.asScala.foreach(entry => copy.add(entry.copy()))
    copy
  }

  /**
   * Merges this TuningConfiguration with another one.
   * The other configuration's values will override this configuration's values.
   * Extra entries are allowed in qualification/profiling lists.
   *
   * @param other The other TuningConfiguration to merge with
   * @return A new TuningConfiguration with merged values
   */
  override protected def mergeWith(other: TuningConfiguration): TuningConfiguration = {
    new TuningConfiguration(
      TuningConfiguration.mergeConfigs(this.default, other.default),
      // Extra entries are allowed while merging qualification/profiling configs
      // as the base default list may not contain all tool-specific entries.
      TuningConfiguration.mergeConfigs(this.qualification, other.qualification, allowExtra = true),
      TuningConfiguration.mergeConfigs(this.profiling, other.profiling, allowExtra = true),
      this.pluginConfigs.merge(other.pluginConfigs)
    )
  }

  /**
   * Helper method to check if this configuration is considered empty.
   * subclasses must override.
   *
   * @return True if the configuration is empty, false otherwise
   */
  override def isEmpty: Boolean = {
    default.isEmpty && qualification.isEmpty && profiling.isEmpty && pluginConfigs.isEmpty
  }
}

/**
 * Companion object for TuningConfiguration providing factory methods and utility functions.
 *
 * This object is responsible for:
 * - Loading default tuning configurations from the bootstrap YAML file
 * - Providing a factory method to obtain a copy of the default configurations
 * - Merging configuration lists with support for tool-specific overrides
 *
 * The default configurations are loaded lazily from the classpath resource file
 * "bootstrap/tuningConfigs.yaml" and cached for efficient reuse. Each call to
 * `getDefaultConfigs` returns a deep copy to prevent accidental modification of
 * the shared default instance.
 */
object TuningConfiguration {
  /**
   * Path to the default tuning configurations YAML file in the classpath.
   * This file is located in the resources/bootstrap directory and contains
   * the base configurations used by all tools.
   */
  private val DEFAULT_CONFIGS_FILE = "bootstrap/tuningConfigs.yaml"

  /**
   * Lazily loaded default tuning configurations.
   *
   * This value is loaded once from the bootstrap YAML file and cached for the
   * lifetime of the application. It serves as the base configuration that is
   * merged with user-provided overrides.
   *
   * @throws RuntimeException if the configuration file is missing or has invalid format
   */
  private lazy val DEFAULT_CONFIGS: TuningConfiguration = {
    PropertiesLoader[TuningConfiguration].loadFromContent(
      UTF8Source.fromResource(DEFAULT_CONFIGS_FILE).mkString
    ).getOrElse {
      throw new RuntimeException(
        "Failed to load default value for tuning config: " +
          "The file is missing or has invalid format.")
    }
  }

  /**
   * Returns a deep copy of the default tuning configurations.
   *
   * This method provides a safe way to obtain the default configurations without
   * risking modification of the shared default instance. Each call returns a new
   * copy with all configuration lists deeply copied.
   *
   * @return A new TuningConfiguration instance with copied values from the defaults
   */
  def getDefaultConfigs: TuningConfiguration = {
    DEFAULT_CONFIGS.copy()
  }

  /**
   * Merges entries from an override list into a base list of configuration entries.
   *
   * This method implements the merge logic used to combine default configurations
   * with tool-specific or user-provided overrides. The merge strategy is:
   * 1. For entries that exist in both lists (matched by name), merge them using
   *    TuningConfigEntry.merge(), with override values taking precedence
   * 2. For entries that only exist in the base list, keep them unchanged
   * 3. For entries that only exist in the override list:
   *    - If allowExtra is true, add them to the result
   *    - If allowExtra is false, throw an IllegalArgumentException
   *
   * The allowExtra parameter is useful for distinguishing between:
   * - Merging tool-specific lists (qualification/profiling) where extra entries
   *   may be valid tool-specific configurations (allowExtra = true)
   * - Merging into the default list where extra entries likely indicate typos
   *   or configuration errors (allowExtra = false)
   *
   * @param baseList The base list of tuning configurations (e.g., defaults)
   * @param overrideList The override list of tuning configurations (e.g., user-provided)
   * @param allowExtra Whether to allow entries in overrideList that don't exist in baseList.
   *                   Default is false for strict validation.
   * @return A new list containing the merged entries, preserving the order of the base list
   *         with overrides applied and any extra entries (if allowed) appended at the end
   * throws java.lang.IllegalArgumentException if allowExtra is false and overrideList contains
   *                                 entries not present in baseList
   *
   * Example:
   * {{{
   *   val base = List(ConfigEntry("A", default="1"), ConfigEntry("B", default="2"))
   *   val override = List(ConfigEntry("A", default="10"), ConfigEntry("C", default="3"))
   *
   *   // With allowExtra = false (strict validation)
   *   mergeConfigs(base, override, allowExtra = false) // throws IllegalArgumentException
   *
   *   // With allowExtra = true (permissive)
   *   mergeConfigs(base, override, allowExtra = true)
   *   // Returns: List(ConfigEntry("A", default="10"), ConfigEntry("B", default="2"),
   *   //               ConfigEntry("C", default="3"))
   * }}}
   */
  @throws[IllegalArgumentException]
  def mergeConfigs(
      baseList: util.List[TuningConfigEntry],
      overrideList: util.List[TuningConfigEntry],
      allowExtra: Boolean = false
  ): util.List[TuningConfigEntry] = {

    if (overrideList == null || overrideList.isEmpty) {
      return baseList
    }

    val result = new util.ArrayList[TuningConfigEntry](baseList)
    val baseMap = baseList.asScala.map(e => e.name -> e).toMap

    overrideList.asScala.foreach { overrideEntry =>
      baseMap.get(overrideEntry.name) match {
        case Some(baseEntry) =>
          // Entry exists in base list, merge and update
          val index = result.indexOf(baseEntry)
          result.set(index, baseEntry.merge(overrideEntry))
        case None =>
          // Entry does not exist in base list
          if (allowExtra) {
            // If extra entries are allowed (e.g. merging user defined qualification overrides with
            // qualification defaults), add the new entry.
            result.add(overrideEntry)
          } else {
            // Else, throw an error to indicate the override entry is invalid (e.g. merging
            // into the default list).
            throw new IllegalArgumentException(s"Override entry '${overrideEntry.name}' " +
              s"does not exist in the base tuning configs. Please check for typos.")
          }
      }
    }
    result
  }
}
