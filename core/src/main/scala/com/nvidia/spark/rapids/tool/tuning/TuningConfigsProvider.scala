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
import scala.jdk.CollectionConverters._

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.rapids.tool.util.{StringUtils, ValidatableProperties}

/**
 * Represents a tuning configuration entry with the following properties:
 *   - name:        The configuration parameter name/key.
 *   - description: Human-readable description of the configuration parameter.
 *   - usedBy:      Comma-separated list of Spark configurations this constant affects.
 *   - pluginRules: Comma-separated list of plugin rules that use this configuration (optional).
 *   - default:     The default value to use (optional).
 *   - min:         The minimum allowed value (optional).
 *   - max:         The maximum allowed value (optional).
 */
class TuningConfigEntry(
  @BeanProperty var name: String = "",
  @BeanProperty var description: String = "",
  @BeanProperty var usedBy: String = "",
  @BeanProperty var pluginRules: String = "",
  @BeanProperty var default: String = "",
  @BeanProperty var min: String = "",
  @BeanProperty var max: String = "") extends ValidatableProperties {

  def this() = this("", "", "", "", "", "", "")

  private def isEmptyValue(value: String): Boolean = {
    value == null || value.isEmpty
  }

  private def isEmpty: Boolean = {
    isEmptyValue(name) && isEmptyValue(description) && isEmptyValue(usedBy) &&
      isEmptyValue(default) && isEmptyValue(min) && isEmptyValue(max)
  }

  // Helper methods for memory unit conversions
  // These methods return the memory values converted to the specified ByteUnit
  // as Long values.
  def getDefaultAsMemory(unit: ByteUnit): Long = {
    getMemoryValueInUnit(default, unit)
  }

  def getMinAsMemory(unit: ByteUnit): Long = {
    getMemoryValueInUnit(min, unit)
  }

  def getMaxAsMemory(unit: ByteUnit): Long = {
    getMemoryValueInUnit(max, unit)
  }

  /** Helper method to convert memory string to target unit */
  private def getMemoryValueInUnit(value: String, targetUnit: ByteUnit): Long = {
    val valueInBytes = StringUtils.convertMemorySizeToBytes(value, Some(ByteUnit.BYTE))
    ByteUnit.BYTE.convertTo(valueInBytes, targetUnit)
  }

  /**
   * Merge this TuningConfigEntry with another one. The other entry's values will override
   * this entry's values if they are not empty.
   * Note:
   * - Name must be the same for both entries, otherwise an exception is thrown.
   * - Description and usedBy are not merged, the original values are kept.
   *
   * @param other The other TuningConfigEntry to merge with
   * @return A new TuningConfigEntry with merged values
   */
  def merge(other: TuningConfigEntry): TuningConfigEntry = {
    require(this.name == other.name, s"Cannot merge configs with different names: '" +
      s"${this.name}' vs '${other.name}'")

    new TuningConfigEntry(
      name = this.name,
      description = this.description,
      usedBy = this.usedBy,
      pluginRules = if (isEmptyValue(other.pluginRules)) this.pluginRules else other.pluginRules,
      default = if (isEmptyValue(other.default)) this.default else other.default,
      min = if (isEmptyValue(other.min)) this.min else other.min,
      max = if (isEmptyValue(other.max)) this.max else other.max
    )
  }

  override def toString: String = {
    s"TuningConfigEntry(name='$name', description='$description', usedBy='$usedBy'" +
      s"pluginRules='$pluginRules', default='$default', min='$min', max='$max')"
  }

  override def validate(): Unit = {
    // Skip validation if the entry is completely empty (i.e. when loaded from YAML with no values)
    if (!isEmpty) {
      // Validate the name is defined
      if (isEmptyValue(name)) {
        throw new IllegalArgumentException(s"Name must be defined for config '$name'")
      }
      // Validate at least one of the default, min, max is defined
      if (isEmptyValue(default) && isEmptyValue(min) && isEmptyValue(max)) {
        throw new IllegalArgumentException(s"At least one of the default, min, max " +
          s"must be defined for config '$name'")
      }
    }
  }
}

object TuningConfigEntry {
  def apply(
      name: String,
      description: String = "",
      usedBy: String = "",
      pluginRules: String = "",
      default: String = "",
      min: String = "",
      max: String = ""): TuningConfigEntry = {
    new TuningConfigEntry(name, description, usedBy, pluginRules, default, min, max)
  }
}

/**
 * Represents the tuning configurations loaded from the tuning table YAML file.
 * This class provides dynamic access to configs with description, default, min, and max values.
 *
 * Example usage:
 * {{{
 *   val tuningConfigs = TuningConfigsProvider(defaultConfigs)
 *   tuningConfigs.withAutoTuner(QualificationAutoTuner)
 *
 *   // Get configuration values
 *   val concurrentGpuTasks = tuningConfigs.get("CONC_GPU_TASKS").getDefault.toInt
 *   val heapPerCore = tuningConfigs.get("HEAP_PER_CORE").getDefaultAsMemory(ByteUnit.MiB)
 * }}}
 */
class TuningConfigsProvider (
    @BeanProperty var default: util.List[TuningConfigEntry] =
      new util.ArrayList[TuningConfigEntry](),
    @BeanProperty var qualification: util.List[TuningConfigEntry] =
      new util.ArrayList[TuningConfigEntry](),
    @BeanProperty var profiling: util.List[TuningConfigEntry] =
      new util.ArrayList[TuningConfigEntry](),
    val selectedAutoTuner: Option[AutoTuner] = None) extends ValidatableProperties {

  def this() = this(
    new util.ArrayList[TuningConfigEntry](),
    new util.ArrayList[TuningConfigEntry](),
    new util.ArrayList[TuningConfigEntry](),
    None)

  /** Tool-specific overrides for qualification/profiling */
  private lazy val toolOverrides: util.List[TuningConfigEntry] = selectedAutoTuner match {
    case Some(_: QualificationAutoTuner) => qualification
    case Some(_: ProfilingAutoTuner) => profiling
    case _ => throw new IllegalArgumentException(
      "Tool must be specified as either QualificationAutoTuner or ProfilingAutoTuner")
  }

  /** Cached lookup map, rebuilt from default and toolOverrides */
  @transient
  private lazy val tuningConfigsMap: Map[String, TuningConfigEntry] = {
    val mergedConfigs = mergeConfigs(default, toolOverrides)
    mergedConfigs.asScala.map(e => e.name -> e).toMap
  }

  /**
   * Merges entries from the override list into the base list.
   *
   * @param baseList The base list of tuning configurations.
   * @param overrideList The override list of tuning configurations.
   * @param allowExtra Whether to allow extra entries in the override list.
   * @return A new list containing the merged entries.
   */
  private def mergeConfigs(
      baseList: util.List[TuningConfigEntry],
      overrideList: util.List[TuningConfigEntry],
      allowExtra: Boolean = false)
  : util.List[TuningConfigEntry] = {
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

  /**
   * Returns a new TuningConfigsProvider instance with the specified tool's overrides applied.
   */
  def withAutoTuner(maybeTuner: Option[AutoTuner]): TuningConfigsProvider = {
    new TuningConfigsProvider(default, qualification, profiling, maybeTuner)
  }

  /**
   * Get a config entry by name.
   */
  def getEntry(key: String): TuningConfigEntry = {
    tuningConfigsMap(key)
  }

  /**
   * Merge another TuningConfigsProvider instance into this one.
   * Returns a new instance with merged configurations.
   */
  def merge(other: TuningConfigsProvider): TuningConfigsProvider = {
    new TuningConfigsProvider(
      mergeConfigs(this.default, other.default),
      // Extra entries are allowed while merging qualification/profiling configs
      // as the base default list may not contain all tool-specific entries.
      mergeConfigs(this.qualification, other.qualification, allowExtra = true),
      mergeConfigs(this.profiling, other.profiling, allowExtra = true),
      this.selectedAutoTuner
    )
  }

  override def validate(): Unit = {
    default.asScala.foreach(_.validate())
    qualification.asScala.foreach(_.validate())
    profiling.asScala.foreach(_.validate())
  }
}

object TuningConfigsProvider {
  val DEFAULT_CONFIGS_FILE = "bootstrap/tuningConfigs.yaml"
}
