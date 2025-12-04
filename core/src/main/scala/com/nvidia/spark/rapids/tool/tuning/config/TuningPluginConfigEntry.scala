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

import scala.beans.BeanProperty

/**
 * Represents a tuning plugin configuration loaded from YAML.
 *
 * A tuning plugin is a modular component that provides tuning capabilities for Spark applications
 * running on GPU. Each plugin encapsulates a set of related tuning rules and provides
 * infrastructure for analyzing and optimizing Spark configurations. Plugins can be independently
 * enabled/disabled and prioritized to control the order in which they are applied.
 *
 * @param name        The unique identifier for the tuning plugin
 * @param description A human-readable description of the plugin's purpose and optimization
 *                    strategies
 * @param className   The fully qualified class name of the implementation for this plugin
 * @param priority    The execution priority of the plugin (0 = highest, 100 = lowest). Lower values
 *                    are executed first. Default is 50.
 * @param enabled     Whether this plugin is currently active and should be used during tuning
 */
class TuningPluginConfigEntry(
    @BeanProperty var name: String,
    @BeanProperty var description: String,
    @BeanProperty var className: String,
    @BeanProperty var priority: Int,
    @BeanProperty var enabled: Boolean
) extends MergeableConfigTrait[TuningPluginConfigEntry] {

  override def configKey: String = name

  /**
   * No-arg constructor required for YAML deserialization using JavaBeans conventions.
   * Initializes all fields with default values.
   */
  def this() = this("", "", "", TuningPluginsConfig.UNDEFINED_PRIORITY, true)

  /**
   * We need to normalize priority here because we lose information if the priority is initialized
   * to default value. In that case we won't be able to identify if the user explicitly set it or
   * not.
   * @return The normalized priority value in the range LOWEST-HIGHEST
   */
  def normalizedPriority: Int = {
    priority match {
      case p if p == TuningPluginsConfig.UNDEFINED_PRIORITY => TuningPluginsConfig.DEFAULT_PRIORITY
      case p => p
    }
  }

  /**
   * Returns a string representation of this tuning plugin configuration.
   *
   * @return A formatted string containing all configuration properties
   */
  override def toString: String = {
    s"TuningPluginConfig(name=$name, description=$description, className=$className, " +
      s"priority=$normalizedPriority, enabled=$enabled)"
  }

  override protected def mergeWith(other: TuningPluginConfigEntry): TuningPluginConfigEntry = {
    new TuningPluginConfigEntry(
      name = name,
      description = if (!isEmptyValue(other.description)) other.description else description,
      className = if (!isEmptyValue(other.className)) other.className else className,
      priority =
        if (other.priority != TuningPluginsConfig.UNDEFINED_PRIORITY) other.priority else priority,
      enabled = other.enabled
    )
  }

  override protected def copyConfig(): TuningPluginConfigEntry = {
    new TuningPluginConfigEntry(name, description, className, priority, enabled)
  }

  /**
   * Helper method to check if this configuration is considered empty.
   * subclasses must override.
   *
   * @return True if the configuration is empty, false otherwise
   */
  override def isEmpty: Boolean = {
    // if name is empty, consider the whole config empty.
    isEmptyValue(getName)
  }
}
