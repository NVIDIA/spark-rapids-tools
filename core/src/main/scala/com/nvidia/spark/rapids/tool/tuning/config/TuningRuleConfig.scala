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
 * Represents a configuration for a single tuning rule loaded from YAML.
 *
 * A tuning rule defines a specific optimization or analysis strategy that can be applied
 * to Spark applications running on GPU. Each rule is associated with a plugin and includes
 * metadata such as its name, description, implementation class, priority, and enabled status.
 *
 * @param name        The unique name identifying the tuning rule
 * @param description A human-readable description of the rule's purpose and logic
 * @param className   The fully qualified class name implementing the rule
 * @param priority    The execution priority of the rule (0 = highest, 100 = lowest)
 * @param enabled     Whether this rule is currently active and should be applied
 * @param pluginName  The name of the plugin this rule belongs to
 */
class TuningRuleConfig(
    @BeanProperty var name: String,
    @BeanProperty var description: String,
    @BeanProperty var className: String,
    @BeanProperty var priority: Int,
    @BeanProperty var enabled: Boolean,
    @BeanProperty var pluginName: String
) extends MergeableConfigTrait[TuningRuleConfig] {

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
   * No-arg constructor required for YAML deserialization.
   * Initializes all fields with default values.
   */
  def this() = this("", "", "", TuningPluginsConfig.UNDEFINED_PRIORITY, true, "")

  override def configKey: String = name

  /**
   * Merges this TuningRuleConfig with another one. The other rule's values will override
   * this rule's values if they are not empty.
   *
   * @param other The other TuningRuleConfig to merge with (keys already verified to match)
   * @return A new TuningRuleConfig with merged values
   */
  override protected def mergeWith(other: TuningRuleConfig): TuningRuleConfig = {
    new TuningRuleConfig(
      name = this.name,
      description = if (isEmptyValue(other.description)) this.description else other.description,
      className = if (isEmptyValue(other.className)) this.className else other.className,
      priority =
        if (other.priority != TuningPluginsConfig.UNDEFINED_PRIORITY) other.priority else priority,
      enabled = other.enabled,
      pluginName = if (isEmptyValue(other.pluginName)) this.pluginName else other.pluginName
    )
  }

  /**
   * Creates a copy of this TuningRuleConfig with all the same field values.
   *
   * @return A new TuningRuleConfig instance with copied values
   */
  override protected def copyConfig(): TuningRuleConfig = {
    new TuningRuleConfig(
      name = this.name,
      description = this.description,
      className = this.className,
      priority = this.priority,
      enabled = this.enabled,
      pluginName = this.pluginName
    )
  }

  /**
   * Returns a string representation of this tuning rule configuration.
   *
   * @return A formatted string containing all configuration properties
   */
  override def toString: String = {
    s"TuningRuleConfig(name=$name, description=$description, className=$className, " +
      s"enabled=$enabled, priority=$normalizedPriority, pluginName=$pluginName)"
  }

  /**
   * Helper method to check if this configuration is considered empty.
   * subclasses must override.
   *
   * @return True if the configuration is empty, false otherwise
   */
  override def isEmpty: Boolean = {
    isEmptyValue(getName)
  }
}
