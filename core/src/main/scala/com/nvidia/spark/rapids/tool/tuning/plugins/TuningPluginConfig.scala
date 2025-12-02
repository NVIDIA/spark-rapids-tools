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
package com.nvidia.spark.rapids.tool.tuning.plugins

import java.util

import scala.beans.BeanProperty
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.util.{PropertiesLoader, UTF8Source, ValidatableProperties}

/**
 * Represents a configuration for a single tuning rule loaded from YAML.
 *
 * A tuning rule defines a specific optimization or analysis strategy that can be applied
 * to Spark applications running on GPU. Each rule is associated with a plugin and includes
 * metadata such as its name, description, implementation class, priority, and enabled status.
 *
 * @param name The unique name identifying the tuning rule
 * @param description A human-readable description of the rule's purpose and logic
 * @param className The fully qualified class name implementing the rule
 * @param priority The execution priority of the rule (0 = highest, 100 = lowest)
 * @param enabled Whether this rule is currently active and should be applied
 * @param pluginName The name of the plugin this rule belongs to
 */
class TuningRuleConfig(
  @BeanProperty var name: String,
  @BeanProperty var description: String,
  @BeanProperty var className: String,
  @BeanProperty var priority: Int,
  @BeanProperty var enabled: Boolean,
  @BeanProperty var pluginName: String) {

  /**
   * No-arg constructor required for YAML deserialization.
   * Initializes all fields with default values.
   */
  def this() = this("", "", "", TuningPluginsConfig.DEFAULT_PRIORITY, true, "")

  /**
   * Returns a string representation of this tuning rule configuration.
   *
   * @return A formatted string containing all configuration properties
   */
  override def toString: String = {
    s"TuningRuleConfig(name=$name, description=$description, className=$className, " +
      s"enabled=$enabled, priority=$priority, pluginName=$pluginName)"
  }
}


/**
 * Represents a tuning plugin configuration loaded from YAML.
 *
 * A tuning plugin is a modular component that provides tuning capabilities for Spark applications
 * running on GPU. Each plugin encapsulates a set of related tuning rules and provides
 * infrastructure for analyzing and optimizing Spark configurations. Plugins can be independently
 * enabled/disabled and prioritized to control the order in which they are applied.
 *
 * @param name The unique identifier for the tuning plugin
 * @param description A human-readable description of the plugin's purpose and optimization
 *                    strategies
 * @param className The fully qualified class name of the implementation for this plugin
 * @param priority The execution priority of the plugin (0 = highest, 100 = lowest). Lower values
 *                 are executed first. Default is 50.
 * @param enabled Whether this plugin is currently active and should be used during tuning
 */
class TuningPluginConfig(
    @BeanProperty var name: String,
    @BeanProperty var description: String,
    @BeanProperty var className: String,
    @BeanProperty var priority: Int,
    @BeanProperty var enabled: Boolean) {

  /**
   * No-arg constructor required for YAML deserialization using JavaBeans conventions.
   * Initializes all fields with default values.
   */
  def this() = this("", "", "", TuningPluginsConfig.DEFAULT_PRIORITY, true)

  /**
   * Returns a string representation of this tuning plugin configuration.
   *
   * @return A formatted string containing all configuration properties
   */
  override def toString: String = {
    s"TuningPluginConfig(name=$name, description=$description, className=$className, " +
      s"priority=$priority, enabled=$enabled)"
  }
}

/**
 * Root configuration container for tuning plugins and rules loaded from YAML.
 *
 * This class serves as the top-level structure for loading and managing the complete tuning
 * framework configuration. It contains both plugin definitions and their associated rules,
 * providing validation and convenient access methods for working with the configurations.
 *
 * The configuration is typically loaded from a YAML file (e.g., `tuningPlugins.yaml`) and
 * provides Java Bean properties for proper YAML deserialization. After loading, the `validate()`
 * method should be called to ensure all required fields are properly set.
 *
 * @param tuningPlugins A list of tuning plugin configurations that define the available plugins
 * @param tuningRules A list of tuning rule configurations that define optimization strategies
 */
class TuningPluginsConfig(
    @BeanProperty var tuningPlugins: util.List[TuningPluginConfig],
    @BeanProperty var tuningRules: util.List[TuningRuleConfig]) extends ValidatableProperties {

  /**
   * No-arg constructor required for YAML deserialization using JavaBeans conventions.
   * Initializes with empty lists for both plugins and rules.
   */
  def this() =
    this(new util.ArrayList[TuningPluginConfig](), new util.ArrayList[TuningRuleConfig]())

  /**
   * Converts the Java list of tuning plugins to a Scala Seq for easier manipulation.
   *
   * This method provides a more idiomatic Scala interface for working with the plugin
   * configurations, supporting operations like map, filter, and foreach.
   *
   * @return A Scala sequence of tuning plugin configurations, or empty sequence if null
   */
  def getTuningPluginsSeq: Seq[TuningPluginConfig] = {
    if (tuningPlugins != null) tuningPlugins.asScala else Seq.empty
  }

  /**
   * Converts the Java list of tuning rules to a Scala Seq for easier manipulation.
   *
   * This method provides a more idiomatic Scala interface for working with the rule
   * configurations, supporting operations like map, filter, and foreach.
   *
   * @return A Scala sequence of tuning rule configurations, or empty sequence if null
   */
  def getTuningRulesSeq: Seq[TuningRuleConfig] = {
    if (tuningRules != null) tuningRules.asScala else Seq.empty
  }

  /**
   * Groups tuning rules by their associated plugin name.
   *
   * This method creates a mapping from plugin names to the list of rules that belong
   * to each plugin. This is useful for organizing and retrieving rules based on
   * their parent plugin.
   *
   * @return A map where keys are plugin names and values are sequences of tuning rule
   *         configurations associated with each plugin
   */
  def getPluginRulesMap: Map[String, Seq[TuningRuleConfig]] = {
    getTuningRulesSeq.groupBy(_.pluginName)
  }

  /**
   * Validates that a priority value is within the acceptable range.
   *
   * Priority values must be between HIGHEST_PRIORITY (0) and LOWEST_PRIORITY (100) inclusive.
   * This ensures consistent ordering of plugin and rule execution.
   *
   * @param v The priority value to validate
   * @return true if the priority is within the valid range [0, 100], false otherwise
   */
  private def validatePriority(v: Int): Boolean = {
    v >= TuningPluginsConfig.HIGHEST_PRIORITY && v <= TuningPluginsConfig.LOWEST_PRIORITY
  }

  /**
   * Validates the configuration to ensure all required fields are properly set and
   * constraints are met.
   *
   * This method performs the following validations:
   * - The tuningPlugins list must not be null
   * - All plugins must have non-null, non-empty names
   * - All rules (if present) must have non-null, non-empty names and plugin names
   * - All rule priorities must be within the valid range [0, 100]
   *
   * Note: Plugin className can be empty if the user wants to disable a plugin.
   *
   */
  @throws[java.lang.IllegalArgumentException]
  override def validate(): Unit = {
    require(tuningPlugins != null, "tuningPlugins must not be null")
    getTuningPluginsSeq.foreach { plugin =>
      // It is possible to have an empty className if the user wants to disable a plugin
      require(plugin.getName != null && plugin.getName.nonEmpty,
        "Plugin name must not be null or empty")
    }
    if (tuningRules != null) {
      getTuningRulesSeq.foreach { rule =>
        require(rule.getName != null && rule.getName.nonEmpty,
          "Rule name must not be null or empty")
        require(rule.getPluginName != null && rule.getPluginName.nonEmpty,
          "Rule's pluginName must not be null or empty")
        require(validatePriority(rule.getPriority),
          s"Rule priority must be between ${TuningPluginsConfig.HIGHEST_PRIORITY} and " +
            s"${TuningPluginsConfig.LOWEST_PRIORITY}")
      }
    }
  }
}

/**
 * Companion object for TuningPluginsConfig providing configuration loading utilities and constants.
 *
 * This object defines the priority range for tuning plugins and rules, provides a mechanism
 * to load the default configuration from a bundled YAML resource file, and includes a main
 * method for testing purposes.
 *
 * Priority scale:
 * - 0 (HIGHEST_PRIORITY) = executed first, highest precedence
 * - 50 (DEFAULT_PRIORITY) = default for most plugins/rules
 * - 100 (LOWEST_PRIORITY) = executed last, lowest precedence
 *
 * The default configuration is loaded from `bootstrap/tuningPlugins.yaml` in the resources
 * directory and provides a baseline set of tuning plugins and rules.
 */
object TuningPluginsConfig extends Logging {

  /**
   * The default priority assigned to plugins and rules when not explicitly specified.
   * This value provides a balanced middle ground for most tuning operations.
   */
  val DEFAULT_PRIORITY = 50

  /**
   * The highest priority value (executed first).
   * Plugins and rules with this priority are applied before all others.
   */
  private val HIGHEST_PRIORITY = 0

  /**
   * The lowest priority value (executed last).
   * Plugins and rules with this priority are applied after all others.
   */
  private val LOWEST_PRIORITY = 100

  /**
   * Configuration loader for YAML deserialization.
   * Uses the PropertiesLoader to parse YAML content into TuningPluginsConfig instances.
   */
  private val CONFIG_LOADER = PropertiesLoader[TuningPluginsConfig]

  /**
   * Path to the default tuning plugins configuration YAML file in the resources directory.
   * This file contains the baseline configuration bundled with the application.
   */
  private val DEFAULT_CONFIG_PATH = "bootstrap/tuningPlugins.yaml"

  /**
   * The default tuning plugins configuration loaded from the bundled YAML resource.
   *
   * This configuration is loaded eagerly during object initialization and provides the
   * baseline set of tuning plugins and rules. All plugins and rules are validated upon
   * loading to ensure they meet the required constraints.
   * No filters should be applied at this stage because the user can still override the plugins.
   *
   * Note: throws java.lang.RuntimeException if the default configuration file is missing, has
   *       invalid format, or fails validation
   */
  @throws[java.lang.RuntimeException]
  val DEFAULT_TUNING_PLUGINS_CONFIG: TuningPluginsConfig = {
    CONFIG_LOADER.loadFromContent(UTF8Source.fromResource(DEFAULT_CONFIG_PATH).mkString) match {
      case Some(config) => config
      case _ => throw new RuntimeException(
        "Failed to load default value for tuning config: " +
          "The file is missing or has invalid format.")
    }
  }
}
