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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.util.ValidatableProperties

/**
 * Root configuration container for tuning plugins and rules loaded from YAML.
 *
 * This class serves as the top-level structure for loading and managing the complete tuning
 * framework configuration. It contains both plugin definitions and their associated rules,
 * providing validation and convenient access methods for working with the configurations.
 *
 * The configuration is typically loaded from a YAML file (e.g., `tuningConfigs.yaml`) and
 * provides Java Bean properties for proper YAML deserialization. After loading, the `validate()`
 * method should be called to ensure all required fields are properly set.
 *
 * @param tuningPlugins A list of tuning plugin configurations that define the available plugins
 * @param tuningRules A list of tuning rule configurations that define optimization strategies
 */
class TuningPluginsConfig(
    @BeanProperty var tuningPlugins: util.List[TuningPluginConfigEntry],
    @BeanProperty var tuningRules: util.List[TuningRuleConfig])
  extends ValidatableProperties
  with MergeableConfigTrait[TuningPluginsConfig] {

  override def isEmpty: Boolean = {
    (tuningPlugins == null || tuningPlugins.isEmpty) &&
      (tuningRules == null || tuningRules.isEmpty)
  }
  /**
   * No-arg constructor required for YAML deserialization using JavaBeans conventions.
   * Initializes with empty lists for both plugins and rules.
   */
  def this() =
    this(
      tuningPlugins = new util.ArrayList[TuningPluginConfigEntry](),
      tuningRules = new util.ArrayList[TuningRuleConfig]()
    )

  /**
   * Returns a unique identifier for this configuration.
   * Since TuningPluginsConfiguration is a singleton-like configuration,
   * we use a constant key.
   */
  override def configKey: String = "tuningPluginsConfiguration"

  /**
   * Converts the Java list of tuning plugins to a Scala Seq for easier manipulation.
   *
   * This method provides a more idiomatic Scala interface for working with the plugin
   * configurations, supporting operations like map, filter, and foreach.
   *
   * @return A Scala sequence of tuning plugin configurations, or empty sequence if null
   */
  def getTuningPluginsSeq: Seq[TuningPluginConfigEntry] = {
    if (tuningPlugins != null) tuningPlugins.asScala.toSeq else Seq.empty
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
    if (tuningRules != null) tuningRules.asScala.toSeq else Seq.empty
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
   * Merges this TuningPluginsConfiguration with another one.
   * For plugins and rules with the same name, the other configuration's values will override.
   *
   * @param other The other TuningPluginsConfiguration to merge with
   * @return A new TuningPluginsConfiguration with merged values
   */
  override protected def mergeWith(
      other: TuningPluginsConfig): TuningPluginsConfig = {
    val mergedPlugins = mergePlugins(this.tuningPlugins, other.tuningPlugins)
    val mergedRules = mergeRules(this.tuningRules, other.tuningRules)
    new TuningPluginsConfig(mergedPlugins, mergedRules)
  }

  /**
   * Merges two lists of plugin configurations, preferring values from the override list
   * when plugin names match.
   *
   * @param basePlugins The base list of plugin configurations
   * @param overridePlugins The override list of plugin configurations
   * @return A merged list containing all plugins with overrides applied
   */
  private def mergePlugins(
    basePlugins: util.List[TuningPluginConfigEntry],
    overridePlugins: util.List[TuningPluginConfigEntry]): util.List[TuningPluginConfigEntry] = {
    mergeConfigLists[TuningPluginConfigEntry](
      basePlugins,
      overridePlugins,
      _.name,
      (baseEntry, overrideEntry) => baseEntry.merge(overrideEntry)
    )
  }

  /**
   * Merges two lists of rule configurations, preferring values from the override list
   * when rule names match.
   *
   * @param baseRules The base list of rule configurations
   * @param overrideRules The override list of rule configurations
   * @return A merged list containing all rules with overrides applied
   */
  private def mergeRules(
    baseRules: util.List[TuningRuleConfig],
    overrideRules: util.List[TuningRuleConfig]): util.List[TuningRuleConfig] = {
    mergeConfigLists[TuningRuleConfig](
      baseRules,
      overrideRules,
      _.name,
      (baseEntry, overrideEntry) => baseEntry.merge(overrideEntry)
    )
  }

  /**
   * Generic helper method to merge two lists of configuration objects.
   *
   * @param baseList The base list of configurations
   * @param overrideList The override list of configurations
   * @param keyExtractor Function to extract the unique key from each configuration
   * @param merger Function to merge two configurations with the same key
   * @tparam T The type of configuration object
   * @return A merged list containing all configurations with overrides applied
   */
  private def mergeConfigLists[T](
    baseList: util.List[T],
    overrideList: util.List[T],
    keyExtractor: T => String,
    merger: (T, T) => T): util.List[T] = {
    if (overrideList == null || overrideList.isEmpty) {
      return baseList
    }

    val result = new util.ArrayList[T]()
    val baseMap = baseList.asScala.map(item => keyExtractor(item) -> item).toMap
    val overrideMap = overrideList.asScala.map(item => keyExtractor(item) -> item).toMap

    // Add all base items, applying overrides where they exist
    baseList.asScala.foreach { baseItem =>
      overrideMap.get(keyExtractor(baseItem)) match {
        case Some(overrideItem) => result.add(merger(baseItem, overrideItem))
        case None => result.add(baseItem)
      }
    }

    // Add any new items from override that don't exist in base
    overrideList.asScala.foreach { overrideItem =>
      if (!baseMap.contains(keyExtractor(overrideItem))) {
        result.add(overrideItem)
      }
    }

    result
  }


  /**
   * Creates a deep copy of this TuningPluginsConfiguration.
   *
   * @return A new TuningPluginsConfiguration instance with copied values
   */
  override protected def copyConfig(): TuningPluginsConfig = {
    val pluginsCopy = new util.ArrayList[TuningPluginConfigEntry]()
    tuningPlugins.asScala.foreach(plugin => pluginsCopy.add(plugin.copy()))

    val rulesCopy = new util.ArrayList[TuningRuleConfig]()
    tuningRules.asScala.foreach(rule => rulesCopy.add(rule.copy()))

    new TuningPluginsConfig(pluginsCopy, rulesCopy)
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
    v >= TuningPluginsConfig.HIGHEST_PRIORITY &&
      v <= TuningPluginsConfig.LOWEST_PRIORITY
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
      require(!isEmptyValue(plugin.getName),
        "Plugin name must not be null or empty")
      require(validatePriority(plugin.normalizedPriority),
        s"Plugin priority must be between ${TuningPluginsConfig.HIGHEST_PRIORITY} and " +
          s"${TuningPluginsConfig.LOWEST_PRIORITY}")
    }
    if (tuningRules != null) {
      getTuningRulesSeq.foreach { rule =>
        require(!isEmptyValue(rule.getName),
          "Rule name must not be null or empty")
        require(!isEmptyValue(rule.getPluginName),
          "Rule's pluginName must not be null or empty")
        require(validatePriority(rule.normalizedPriority),
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
 * The default configuration is loaded from `bootstrap/tuningConfigs.yaml` in the resources
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

  val UNDEFINED_PRIORITY = 999
}
