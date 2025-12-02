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

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.plugins.ConditionTrait
import com.nvidia.spark.rapids.tool.tuning.AutoTuner

/**
 * Base class for tuning plugins that apply optimization rules to the AutoTuner.
 *
 * A tuning plugin is a collection of rules that can be conditionally activated
 * based on specific criteria. When activated, the plugin fires its rules in
 * priority order to optimize Spark application configurations.
 *
 * @param tunerInst The AutoTuner instance this plugin will operate on
 */
abstract class BaseTuningPlugin(tunerInst: AutoTuner) extends TuningPluginTrait {
  /** Optional configuration for this plugin */
  var pluginConfig: Option[TuningPluginConfig] = None

  /** Whether this plugin has been activated */
  var activated = false

  /**
   * Defines the condition that must be satisfied for this plugin to activate.
   * Subclasses must implement this to specify when the plugin should run.
   */
  override val activationCond: ConditionTrait[AutoTuner]

  /**
   * Executes all rules in this plugin, ordered by priority (0 = highest, 100 = lowest).
   * Each rule is triggered sequentially on the tuner instance.
   * The cons of using this method that it is not convenient when coordination is needed across
   * plugins. For example, if order between rules across plugins is required, or when preventing a
   * single rule from running twice.
   */
  override def fire(): Unit = {
    rules.sortBy(_.priority).foreach { rule =>
      rule.trigger(tunerInst)
    }
  }

  /**
   * Returns the name of this plugin.
   *
   * @return The plugin name from configuration if available, otherwise the simple class name
   */
  def getName: String = {
    pluginConfig match {
      case Some(c) => c.getName
      case _ => this.getClass.getSimpleName
    }
  }

  /**
   * Returns the priority of this plugin.
   *
   * @return The priority from configuration if available, otherwise the default priority
   */
  def getPriority: Int = {
    pluginConfig match {
      case Some(c) => c.getPriority
      case _ => TuningPluginsConfig.DEFAULT_PRIORITY
    }
  }

  /**
   * Evaluates the activation condition and updates the activation state.
   *
   * @return true if the plugin should be activated, false otherwise
   */
  def activate(): Boolean = {
    activated = activationCond.eval(tunerInst)
    activated
  }

  /**
   * Sets the configuration for this plugin.
   *
   * @param config The plugin configuration to apply
   */
  def setPluginConfig(config: TuningPluginConfig): Unit = {
    this.pluginConfig = Option(config)
  }
}


object BaseTuningPlugin {
  /**
   * Creates a new Builder instance for constructing BaseTuningPlugin instances.
   *
   * @return A new Builder
   */
  def getBuilder: Builder = {
    new Builder()
  }

  /**
   * Builder class for creating BaseTuningPlugin instances.
   * Provides a fluent API for configuring plugins with tuner instances,
   * plugin configurations, and associated rules.
   */
  class Builder {
    /** The AutoTuner instance the plugin will operate on */
    protected var tunerInst: AutoTuner = _

    /** Configuration for the plugin being built */
    protected var pluginConfig: TuningPluginConfig = _

    /** Collection of rule configurations to be loaded into the plugin */
    protected val rulesConfig: ArrayBuffer[TuningRuleConfig] = ArrayBuffer()

    /**
     * Sets the plugin configuration.
     *
     * @param config The plugin configuration to use
     * @return This builder for chaining
     */
    def withPluginConfig(config: TuningPluginConfig): Builder = {
      this.pluginConfig = config
      this
    }

    /**
     * Instantiates a plugin from its configuration using reflection.
     * Loads all configured rules into the plugin instance.
     *
     * @param pluginConfig The configuration specifying the plugin class and settings
     * @return A Try containing the instantiated plugin or an exception
     * Note: throws java.lang.IllegalArgumentException if plugin instantiation fails
     */
    @throws[java.lang.IllegalArgumentException]
    def instantiatePlugin(
        pluginConfig: TuningPluginConfig): Try[BaseTuningPlugin] = Try {
      val className = pluginConfig.getClassName
      val clazz = Class.forName(className)
      val plugin = try {
        clazz.getDeclaredConstructor(classOf[AutoTuner])
          .newInstance(tunerInst)
          .asInstanceOf[BaseTuningPlugin]
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(
            s"Failed to instantiate Plugin for rule $pluginConfig: ${e.getMessage}", e)
      }
      // Load rules for this plugin
      plugin match {
        case defaultPlugin: BaseTuningPlugin =>
          defaultPlugin.setPluginConfig(pluginConfig)
          val loadedRules = rulesConfig.flatMap { ruleConfig =>
            BaseTuningRule.builder()
              .withTuningRuleConfig(ruleConfig)
              .build()
          }
          defaultPlugin.rules ++= loadedRules
        case _ =>
          // do Nothing for now.
      }
      plugin
    }

    /**
     * Sets the AutoTuner instance the plugin will operate on.
     *
     * @param tuner The AutoTuner instance
     * @return This builder for chaining
     */
    def withTuner(tuner: AutoTuner): Builder = {
      this.tunerInst = tuner
      this
    }

    /**
     * Adds multiple rule configurations to this plugin.
     *
     * @param rulesConfigs The sequence of rule configurations to add
     * @return This builder for chaining
     */
    def addRules(rulesConfigs: Seq[TuningRuleConfig]): Builder = {
      this.rulesConfig ++= rulesConfigs
      this
    }

    /**
     * Builds and returns the configured plugin instance.
     *
     * @return A Try containing the built plugin or an exception
     * Note: throws java.lang.IllegalStateException if plugin config, tuner instance, or rules are
     *       not set.
     */
    @throws[java.lang.IllegalStateException]
    def build(): Try[BaseTuningPlugin] = {
      if (pluginConfig == null) {
        throw new IllegalStateException("Plugin config must be set before building")
      }

      if (tunerInst == null) {
        throw new IllegalStateException("Tuner instance must be set before building")
      }

      if (rulesConfig.isEmpty) {
        throw new IllegalStateException("At least one rule config must be added before building")
      }

      instantiatePlugin(pluginConfig)
    }
  }
}
