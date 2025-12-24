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

package com.nvidia.spark.rapids.tool.plugins

import scala.util.Try

import org.apache.spark.internal.Logging

/**
 * Abstract base class for application property plugins.
 * This class provides a common structure for plugins that handle application properties.
 *
 * @param pluginConfig The configuration for the application property plugin.
 */
abstract class BaseAppPropPlug(
    val pluginConfig: AppPropPlugConfig
) extends AppPropPlugTrait {
  val id: String = pluginConfig.plugId
  override val hasJobLevelConfigs: Boolean = pluginConfig.hasJobLevelConfigs

  /**
   * Mutable metadata container that child classes can populate with custom key-value pairs.
   * This allows plugins to store additional information such as versions, configurations,
   * or feature flags discovered during property evaluation.
   */
  protected val pluginMetadata: scala.collection.mutable.Map[String, Any] =
    scala.collection.mutable.Map.empty[String, Any]

  /**
   * Returns an immutable view of the plugin metadata.
   * @return Immutable map containing all metadata key-value pairs.
   */
  def getMetadata: Map[String, Any] = pluginMetadata.toMap

  /**
   * Helper method to add a metadata entry.
   * @param key The metadata key.
   * @param value The metadata value.
   */
  protected def addMetadata(key: String, value: Any): Unit = {
    pluginMetadata(key) = value
  }

  /**
   * Helper method to add multiple metadata entries at once.
   * @param entries Map of key-value pairs to add to metadata.
   */
  protected def addMetadata(entries: Map[String, Any]): Unit = {
    pluginMetadata ++= entries
  }
}

/**
 * Configuration case class for application property plugins.
 *
 * @param plugId The unique identifier for the plugin.
 * @param enabled Flag indicating whether the plugin is enabled.
 * @param additionalConfigs Map of additional configuration key-value pairs.
 */
case class AppPropPlugConfig(
    plugId: String,
    enabled: Boolean,
    additionalConfigs: Map[String, String]) {
  def getClassName: String = additionalConfigs.getOrElse("className", "")
  def hasJobLevelConfigs: Boolean = {
    additionalConfigs.get("hasJobLevelConfigs")
      .exists(_.equalsIgnoreCase("true"))
  }
}

/**
 * Companion object for BaseAppPropPlug.
 * Contains constants for known plugin IDs.
 */
object BaseAppPropPlug extends Logging {
  // Define known plugin IDs as constants
  val AURON_PLUG_ID: String = "auron"
  val DELTA_OSS_PLUG_ID: String = "delta-oss"
  val HIVE_PLUG_ID: String = "hive"
  val ICEBERG_PLUG_ID: String = "iceberg"
  val DB_PLUG_ID: String = "databricks"

  /**
   * Default configuration map for application property plugins.
   * This map defines the default settings for each known plugin.
   * Later we can move this into a configuration file to allow easier customization.
   */
  val APP_PROP_PLUG_CONFIG_MAP: Map[String, AppPropPlugConfig] = Map(
    AURON_PLUG_ID -> AppPropPlugConfig(
      plugId = AURON_PLUG_ID,
      enabled = true,
      additionalConfigs = Map(
        "className" -> "com.nvidia.spark.rapids.tool.planparser.auron.AuronPlugin"
      )
    ),
    DELTA_OSS_PLUG_ID -> AppPropPlugConfig(
      plugId = DELTA_OSS_PLUG_ID,
      enabled = true,
      additionalConfigs = Map(
        "className" -> "com.nvidia.spark.rapids.tool.planparser.delta.DeltaLakeOSSPlugin"
      )
    ),
    ICEBERG_PLUG_ID -> AppPropPlugConfig(
      plugId = ICEBERG_PLUG_ID,
      enabled = true,
      additionalConfigs = Map(
        "hasJobLevelConfigs" -> "true", // config this plugin to be re-evaluated on job level
        "className" -> "com.nvidia.spark.rapids.tool.planparser.iceberg.IcebergPlugin"
      )
    ),
    DB_PLUG_ID -> AppPropPlugConfig(
      plugId = DB_PLUG_ID,
      enabled = true,
      additionalConfigs = Map(
        "hasJobLevelConfigs" -> "true", // config this plugin to be re-evaluated on job level
        "className" -> "com.nvidia.spark.rapids.tool.planparser.db.DBPlugin"
      )
    ),
    HIVE_PLUG_ID -> AppPropPlugConfig(
      plugId = HIVE_PLUG_ID,
      enabled = true,
      additionalConfigs = Map(
        "hasJobLevelConfigs" -> "true", // config this plugin to be re-evaluated on job level
        "className" -> "com.nvidia.spark.rapids.tool.planparser.hive.HivePlugin"
      )
    )
  )

  /**
   * Load the default application property plugin configurations.
   * @return Map of plugin IDs to their configurations for enabled plugins.
   */
  def loadDefaultAppPropPlugConfigs(): Map[String, AppPropPlugConfig] = {
    APP_PROP_PLUG_CONFIG_MAP.filter { case (_, v) => v.enabled }
  }

  /**
   * Instantiate an application property plugin based on its configuration using reflection.
   * @param plugConf The configuration for the plugin to instantiate.
   * @return Try[BaseAppPropPlug] instance if successful, or a Failure if instantiation fails.
   */
  private def instantiateAppPropPlug(plugConf: AppPropPlugConfig): Try[BaseAppPropPlug] = Try {
    val className = plugConf.getClassName
    val clazz = Class.forName(className)
    val plugin = try {
      // Try to instantiate with a String constructor (plugId)
      clazz.getConstructor(classOf[AppPropPlugConfig])
        .newInstance(plugConf)
        .asInstanceOf[BaseAppPropPlug]
    } catch {
      case _: NoSuchMethodException =>
        // Fall back to no-arg constructor
        clazz.getConstructor().newInstance().asInstanceOf[BaseAppPropPlug]
    }
    plugin
  }

  /**
   * Load and instantiate all enabled application property plugins.
   * @return Map of plugin IDs to their instantiated BaseAppPropPlug instances.
   */
  def loadAppPropPlugs(): Map[String, BaseAppPropPlug] = {
    val enabledPlugs = loadDefaultAppPropPlugConfigs()
    enabledPlugs.flatMap { case (plugId, plugConf) =>
      instantiateAppPropPlug(plugConf) match {
        case scala.util.Success(plugin) =>
          Some(plugId -> plugin)
        case scala.util.Failure(exception) =>
          // Log the error and skip this plugin
          logWarning(s"Failed to instantiate plugin $plugId: ${exception.getMessage}")
          None
      }
    }
  }
}
