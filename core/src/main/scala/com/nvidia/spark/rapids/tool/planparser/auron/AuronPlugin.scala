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

package com.nvidia.spark.rapids.tool.planparser.auron

import com.nvidia.spark.rapids.tool.plugins.{AppPropPlugConfig, AppPropRuntimeTrait, AppPropVersionExtractorTrait, BaseAppPropPlug, PropConditionTrait}

import org.apache.spark.sql.rapids.tool.util.SparkRuntime
import org.apache.spark.sql.rapids.tool.util.SparkRuntime.SparkRuntime

/**
 * Trait to indicate that the plugin is related to Auron runtime.
 */
trait AuronPropRuntimeTrait extends AppPropRuntimeTrait {
  override def runtime: Option[SparkRuntime] = Some(SparkRuntime.AURON)
}

/**
 * Plugin implementation for Auron usage in Spark applications.
 * Note that Auron can be used with feature support such as GPU and Iceberg.
 * @param pluginConfig configuration for the Auron plugin
 */
class AuronPlugin(
    override val pluginConfig: AppPropPlugConfig
) extends BaseAppPropPlug(pluginConfig = pluginConfig) with AuronPropRuntimeTrait {
  /**
   * Callback function to evaluate the property based on application properties.
   */
  override val propCondition: PropConditionTrait = AuronParseHelper

  /**
   * Callback function to extract the version of Auron from application properties.
   */
  override val versionExtractor: Option[AppPropVersionExtractorTrait] = Some(AuronParseHelper)

  /**
   * Post-enable action that extracts and stores Auron version in plugin metadata.
   * This demonstrates how child classes can populate the pluginMetadata container.
   */
  override def postEnableAction(properties: collection.Map[String, String]): Unit = {
    // Extract version and store in metadata
    versionExtractor.flatMap(_.extractVersion(properties)).foreach { version =>
      addMetadata("version", version)
    }

    // Store Auron-specific configuration in metadata
    properties.get("spark.auron.enabled").foreach { enabled =>
      addMetadata("auron.enabled", enabled)
    }
  }
}
