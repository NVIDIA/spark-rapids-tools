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

import org.apache.spark.sql.rapids.tool.util.SparkRuntime

/**
 * Trait representing the reported Runtime based on each type of plugin.
 * By default, runtime plugins do not specify the runtime; except some specific ones
 * that override this trait to indicate the runtime they represent. Example of that is
 * the Auron plugin which indicates the Auron runtime.
 */
trait AppPropRuntimeTrait {
  def runtime: Option[SparkRuntime.SparkRuntime] = None
}

/**
 * Trait representing a plugin that evaluates application properties to determine
 * if certain features or configurations are enabled.
 */
trait AppPropPlugTrait extends AppPropRuntimeTrait {
  /**
   * Indicates that once the property is set to True, it should remain True and not be re-evaluated.
   */
  val stickyTrue: Boolean = true
  /**
   * Indicates whether the property has job-level configuration.
   * By default, this is set to false.
   */
  val hasJobLevelConfigs: Boolean = false
  /**
   * Indicates whether the property is currently enabled. This flag is mutable and can be updated
   * based on the evaluation of application properties.
   */
  var isEnabled: Boolean = false
  /**
   * Callback function to evaluate the property based on application properties.
   */
  val propCondition: PropConditionTrait

  /**
   * Optional callback function to extract the version of the feature from application properties.
   */
  val versionExtractor: Option[AppPropVersionExtractorTrait] = None

  /**
   * Re-evaluates the property based on the provided application properties.
   * This is only done if the property is not already enabled and stickyTrue is false.
   * @param properties Application properties to evaluate against.
   */
  def reEvaluate(properties: collection.Map[String, String]): Unit = {
    if (!(isEnabled && stickyTrue)) {
      isEnabled ||= propCondition.eval(properties)
      if (isEnabled) {
        // trigger an action once a plugin becomes enabled.
        postEnableAction(properties)
      }
    }
  }

  /**
   * Callback function that is invoked after the property has been enabled.
   * @param properties Application properties based on which the property was enabled.
   */
  def postEnableAction(properties: collection.Map[String, String]): Unit = {
    // default no-op
  }

  /**
   * Extracts the version information from the application properties.
   * By default, this method returns None. Subclasses can override this method
   * to provide specific logic for extracting version information.
   *
   * @param properties Application properties to extract version from.
   * @return An Option containing the version string if found, otherwise None.
   */
  def evalPluginVersion(properties: collection.Map[String, String]): Option[String] = {
    if (!isEnabled) {
      return None
    }
    versionExtractor.flatMap(_.extractVersion(properties))
  }
}
