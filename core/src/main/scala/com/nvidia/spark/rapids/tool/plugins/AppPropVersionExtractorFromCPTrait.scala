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

import scala.util.matching.Regex

/**
 * An implementation of AppPropVersionExtractorTrait that extracts version information
 * from classpath keys in the application properties using a regular expression.
 */
trait AppPropVersionExtractorFromCPTrait extends AppPropVersionExtractorTrait {
  /**
   * Regular expression to match the classpath key for version extraction.
   * The regex should contain a capturing group for the version.
   */
  val cpKeyRegex: Regex

  /**
   * Extracts the version information from the application properties by searching
   * for classpath keys that match the defined regular expression.
   *
   * @param properties Application properties to extract version from.
   * @return An Option containing the version string if found, otherwise None.
   */
  override def extractVersion(properties: collection.Map[String, String]): Option[String] = {
    properties.keys.collectFirst {
      case cpKeyRegex(version) => version
    }
  }
}
