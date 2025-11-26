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

/**
 * Trait for extracting version information from application properties.
 * By default, this trait does not implement any extraction logic. Each specific implementation
 * should extend this to specify how to extract version information.
 * For example, some plugins extract the version from the classPath jar file names.
 */
trait AppPropVersionExtractorTrait {
  /**
   * Extracts the version information from the application properties.
   * By default, this method returns None. Subclasses can override this method
   * to provide specific logic for extracting version information.
   *
   * @param properties Application properties to extract version from.
   * @return An Option containing the version string if found, otherwise None.
   */
  def extractVersion(properties: collection.Map[String, String]): Option[String] = None
}
