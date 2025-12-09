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

/**
 * Trait for configuration objects that support merging and copying operations.
 *
 * Implementations must provide a unique identifier via the `configKey` method and
 * concrete implementations of `mergeWith` and `copyConfig` that return the correct type.
 *
 * @tparam T The concrete type of the configuration class implementing this trait
 */
trait MergeableConfigTrait[T <: MergeableConfigTrait[T]] { self: T =>

  /**
   * Returns the unique key/identifier for this configuration.
   * Used to ensure that only configs with the same key can be merged.
   */
  def configKey: String

  /**
   * Merges this configuration with another configuration of the same type.
   * The other configuration's values typically override this configuration's values
   * when they are non-empty.
   *
   * @param other The other configuration to merge with
   * @return A new configuration instance with merged values
   */
  def merge(other: T): T = {
    require(this.configKey == other.configKey,
      s"Cannot merge configs with different keys: '${this.configKey}' vs '${other.configKey}'")
    // if the other config is empty, then simply return the current one.
    if (!other.isEmpty) {
      mergeWith(other)
    } else {
      this
    }
  }

  /**
   * Performs the actual merge logic. Subclasses must implement this method
   * to define how to merge two configurations of the same type.
   *
   * @param other The other configuration to merge with (keys already verified to match)
   * @return A new configuration instance with merged values
   */
  protected def mergeWith(other: T): T

  /**
   * Creates a deep copy of this configuration.
   *
   * @return A new configuration instance with copied values
   */
  def copy(): T = copyConfig()

  /**
   * Performs the actual copy logic. Subclasses must implement this method
   * to define how to create a copy of the configuration.
   *
   * @return A new configuration instance with copied values
   */
  protected def copyConfig(): T

  /**
   * Helper method to check if a string value is considered empty (null or empty string).
   *
   * @param value The string value to check
   * @return True if the value is null or empty, false otherwise
   */
  protected def isEmptyValue(value: String): Boolean = {
    value == null || value.isEmpty
  }

  /**
   * Helper method to check if this configuration is considered empty.
   * subclasses must override.
   *
   * @return True if the configuration is empty, false otherwise
   */
  def isEmpty: Boolean
}
