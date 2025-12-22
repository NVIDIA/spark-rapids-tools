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

package org.apache.spark.sql.rapids.tool.util

import java.util.concurrent.ConcurrentHashMap

/**
 * Base trait for reference objects that cache CSV-formatted values.
 *
 * @tparam T The concrete type implementing this trait
 */
trait CachedRefBase[T] {
  def value: String
  lazy val csvValue: String = StringUtils.reformatCSVString(value)
}

/**
 * Companion object providing common caching functionality.
 */
object CachedRefBase {
  /**
   * Creates or retrieves a cached reference from the provided table.
   * @param key The string key to lookup
   * @param table The concurrent hash map cache
   * @param factory Function to create new instances
   * @tparam T The reference type
   * @return The cached or newly created reference
   */
  def getOrCreate[T <: CachedRefBase[T]](
    key: String,
    table: ConcurrentHashMap[String, T],
    factory: String => T): T = {
    table.computeIfAbsent(key, factory(_))
  }

  /**
   * Creates a default/empty reference and ensures it's in the cache.
   */
  def createDefault[T <: CachedRefBase[T]](
    defaultValue: String,
    table: ConcurrentHashMap[String, T],
    factory: String => T): T = {
    val ref = factory(defaultValue)
    table.put(defaultValue, ref)
    ref
  }
}
