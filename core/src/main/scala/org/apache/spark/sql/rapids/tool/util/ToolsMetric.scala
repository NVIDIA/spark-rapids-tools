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

import java.util.concurrent.atomic.AtomicLong

/**
 * Data class to hold information about some statistics related to the core-tools runTime.
 * @param name name of the metric.
 * @param description any comments to define what the metric is going to be used for.
 * @param value AtomicLong represent the initial value of the metric.
 * @param onlyPositives flag to enforce return of 0 if the value is negative.
 */
case class ToolsMetric(
    name: String,
    description: String,
    value: AtomicLong = new AtomicLong(0),
    onlyPositives: Boolean = true) {
  def inc(): Long = {
    value.incrementAndGet()
  }

  def inc(delta: Long): Long = {
    value.addAndGet(delta)
  }

  def dec(): Long = {
    value.decrementAndGet()
  }

  def dec(delta: Long): Long = {
    value.addAndGet(-delta)
  }

  /**
   * Public method to retrieve the value of the counter.
   * It checks if the onlyPositives flag is enabled to enforce return of 0 if the value is negative.
   * @return the value of the counter if onlyPositives is enabled and the value is GTE 0.
   *         Otherwise, it returns 0.
   */
  def getValue: Long = {
    val actualCounter = value.get()
    processReturnValue(actualCounter)
  }

  /**
   * Public method to retrieve the value of the counter and reset it to newValue.
   * It checks if the onlyPositives flag is enabled to enforce return of 0 if the value is negative.
   * @return the value of the counter if onlyPositives is enabled and the value is GTE 0.
   *         Otherwise, it returns 0.
   */
  def getValueAndReset(newValue: Option[Long] = None): Long = {
    val actualCounter = value.getAndSet(newValue.getOrElse(0L))
    processReturnValue(actualCounter)
  }

  private def processReturnValue(actualCounter: Long): Long = {
    if (onlyPositives && actualCounter < 0) 0 else actualCounter
  }
}
