/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

/**
 * This code is mostly copied from org.apache.spark.benchmark.Benchmark.Timer
 *
 * Utility class to measure timing.
 * @param iteration specifies this is the nth iteration of running the benchmark case
 */
class ToolsTimer(val iteration: Int) {
  private var accumulatedTime: Long = 0L
  private var timeStart: Long = 0L

  def startTiming(): Unit = {
    assert(timeStart == 0L, "Already started timing.")
    timeStart = System.nanoTime
  }

  def stopTiming(): Unit = {
    assert(timeStart != 0L, "Have not started timing.")
    accumulatedTime += System.nanoTime - timeStart
    timeStart = 0L
  }

  def totalTime(): Long = {
    assert(timeStart == 0L, "Have not stopped timing.")
    accumulatedTime
  }
}
