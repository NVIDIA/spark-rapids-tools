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

package com.nvidia.spark.rapids.tool.views

import com.nvidia.spark.rapids.tool.analysis.AppIndexMapperTrait
import com.nvidia.spark.rapids.tool.profiling.ProfileResult

import org.apache.spark.sql.rapids.tool.AppBase

trait ViewableTrait[R <: ProfileResult] extends AppIndexMapperTrait {
  def getLabel: String
  def getDescription: String = ""

  def getRawView(app: AppBase, index: Int): Seq[R]

  def getRawView(apps: Seq[AppBase]): Seq[R] = {
    val allRows = zipAppsWithIndex(apps).flatMap { case (app, index) =>
      getRawView(app, index)
    }
    if (allRows.isEmpty) {
      allRows
    } else {
      sortView(allRows)
    }
  }

  def sortView(rows: Seq[R]): Seq[R] = rows
}
