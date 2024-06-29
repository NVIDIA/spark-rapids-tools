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

import com.nvidia.spark.rapids.tool.analysis.ProfAppIndexMapperTrait
import com.nvidia.spark.rapids.tool.profiling.AccumProfileResults

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.annotation.Since
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

trait AppStageMetricsViewTrait extends ViewableTrait[AccumProfileResults] {
  override def getLabel: String = "Stage Level All Metrics for Application"
  override def getDescription: String = "Stage Level Metrics"

  override def sortView(
      rows: Seq[AccumProfileResults]): Seq[AccumProfileResults] = {
    rows.sortBy(cols => (cols.stageId, cols.appIndex, cols.accumulatorId))
  }
}

@Since("24.06.2")
object ProfStageMetricView extends AppStageMetricsViewTrait with ProfAppIndexMapperTrait {

  override def getRawView(app: AppBase, index: Int): Seq[AccumProfileResults] = {
    app match {
      case app: ApplicationInfo =>
        app.planMetricProcessor.generateStageLevelAccums()
      case _ => Seq.empty
    }
  }
}
