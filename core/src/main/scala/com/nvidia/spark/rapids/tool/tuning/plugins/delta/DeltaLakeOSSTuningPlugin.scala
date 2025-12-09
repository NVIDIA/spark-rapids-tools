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

package com.nvidia.spark.rapids.tool.tuning.plugins.delta

import com.nvidia.spark.rapids.tool.planparser.delta.DeltaLakeHelper
import com.nvidia.spark.rapids.tool.plugins.ConditionTrait
import com.nvidia.spark.rapids.tool.tuning.AutoTuner
import com.nvidia.spark.rapids.tool.tuning.plugins.BaseTuningPlugin

/**
 * Tuning plugin for optimizing Delta Lake operations on OSS.
 *
 * This plugin activates when Delta Lake is detected in the application properties.
 * It applies specific tuning rules to enhance performance for Delta Lake workloads.
 *
 * @param tunerInst The AutoTuner instance to which this plugin is attached
 */
class DeltaLakeOSSTuningPlugin(tunerInst: AutoTuner) extends BaseTuningPlugin(tunerInst) {

  /**
   * A ConditionTrait that evaluates to true when DeltaLake OSS is detected.
   */
  override val activationCond: ConditionTrait[AutoTuner] = (tunerInst: AutoTuner) => {
    DeltaLakeHelper.eval(tunerInst.appInfoProvider.getAllProperties)
  }
}
