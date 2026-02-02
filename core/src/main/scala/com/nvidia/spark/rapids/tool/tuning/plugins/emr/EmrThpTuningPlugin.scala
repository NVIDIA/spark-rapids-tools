/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.tuning.plugins.emr

import com.nvidia.spark.rapids.tool.PlatformNames
import com.nvidia.spark.rapids.tool.plugins.ConditionTrait
import com.nvidia.spark.rapids.tool.tuning.AutoTuner
import com.nvidia.spark.rapids.tool.tuning.plugins.BaseTuningPlugin

/**
 * EMR tuning plugin for disabling EMR Transparent Huge Pages (THP).
 *
 * EMR has THP enabled by default for Spark applications.
 * Disabling THP can improve performance and reduce execution time variance.
 * Reference: https://docs.kernel.org/admin-guide/mm/transhuge.html
 *
 * @param tunerInst The AutoTuner instance to which this plugin is attached
 */
class EmrThpTuningPlugin(tunerInst: AutoTuner) extends BaseTuningPlugin(tunerInst) {

  /**
   * A ConditionTrait that evaluates to true when the platform is EMR.
   */
  override val activationCond: ConditionTrait[AutoTuner] = (tunerInst: AutoTuner) => {
    tunerInst.platform.platformName.startsWith(PlatformNames.EMR)
  }
}
