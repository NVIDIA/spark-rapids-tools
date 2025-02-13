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

package com.nvidia.spark.rapids.tool.tuning

/**
 * Enumerated type to define the different modifications that the AutoTuner performs on
 * a sepecific property.
 */
object TuningOpTypes extends Enumeration {
  type TuningOpType = Value
  val ADD,    // the property is added
      REMOVE, // the property is removed
      UPDATE, // the property is updated
      CLONE,  // the property is the same
      UNRESOLVED, // the property is processed by the AutoTuner but the value is not resolved
      UNKNOWN = Value

  def isTuned(tuningOpType: TuningOpType): Boolean = {
    tuningOpType == ADD || tuningOpType == UPDATE ||
      tuningOpType == REMOVE || tuningOpType == UNRESOLVED
  }
}
