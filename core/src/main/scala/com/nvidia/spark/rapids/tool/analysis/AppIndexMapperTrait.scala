/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.analysis

import org.apache.spark.sql.rapids.tool.AppBase

trait AppIndexMapperTrait {
  def zipAppsWithIndex(apps: Seq[AppBase]): Seq[(AppBase, Int)]
}

// Implementation used by Qualification components because AppBase has no appIndex field. Instead,
// this implementation generates index based on the order of the apps.
trait QualAppIndexMapperTrait extends AppIndexMapperTrait {
  def zipAppsWithIndex(apps: Seq[AppBase]): Seq[(AppBase, Int)] = {
    // we did not use zipWithIndex because we want to start from 1 instead of 0
    apps.zipWithIndex.map { case (app, idx) => (app, idx + 1) }
  }
}

// Implementation used by Profiling components because ApplicationInfo has appIndex field which is
// used in generating reports with multiple AppIds
trait ProfAppIndexMapperTrait extends QualAppIndexMapperTrait
