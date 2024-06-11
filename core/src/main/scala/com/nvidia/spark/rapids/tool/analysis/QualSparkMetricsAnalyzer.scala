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
package com.nvidia.spark.rapids.tool.analysis

object QualSparkMetricsAnalyzer extends AppSparkMetricsAggTrait with QualAppIndexMapperTrait {
  // This object is kept to provide the aggregation of the application data for the Qualification.
  // In future, we might need to provide customized logic for the Qualification
  // (i.e., handle metrics; or filter; ..etc)
}
