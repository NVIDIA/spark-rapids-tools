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

package com.nvidia.spark.rapids.tool.planparser.auron

import com.nvidia.spark.rapids.tool.planparser.SQLPlanParser.OssSparkPlanParserTrait

import org.apache.spark.sql.rapids.tool.AppBase

/**
 * SQL Plan Parser for Auron (Native Spark) execution plans.
 *
 * This parser handles Spark applications that use Auron's native execution engine, which
 * provides accelerated execution through native operators (e.g., NativeProject, NativeFilter).
 * It extends OssSparkPlanParserTrait to inherit standard Spark plan parsing capabilities while
 * enabling Auron-specific operator handling through AuronOssOpMapper.
 *
 * Key features:
 * - Detects Auron-enabled applications via spark.sql.extensions containing
 *   AuronSparkSessionExtension
 * - Delegates operator mapping to AuronOssOpMapper for Native* operator conversions
 * - Inherits standard OSS Spark parsing logic from parent trait
 *
 * The parser is registered in SQLPlanParserConfig and tried in order with other platform parsers
 * (GPU, Photon) during plan analysis.
 *
 * @see AuronOssOpMapper for Auron-specific operator name mappings
 * @see OssSparkPlanParserTrait for inherited parsing implementation
 */
object AuronPlanParser extends OssSparkPlanParserTrait {

  /**
   * Determines if this parser accepts the given application context.
   *
   * Only accepts applications where Auron is enabled, identified by the presence of
   * AuronSparkSessionExtension in spark.sql.extensions and spark.auron.enabled=true.
   *
   * @param app The application context to evaluate
   * @return true if Auron is enabled, false otherwise
   */
  override def acceptsCtxt(app: AppBase): Boolean = {
    app.isAuronEnabled
  }
}
