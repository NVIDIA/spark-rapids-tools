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
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

/**
 * Base class for application analysis
 * @param app the AppBase object to analyze
 */
abstract class AppAnalysisBase(app: AppBase) {
 // Keep for future refactoring to use common methods for all Analysis classes.
 // Ideally, we can common interface
 // 1- caching layer
 // 2- initializations
 // 3- interface to pull information to generate views and reports

 /**
  * Determines if diagnostic views are enabled. Diagnostic views are enabled in case of Profiler
  * and only when explicitly enabled for the ApplicationInfo object.
  */
 protected val isDiagnosticViewsEnabled: Boolean = app match {
   case appInfo: ApplicationInfo => appInfo.isDiagnosticViewsEnabled
   case _ => false
 }
}
