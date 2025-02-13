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

package com.nvidia.spark.rapids.tool.qualification

import com.nvidia.spark.rapids.tool.analysis.AppSQLPlanAnalyzer

import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo

/**
 * A class that extends the AppSQLPlanAnalyzer to analyze the SQL plans of the applications.
 * It needs to override the parent class to process the WriteDataFormat while visiting a
 * SparkGraphNode.
 * In addition to the side effects mentioned in AppSQLPlanAnalyzer, it updates the writeDataFormat
 * field defined in the QualificationAppInfo object.
 *
 * @param app the Application info objects that contains the SQL plans to be processed
 * @param appIndex the application index used in viewing multiple applications together.
 */
class QualSQLPlanAnalyzer(
    app: QualificationAppInfo, appIndex: Integer) extends AppSQLPlanAnalyzer(app, appIndex) {

}
