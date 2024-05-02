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

package com.nvidia.spark.rapids.tool.qualification

import org.apache.spark.sql.rapids.tool.AppMetaData

/**
 * A class to handle appMetadata for a running SparkApplication. This is created when a Spark
 * Listener is used to analyze an existing App.
 * The AppMetaData for a running application does not have eventlog.
 *
 * @param rName      name of the application
 * @param rId        application id
 * @param rUser      user who ran the Spark application
 * @param rStartTime startTime of a Spark application
 */
class RunningAppMetadata(
    rName: String,
    rId: Option[String],
    val rUser: String,
    val rStartTime: Long) extends AppMetaData(None, rName, rId, rUser, rStartTime) {

}

object RunningAppMetadata {
  def apply(
      rName: String,
      rId: Option[String],
      rStartTime: Long): RunningAppMetadata = {
    new RunningAppMetadata(rName, rId, "", rStartTime)
  }
}