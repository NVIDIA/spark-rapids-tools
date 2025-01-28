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

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import org.apache.hadoop.conf.Configuration

/**
 * A class that generates the report containing only the required and tuned configurations.
 * @param tuningResult The result of the tuning process
 * @param outputDir The directory where the report will be written.
 * @param hadoopConf The Hadoop configuration
 */
class BootstrapReport(tuningResult: TuningResult,
  outputDir: String, hadoopConf: Configuration) {

  /**
   * Loads the bootstrap entries from the tuning result. This applies for any entry that is
   * not removed.
   * @return the list of bootstrap entries
   */
  private def loadBootstrapEntries(): Seq[TuningEntryTrait] = {
    tuningResult.recommendations.filter(e => e.isEnabled() && e.isBootstrap() && !e.isRemoved())
  }
  def generateReport(): Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir,
      s"${tuningResult.appID}-bootstrap.conf",
      s"Required and Tuned configurations to run - ${tuningResult.appID}", Option(hadoopConf))
    try {
      textFileWriter.write(loadBootstrapEntries().map(_.toConfString).reduce(_ + "\n" + _))
    } finally {
      textFileWriter.close()
    }
  }
}
