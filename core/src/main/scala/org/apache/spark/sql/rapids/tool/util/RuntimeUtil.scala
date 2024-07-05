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

package org.apache.spark.sql.rapids.tool.util

import java.io.{PrintWriter, StringWriter}

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.ToolUtils

/**
 * Utility class to pull information about the runtime system and the properties of the build
 * loaded.
 * In addition, it concatenates properties from the runtime (i.e., SparkVersion).
 * It is expected that the list of properties in that file will grow depending on whether a
 * property helps understanding and investigating the tools output.
 */
object RuntimeUtil extends Logging {
  private val REPORT_LABEL = "RAPIDS Accelerator for Apache Spark's Build/Runtime Information"
  private val REPORT_FILE_NAME = "runtime.properties"

  /**
   * Generates a file containing the properties of the build loaded.
   * In addition, it concatenates properties from the runtime (i.e., SparkVersion).
   * It is expected that the list of properties in that file will grow depending on whether a
   * property helps understanding and investigating the tools output.
   *
   * @param outputDir the directory where the report is generated.
   * @param hadoopConf the hadoop configuration object used to access the HDFS if any.
   */
  def generateReport(outputDir: String, hadoopConf: Option[Configuration] = None): Unit = {
    val buildProps = RapidsToolsConfUtil.loadBuildProperties
    // Add the Spark version used in runtime.
    // Note that it is different from the Spark version used in the build.
    buildProps.setProperty("runtime.spark.version", ToolUtils.sparkRuntimeVersion)
    val reportWriter = new ToolTextFileWriter(outputDir, REPORT_FILE_NAME, REPORT_LABEL, hadoopConf)
    try {
      reportWriter.writeProperties(buildProps, REPORT_LABEL)
    } finally {
      reportWriter.close()
    }
    // Write the properties to the log
    val writer = new StringWriter
    buildProps.list(new PrintWriter(writer))
    logInfo(s"\n$REPORT_LABEL\n${writer.getBuffer.toString}")
  }

  /**
   * Returns a map of the JVM and OS information.
   * @return Map[String, String] - Map of the JVM and OS information.
   */
  def getJVMOSInfo: Map[String, String] = {
    Map(
      "jvm.name" -> System.getProperty("java.vm.name"),
      "jvm.version" -> System.getProperty("java.version"),
      "os.name" -> System.getProperty("os.name"),
      "os.version" -> System.getProperty("os.version")
    )
  }
}


