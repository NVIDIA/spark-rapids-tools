/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.tool

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.profiling.ProfileUtils.replaceDelimiter
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import org.apache.maven.artifact.versioning.ComparableVersion
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.sql.DataFrame

object ToolUtils extends Logging {

  // Add more entries to this lookup table as necessary.
  // There is no need to list all supported versions.
  private val lookupVersions = Map(
    "311" -> new ComparableVersion("3.1.1"),
    "320" -> new ComparableVersion("3.2.0"),
    "340" -> new ComparableVersion("3.4.0")
  )

  // Property to check the spark runtime version. We need this outside of test module as we
  // extend the support runtime for different platforms such as Databricks.
  lazy val sparkRuntimeVersion = {
    org.apache.spark.SPARK_VERSION
  }

  lazy val getEventFromJsonMethod: (String) => org.apache.spark.scheduler.SparkListenerEvent = {
    // Spark 3.4 and Databricks changed the signature on sparkEventFromJson
    val c = Class.forName("org.apache.spark.util.JsonProtocol")
    // Explicitly check against the spark version so that any exception thrown by the JSON protocol
    // won't be swallowed by the invocation error
    if (isSpark340OrLater()) {
      val m = c.getDeclaredMethod("sparkEventFromJson", classOf[String])
      (line: String) =>
        m.invoke(null, line).asInstanceOf[org.apache.spark.scheduler.SparkListenerEvent]
    } else {
      val m = c.getDeclaredMethod("sparkEventFromJson", classOf[org.json4s.JValue])
      (line: String) =>
        m.invoke(null, parse(line)).asInstanceOf[org.apache.spark.scheduler.SparkListenerEvent]
    }
  }

  def compareToSparkVersion(currVersion: String, lookupVersion: String): Int = {
    val lookupVersionObj = lookupVersions.get(lookupVersion).get
    val currVersionObj = new ComparableVersion(currVersion)
    currVersionObj.compareTo(lookupVersionObj)
  }

  def isSpark320OrLater(sparkVersion: String = sparkRuntimeVersion): Boolean = {
    compareToSparkVersion(sparkVersion, "320") >= 0
  }

  def isSpark340OrLater(sparkVersion: String = sparkRuntimeVersion): Boolean = {
    compareToSparkVersion(sparkVersion, "340") >= 0
  }

  def isPluginEnabled(properties: Map[String, String]): Boolean = {
    (properties.getOrElse(config.PLUGINS.key, "").contains("com.nvidia.spark.SQLPlugin")
      && properties.getOrElse("spark.rapids.sql.enabled", "true").toBoolean)
  }

  def showString(df: DataFrame, numRows: Int) = {
    df.showString(numRows, 0)
  }

  /**
   * Parses the string which contains configs in JSON format ( key : value ) pairs and
   * returns the Map of [String, String]
   * @param clusterTag  String which contains property clusterUsageTags.clusterAllTags in
   *                    JSON format
   * @return Map of ClusterTags
   */
  def parseClusterTags(clusterTag: String): Map[String, String] = {
    // clusterTags will be in this format -
    // [{"key":"Vendor","value":"Databricks"},
    // {"key":"Creator","value":"abc@company.com"},{"key":"ClusterName",
    // "value":"job-215-run-1"},{"key":"ClusterId","value":"0617-131246-dray530"},
    // {"key":"JobId","value":"215"},{"key":"RunName","value":"test73longer"},
    // {"key":"DatabricksEnvironment","value":"workerenv-7026851462233806"}]

    // case class to hold key -> value pairs
    case class ClusterTags(key: String, value: String)
    implicit val formats = DefaultFormats
    try {
      val listOfClusterTags = parse(clusterTag)
      val clusterTagsMap = listOfClusterTags.extract[List[ClusterTags]].map(
        x => x.key -> x.value).toMap
      clusterTagsMap
    } catch {
      case NonFatal(_) =>
        logWarning(s"There was an exception parsing cluster tags string: $clusterTag, skipping")
        Map.empty
    }
  }

  /**
   * Try to get the JobId from the cluster name. Parse the clusterName string which
   * looks like:
   * "spark.databricks.clusterUsageTags.clusterName":"job-557875349296715-run-4214311276"
   * and look for job-XXXXX where XXXXX represents the JobId.
   *
   * @param clusterNameString String which contains property clusterUsageTags.clusterName
   * @return Optional JobId if found
   */
  def parseClusterNameForJobId(clusterNameString: String): Option[String] = {
    var jobId: Option[String] = None
    val splitArr = clusterNameString.split("-")
    if (splitArr.contains("job")) {
      val jobIdx = splitArr.indexOf("job")
      // indexes are 0 based so adjust to compare to length
      if (splitArr.length > jobIdx + 1) {
        jobId = Some(splitArr(jobIdx + 1))
      }
    }
    jobId
  }

  // given to duration values, calculate a human readable percent
  // rounded to 2 decimal places. ie 39.12%
  def calculateDurationPercent(first: Long, total: Long): Double = {
    val firstDec = BigDecimal.decimal(first)
    val totalDec = BigDecimal.decimal(total)
    if (firstDec == 0 || totalDec == 0) {
      0.toDouble
    } else {
      val res = (firstDec / totalDec) * 100
      formatDoubleValue(res, 2)
    }
  }

  // given to duration values, calculate a human average
  // rounded to specified number of decimal places.
  def calculateAverage(first: Double, size: Long, places: Int): Double = {
    val firstDec = BigDecimal.decimal(first)
    val sizeDec = BigDecimal.decimal(size)
    if (firstDec == 0 || sizeDec == 0) {
      0.toDouble
    } else {
      val res = (firstDec / sizeDec)
      formatDoubleValue(res, places)
    }
  }

  def formatDoubleValue(bigValNum: BigDecimal, places: Int): Double = {
    bigValNum.setScale(places, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def formatDoublePrecision(valNum: Double): String = {
    truncateDoubleToTwoDecimal(valNum).toString
  }

  def truncateDoubleToTwoDecimal(valNum: Double): Double = {
    // floor is applied after multiplying by 100. This keeps the number "as is" up-to two decimal.
    math.floor(valNum * 100) / 100
  }

  def escapeMetaCharacters(str: String): String = {
    str.replaceAll("\n", "\\\\n")
      .replaceAll("\r", "\\\\r")
      .replaceAll("\t", "\\\\t")
      .replaceAll("\f", "\\\\f")
      .replaceAll("\b", "\\\\b")
      .replaceAll("\u000B", "\\\\v")
      .replaceAll("\u0007", "\\\\a")
  }

  /**
   * Converts a sequence of elements to a single string that can be appended to a formatted text.
   * Delegates to [[com.nvidia.spark.rapids.tool.profiling.ProfileUtils.replaceDelimiter]] to
   * replace what is used as a text delimiter with something else.
   *
   * @param values the sequence of elements to join together.
   * @param separator the separator string to use.
   * @param txtDelimiter the delimiter used by the output file format (i.e., comma for CSV).
   * @return a string representation of the input sequence value. In the resulting string the string
   *         representations (w.r.t. the method toString) of all elements are separated by
   *         the string sep.
   */
  def renderTextField(values: Seq[Any], separator: String, txtDelimiter: String): String = {
    replaceDelimiter(values.mkString(separator), txtDelimiter)
  }

  def formatComplexTypes(
      values: Seq[String], fileDelimiter: String = QualOutputWriter.CSV_DELIMITER): String = {
    renderTextField(values, ";", fileDelimiter)
  }

  def formatPotentialProblems(
      values: Seq[String], fileDelimiter: String = QualOutputWriter.CSV_DELIMITER): String = {
    renderTextField(values, ":", fileDelimiter)
  }
}

object JoinType {
  val Inner = "Inner"
  val Cross = "Cross"
  val LeftOuter = "LeftOuter"
  val RightOuter = "RightOuter"
  val FullOuter = "FullOuter"
  val LeftSemi = "LeftSemi"
  val LeftAnti = "LeftAnti"
}

object BuildSide {
  val BuildLeft = "BuildLeft"
  val BuildRight = "BuildRight"
}

object MlOps {
  val sparkml = "spark.ml."
  val xgBoost = "spark.XGBoost"
  val pysparkLog = "py4j.GatewayConnection.run" // pyspark eventlog contains py4j
}

object MlOpsEventLogType {
  val pyspark = "pyspark"
  val scala = "scala"
}

object SupportedMLFuncsName {
  val funcName: Map[String, String] = Map(
    "org.apache.spark.ml.clustering.KMeans.fit" -> "KMeans",
    "org.apache.spark.ml.feature.PCA.fit" -> "PCA",
    "org.apache.spark.ml.regression.LinearRegression.train" -> "LinearRegression",
    "org.apache.spark.ml.classification.RandomForestClassifier.train" -> "RandomForestClassifier",
    "org.apache.spark.ml.regression.RandomForestRegressor.train" -> "RandomForestRegressor",
    "ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier.train" -> "XGBoost"
  )
}

case class GpuEventLogException(message: String) extends Exception(message)
