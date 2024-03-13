/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

import scala.collection.mutable.{ArrayBuffer,HashMap}
import scala.io.{BufferedSource, Source}
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.{Platform, PlatformFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging

/**
 * This class is used to check what the RAPIDS Accelerator for Apache Spark
 * supports for data formats and data types.
 * By default it relies on a csv file included in the jar which is generated
 * by the plugin which lists the formats and types supported.
 * The class also supports a custom speedup factor file as input.
 */
class PluginTypeChecker(platform: Platform = PlatformFactory.createInstance(),
                        speedupFactorFile: Option[String] = None) extends Logging {

  private val NS = "NS"
  // configured off
  private val CO = "CO"
  private val NA = "NA"
  private val NONE = "None"

  private val DEFAULT_DS_FILE = "supportedDataSource.csv"
  private val SUPPORTED_EXECS_FILE = "supportedExecs.csv"
  private val SUPPORTED_EXPRS_FILE = "supportedExprs.csv"

  // map of file format => Map[support category => Seq[Datatypes for that category]]
  // contains the details of formats to which ones have datatypes not supported.
  // Write formats contains only the formats that are supported. Types cannot be determined
  // from event logs for write formats.
  // var for testing purposes
  private var (readFormatsAndTypes, writeFormats) = readSupportedTypesForPlugin

  private var supportedOperatorsScore = readOperatorsScore

  private var supportedExecs = readSupportedExecs

  private var supportedExprs = readSupportedExprs

  // for testing purposes only
  def setPluginDataSourceFile(filePath: String): Unit = {
    val source = Source.fromFile(filePath)
    val (readFormatsAndTypesTest, writeFormatsTest) = readSupportedTypesForPlugin(source)
    readFormatsAndTypes = readFormatsAndTypesTest
    writeFormats = writeFormatsTest
  }

  def setOperatorScore(filePath: String): Unit = {
    val source = Source.fromFile(filePath)
    supportedOperatorsScore = readOperators(source, "score", true,
      processOperatorLine).map(x => (x._1, x._2.toDouble))
  }

  def setSupportedExecs(filePath: String): Unit = {
    val source = Source.fromFile(filePath)
    // We are reading only first 2 columns for now and other columns are ignored intentionally.
    supportedExecs = readOperators(source, "execs", true, processOperatorLine)
  }

  def setSupportedExprs(filePath: String): Unit = {
    val source = Source.fromFile(filePath)
    // We are reading only first 2 columns for now and other columns are ignored intentionally.
    supportedExprs = readOperators(source, "exprs", true, processOperatorLine)
  }

  def getSupportedExprs: Map[String, String] = supportedExprs

  private def readOperatorsScore: Map[String, Double] = {
    speedupFactorFile match {
      case None =>
        logInfo(s"Reading operators scores with platform: $platform")
        val file = platform.getOperatorScoreFile
        val source = Source.fromResource(file)
        readOperators(source, "score", true,
          processOperatorLine).map(x => (x._1, x._2.toDouble))
      case Some(file) =>
        logInfo(s"Reading operators scores from custom speedup factor file: $file")
        try {
          val path = new Path(file)
          val fs = FileSystem.get(path.toUri, new Configuration())
          val source = new BufferedSource(fs.open(path))
          readOperators(source, "score", true,
            processOperatorLine).map(x => (x._1, x._2.toDouble))
        } catch {
          case NonFatal(e) =>
            logError(s"Exception processing operators scores with file: $file", e)
          Map.empty[String, Double]
        }
    }
  }

  private def readSupportedExecs: Map[String, String] = {
    val source = Source.fromResource(SUPPORTED_EXECS_FILE)
    readOperators(source, "execs", true, processOperatorLine)
  }

  private def readSupportedExprs: Map[String, String] = {
    val source = Source.fromResource(SUPPORTED_EXPRS_FILE)
    // Some SQL function names have backquotes(`) around their names,
    // so we remove them before saving.
    readOperators(source, "exprs", true, processOperatorLine).map(
      x => (x._1.toLowerCase.replaceAll("\\`", "").replaceAll(" ",""), x._2))
  }

  def readUnsupportedOpsByDefaultReasons: Map[String, String] = {
    val execsSource = Source.fromResource(SUPPORTED_EXECS_FILE)
    val unsupportedExecsBydefault = readOperators(execsSource, "execs", false, processOperatorLine)
    val exprsSource = Source.fromResource(SUPPORTED_EXPRS_FILE)
    val unsupportedExprsByDefault = readOperators(exprsSource, "exprs", false, processOperatorLine).
        map(x => (x._1.toLowerCase.replaceAll("\\`", "").replaceAll(" ",""), x._2))
    unsupportedExecsBydefault ++ unsupportedExprsByDefault
  }

  private def readSupportedTypesForPlugin: (
      Map[String, Map[String, Seq[String]]], ArrayBuffer[String]) = {
    val source = Source.fromResource(DEFAULT_DS_FILE)
    readSupportedTypesForPlugin(source)
  }

  /**
   * Reads operators from a source and processes them based on the provided logic.
   *
   * @param source The source from which to read the operators.
   * @param operatorType The type of operators being read ("execs", "exprs" or "score").
   * @param isSupported Flag to determine if we are reading supported or unsupported operators.
   * @param processLine A function that takes a line, it's operator type and isSupported flag,
   *                    and returns a sequence of key-value pairs to add to the result map.
   * @return A Map containing the processed operators.
   */
  def readOperators(source: BufferedSource, operatorType: String, isSupported: Boolean,
      processLine: (Array[String], String, Boolean) => Seq[(String, String)]
  ): Map[String, String] = {
    val operatorsMap = HashMap.empty[String, String]
    try {
      val fileContents = source.getLines().toSeq
      if (fileContents.size < 2) {
        throw new IllegalStateException(s"${source.toString} file appears corrupt, " +
            s"must have at least the header and one line")
      }
      val header = fileContents.head.split(",").map(_.trim.toLowerCase)
      fileContents.tail.foreach { line =>
        val cols = line.split(",").map(_.trim)
        if (header.size != cols.size) {
          throw new IllegalStateException(s"${source.toString} file appears corrupt, " +
              s"header length doesn't match rows length. " +
              s"Row that doesn't match is ${cols.mkString(",")}")
        }
        processLine(cols, operatorType, isSupported).foreach { case (key, value) =>
          operatorsMap.put(key, value)
        }
      }
    } finally {
      source.close()
    }
    operatorsMap.toMap
  }

  // Custom logic for processing lines for supported or unsupported operators
  // Here unsupported operators mean the ones that are not supported by default.
  // In the notes section of the supported csv files, it specifies reason for why it is not
  // supported by default. We use this information to propagate it unsupported operators
  // csv file.
  private def processOperatorLine(cols: Array[String], operatorType:String,
      isSupported: Boolean): Seq[(String, String)] = {
    operatorType match {
      case "exprs" if isSupported =>
        // Logic for supported expressions
        val exprName = Seq((cols(0), cols(1)))
        val sqlFuncNames = if (cols(2).nonEmpty && cols(2) != NONE ){
          // There are addidtional checks for Expressions. In physical plan, SQL function name is
          // printed instead of expression name. We have to save both expression name and
          // SQL function name(if there is one) so that we don't miss the expression while
          // parsing the execs.
          // Ex: Expression name = Substring, SQL function= `substr`; `substring`
          // Ex: Expression name = Average, SQL function name = `avg`
          cols(2).split(";").map(_.trim).toSeq
        } else {
          Seq.empty
        }
        exprName ++ sqlFuncNames.map(name => (name, cols(1)))
      case "exprs" =>
        // Logic for unsupported expressions
        if (cols(1) == "NS" && cols(3) != NONE) {
          val exprName = Seq((cols(0), cols(3)))
          val sqlFuncNames = if (cols(2).nonEmpty && cols(2) != NONE) {
            cols(2).split(";").map(_.trim).toSeq
          } else {
            Seq.empty
          }
          exprName ++ sqlFuncNames.map(name => (name, cols(3)))
        } else {
          Seq.empty
        }
      case _ if isSupported =>
        // Logic for supported execs
        Seq((cols(0), cols(1)))
      case _ =>
        // Logic for unsupported execs
        if (cols(1) == "NS" && cols(2) != NONE) {
          //Exec names have Exec at the end, we need to remove it to match with the names
          // saved in the csv file.
          Seq((cols(0).dropRight(4), cols(2)))
        } else {
          Seq.empty
        }
    }
  }

  // file format should be like this:
  // Format,Direction,BOOLEAN,BYTE,SHORT,INT,LONG,FLOAT,DOUBLE,DATE,...
  // CSV,read,S,S,S,S,S,S,S,S,S*,S,NS,NA,NS,NA,NA,NA,NA,NA
  // Parquet,write,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA
  private def readSupportedTypesForPlugin(
      source: BufferedSource): (Map[String, Map[String, Seq[String]]], ArrayBuffer[String]) = {
    // get the types the Rapids Plugin supports
    val allSupportedReadSources = HashMap.empty[String, Map[String, Seq[String]]]
    val allSupportedWriteFormats = ArrayBuffer[String]()
    try {
      val fileContents = source.getLines().toSeq
      if (fileContents.size < 2) {
        throw new IllegalStateException("supportedDataSource file appears corrupt," +
          " must have at least the header and one line")
      }
      // first line is header
      val header = fileContents.head.split(",").map(_.toLowerCase)
      // the rest of the rows are file formats with type supported info
      fileContents.tail.foreach { line =>
        val cols = line.split(",")
        if (header.size != cols.size) {
          throw new IllegalStateException("supportedDataSource file appears corrupt," +
            " header length doesn't match rows length")
        }
        val format = cols(0).toLowerCase
        val direction = cols(1).toLowerCase()
        if (direction.equals("read")) {
          val dataTypesToSup = header.drop(2).zip(cols.drop(2)).toMap
          val nsTypes = dataTypesToSup.filter { case (_, sup) =>
            sup.equals(NA) || sup.equals(NS) || sup.equals(CO)
          }.keys.toSeq.map(_.toLowerCase)
          val allNsTypes = nsTypes.flatMap(t => getOtherTypes(t) :+ t)
          val allBySup = HashMap(NS -> allNsTypes)
          allSupportedReadSources.put(format, allBySup.toMap)
        } else if (direction.equals("write")) {
          allSupportedWriteFormats += format
        }
      }
    } finally {
      source.close()
    }
    (allSupportedReadSources.toMap, allSupportedWriteFormats)
  }

  def getOtherTypes(typeRead: String): Seq[String] = {
    typeRead match {
      case "long" => Seq("bigint")
      case "short" => Seq("smallint")
      case "int" => Seq("integer")
      case "byte" => Seq("tinyint")
      case "float" => Seq("real")
      case "decimal" => Seq("dec", "numeric")
      case "calendar" => Seq("interval")
      case _ => Seq.empty[String]
    }
  }

  // Parsing the schema string is very complex when you get into nested types, so for now
  // we do the simpler thing of checking to see if the schema string contains types we
  // don't support.
  // NOTE, UDT doesn't show up in the event log, when its written, it gets written as
  // other types since parquet/orc has to know about it
  def scoreReadDataTypes(format: String, schema: String): (Double, Set[String]) = {
    val schemaLower = schema.toLowerCase
    val formatInLower = format.toLowerCase
    val typesBySup = readFormatsAndTypes.get(formatInLower)
    val score = typesBySup match {
      case Some(dtSupMap) =>
        // check if any of the not supported types are in the schema
        val nsFiltered = dtSupMap(NS).filter(t => schemaLower.contains(t.toLowerCase()))
        if (nsFiltered.nonEmpty) {
          (0.0, nsFiltered.toSet)
        } else {
          // Started out giving different weights based on partial support and so forth
          // but decided to be optimistic and not penalize if we don't know, perhaps
          // make it smarter later.
          // Schema could also be incomplete, but similarly don't penalize since we don't
          // know.
          (1.0, Set.empty[String])
        }
      case None =>
        // assume we don't support that format
        (0.0, Set("*"))
    }
    score
  }

  def isWriteFormatSupported(writeFormat: String): Boolean = {
    val format = writeFormat.toLowerCase.trim
    writeFormats.map(x => x.trim).contains(format)
  }

  def isWriteFormatSupported(writeFormat: ArrayBuffer[String]): ArrayBuffer[String] = {
    writeFormat.map(x => x.toLowerCase.trim).filterNot(
      writeFormats.map(x => x.trim).contains(_))
  }

  def getSpeedupFactor(execOrExpr: String): Double = {
    supportedOperatorsScore.get(execOrExpr).getOrElse(1)
  }

  def isExecSupported(exec: String): Boolean = {
    // special case ColumnarToRow and assume it will be removed or will we replace
    // with GPUColumnarToRow. TODO - we can add more logic here to look at operator
    //  before and after
    if (exec == "ColumnarToRow") {
      return true
    }
    if (supportedExecs.contains(exec)) {
      val execSupported = supportedExecs.getOrElse(exec, "NS")
      if (execSupported == "S") {
        true
      } else {
        logDebug(s"Exec explicitly not supported, value: $execSupported")
        false
      }
    } else {
      logDebug(s"Exec $exec does not exist in supported execs file")
      false
    }
  }

  def isExprSupported(expr: String): Boolean = {
    val exprLowercase = expr.toLowerCase
    if (supportedExprs.contains(exprLowercase)) {
      val exprSupported = supportedExprs.getOrElse(exprLowercase, "NS")
      if (exprSupported == "S") {
        true
      } else {
        logDebug(s"Expression explicitly not supported, value: $exprSupported")
        false
      }
    } else {
      logDebug(s"Expr $expr does not exist in supported execs file")
      false
    }
  }
}
