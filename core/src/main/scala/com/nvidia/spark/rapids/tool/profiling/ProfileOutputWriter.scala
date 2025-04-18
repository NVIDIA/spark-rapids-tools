/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.tool.profiling

import com.nvidia.spark.rapids.SparkRapidsBuildInfoEvent
import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import org.apache.commons.lang3.StringUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization


class ProfileOutputWriter(outputDir: String, filePrefix: String, numOutputRows: Int,
    outputCSV: Boolean = false) {

  implicit val formats: DefaultFormats.type = DefaultFormats

  private val textFileWriter = new ToolTextFileWriter(outputDir,
    s"$filePrefix.log", "Profile summary")

  def writeText(strToWrite: String): Unit = {
    textFileWriter.write(strToWrite)
  }

  private def writeTextTable(messageHeader: String, outRows: Seq[ProfileResult],
      emptyText: Option[String], tableDesc: Option[String]): Unit = {
    val headerText = tableDesc match {
      case Some(desc) => s"$messageHeader: $desc"
      case None => s"$messageHeader:"
    }
    textFileWriter.write(s"\n$headerText\n")

    if (outRows.nonEmpty) {
      val outStr = ProfileOutputWriter.makeFormattedString(numOutputRows, 0,
        outRows.head.outputHeaders, outRows.map(_.convertToSeq()))
      textFileWriter.write(outStr)
    } else {
      val finalEmptyText = emptyText match {
        case Some(text) => text
        case None => messageHeader
      }
      textFileWriter.write(s"No $finalEmptyText Found!\n")
    }
  }

  def writeSparkRapidsBuildInfo(headerText: String,
      sparkRapidsBuildInfo: Seq[SparkRapidsBuildInfoEvent]): Unit = {
    val fileName = headerText.replace(" ", "_").toLowerCase
    val jsonWriter = new ToolTextFileWriter(outputDir, s"$fileName.json", s"$headerText JSON:")
    try {
      jsonWriter.write(Serialization.writePretty(sparkRapidsBuildInfo) + "\n")
    } finally {
      jsonWriter.close()
    }
  }

  def write(headerText: String, outRows: Seq[ProfileResult],
      emptyTableText: Option[String] = None, tableDesc: Option[String] = None): Unit = {
    writeTextTable(headerText, outRows, emptyTableText, tableDesc)
    if (outputCSV) {
      ProfileOutputWriter.writeCSVTable(headerText, outRows, outputDir)
    }
  }

  def writeCSVTable(headerText: String, outRows: Seq[ProfileResult]): Unit = {
    if (outputCSV) {
      ProfileOutputWriter.writeCSVTable(headerText, outRows, outputDir)
    }
  }

  /**
   * Writes the given profile results as JSON Lines (JSONL) format to a file.
   *
   * @param headerText The header text used to generate the filename.
   * @param outRows The sequence of profile results to write.
  */
  def writeJsonL(headerText: String, outRows: Seq[ProfileResult]): Unit = {
    val fileName = headerText.replace(" ", "_").toLowerCase
    val jsonWriter = new ToolTextFileWriter(outputDir, s"${fileName}.json", s"$headerText JSON:")
    try {
      outRows.foreach { row =>
        jsonWriter.write(Serialization.write(row) + "\n")
      }
    } finally {
      jsonWriter.close()
    }
  }

  def close(): Unit = {
    textFileWriter.close()
  }
}

object ProfileOutputWriter {
  val CSVDelimiter = ","

  private def stringIfempty(str: String): String = {
    if (str == null || str.isEmpty) "\"\"" else str
  }

  /**
   * Write a CSV file give the input header and data.
   */
  def writeCSVTable(header: String, outRows: Seq[ProfileResult], outputDir: String): Unit = {
    if (outRows.nonEmpty) {
      // need to have separate CSV file per table, use header text
      // with spaces as _ and lowercase as filename
      val suffix = header.replace(" ", "_").toLowerCase
      val csvWriter = new ToolTextFileWriter(outputDir, s"$suffix.csv", s"$header CSV:")
      try {
        val headerString = outRows.head.outputHeaders.mkString(CSVDelimiter)
        csvWriter.write(headerString + "\n")
        val rows = outRows.map(_.convertToCSVSeq())
        rows.foreach { row =>
          val formattedRow = row.map(stringIfempty)
          val outStr = formattedRow.mkString(CSVDelimiter)
          csvWriter.write(outStr + "\n")
        }
      } finally {
        csvWriter.close()
      }
    }
  }

  /**
   * Regular expression matching full width characters.
   *
   * Looked at all the 0x0000-0xFFFF characters (unicode) and showed them under Xshell.
   * Found all the full width characters, then get the regular expression.
   */
  private val fullWidthRegex = ("""[""" +
    // scalastyle:off nonascii
    "\u1100-\u115F" +
    "\u2E80-\uA4CF" +
    "\uAC00-\uD7A3" +
    "\uF900-\uFAFF" +
    "\uFE10-\uFE19" +
    "\uFE30-\uFE6F" +
    "\uFF00-\uFF60" +
    "\uFFE0-\uFFE6" +
    // scalastyle:on nonascii
    """]""").r

  /**
   * Return the number of half widths in a given string. Note that a full width character
   * occupies two half widths.
   *
   * For a string consisting of 1 million characters, the execution of this method requires
   * about 50ms.
   */
  def stringHalfWidth(str: String): Int = {
    if (str == null) 0 else str.length + fullWidthRegex.findAllIn(str).size
  }

  // originally copied from Spark showString and modified
  def makeFormattedString(
      _numRows: Int,
      truncate: Int = 20,
      schema: Seq[String],
      rows: Seq[Array[String]]): String = {
    val numRows = _numRows.max(0).min(2147483632 - 1)
    val hasMoreData = rows.length - 1 > numRows

    val sb = new StringBuilder
    val numCols = schema.length
    // We set a minimum column width at '3'
    val minimumColWidth = 3

    // Initialise the width of each column to a minimum value
    val colWidths = Array.fill(numCols)(minimumColWidth)

    if (rows.nonEmpty && schema.size != rows.head.length) {
      throw new IllegalArgumentException("schema must be same size as data!")
    }

    val escapedSchema = schema.map(
      org.apache.spark.sql.rapids.tool.util.StringUtils.renderStr(_, doEscapeMetaCharacters = true,
        maxLength = 0))

    val schemaAndData = escapedSchema +: rows.map { row =>
      row.map {
        case null => "null"
        case str: String =>
          // Escapes meta-characters not to break the `showString` format
          org.apache.spark.sql.rapids.tool.util.StringUtils.renderStr(
            str, doEscapeMetaCharacters = true, maxLength = truncate, showEllipses = true)
      }: Seq[String]
    }

    // Compute the width of each column
    for (row <- schemaAndData) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), stringHalfWidth(cell))
      }
    }

    val paddedRows = schemaAndData.map { row =>
      row.zipWithIndex.map { case (cell, i) =>
        if (truncate > 0) {
          StringUtils.leftPad(cell, colWidths(i) - stringHalfWidth(cell) + cell.length)
        } else {
          StringUtils.rightPad(cell, colWidths(i) - stringHalfWidth(cell) + cell.length)
        }
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    paddedRows.head.addString(sb, "|", "|", "|\n")
    sb.append(sep)

    // data
    paddedRows.tail.foreach(_.addString(sb, "|", "|", "|\n"))
    sb.append(sep)


    // Print a footer
    if (hasMoreData) {
      // For Data that has more than "numRows" records
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }
}
