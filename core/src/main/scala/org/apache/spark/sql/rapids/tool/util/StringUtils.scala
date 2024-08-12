/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import scala.concurrent.duration.{DurationDouble, DurationLong}

import org.apache.spark.internal.Logging

/**
 * Utility containing the implementation of helpers used for parsing and/or formatting
 * strings.
 */
object StringUtils extends Logging {
  // Regular expression for duration-format 'H+:MM:SS.FFF'
  // Note: this is not time-of-day. Hours can be larger than 12.
  private val regExDurationFormat = "^(\\d+):([0-5]\\d):([0-5]\\d\\.\\d+)$"
  private val regExMemorySize =
    "^(?i)(\\d+(?:\\.\\d+)?)(b|k(?:ib|b)?|m(?:ib|b)?|g(?:ib|b)?|t(?:ib|b)?|p(?:ib|b)?)$".r
  val SUPPORTED_SIZE_UNITS: Seq[String] = Seq("b", "k", "m", "g", "t", "p")
  /**
   * Checks if the strData is of pattern 'HH:MM:SS.FFF' and return the time as long if applicable.
   * If the string is not in the expected format, the result is None.
   * Note that the hours part can be larger than 23.
   *
   * @param stData the data to be evaluated
   * @return long value of the duration
   */
  def parseFromDurationToLongOption(strData: String): Option[Long] = {
    // String operation is usually faster than regular expression match/find. That's why we check
    // using string instead of `regEx.findFirstMatchIn()`
    if (strData.matches(regExDurationFormat)) {
      val tokens = strData.split(":")
      Some(tokens(0).toLong.hours.toMillis +
        tokens(1).toLong.minutes.toMillis +
        tokens(2).toDouble.seconds.toMillis)
    } else {
      None
    }
  }

  def reformatCSVString(str: String): String = {
    "\"" + str.replace("\"", "\"\"") + "\""
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
   * Process a string based on parameters:
   * - escapes special characters (i.e., \n) if doEscapeMetaCharacters is set to true.
   * - truncates the string if maxLength is a positive integer. It adds ellipses to the end of the
   *   truncated string if showEllipses is set to true.
   * - trim trailing white spaces.
   * @param str the original input string
   * @param doEscapeMetaCharacters when set to true, escapes the special characters. See the full
   *                               list of characters in #escapeMetaCharacters(java.lang.String)
   * @param maxLength maximum length of the output string. Default is 100.
   *                  A 0-value does not truncate the output string.
   * @param showEllipses When set to true and maxLength is >= 0, ellipses will be appended to the
   *                     output string to indicate the string content was truncated.
   * @return a formatted output string based on the input parameters.
   */
  def renderStr(str: String,
      doEscapeMetaCharacters: Boolean,
      maxLength: Int = 100,
      showEllipses: Boolean = false): String = {
    val truncatedStr = if (maxLength > 0) {
      val tmpStr = str.substring(0, Math.min(str.size, maxLength))
      if (showEllipses && tmpStr.length > 4) {
        // do not show ellipses for strings shorter than 4 characters.
        tmpStr + "..."
      } else {
        tmpStr
      }
    } else {
      str
    }
    val escapedStr = if (doEscapeMetaCharacters) {
      escapeMetaCharacters(truncatedStr)
    } else {
      truncatedStr
    }
    // Finally trim the string to remove any trailing spaces.
    escapedStr.trim
  }

  // Convert a null-able String to Option[Long]
  def stringToLong(in: String): Option[Long] = try {
    Some(in.toLong)
  } catch {
    case _: NumberFormatException => None
  }

  def isMemorySize(value: String): Boolean = {
    value.matches(regExMemorySize.regex)
  }

  /**
   * Converts size from human readable to bytes.
   * Eg, "4m" -> 4194304.
   */
  def convertMemorySizeToBytes(value: String): Long = {
    regExMemorySize.findFirstMatchIn(value.toLowerCase) match {
      case None =>
        // try to convert to long
        stringToLong(value) match {
          case Some(num) => num
          case _ =>
            logError(s"Could not convert memorySize input [$value] to Long")
            0L
        }
      case Some(m) =>
        val unitSize = m.group(2).substring(0, 1)
        val sizeNum = m.group(1).toDouble
        (sizeNum * Math.pow(1024, SUPPORTED_SIZE_UNITS.indexOf(unitSize))).toLong
    }
  }

  def convertToMB(value: String): Long = {
    convertMemorySizeToBytes(value) / (1024 * 1024)
  }
}
