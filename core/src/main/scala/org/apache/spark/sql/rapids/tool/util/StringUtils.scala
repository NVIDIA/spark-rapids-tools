/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

/**
 * Utility containing the implementation of helpers used for parsing and/or formatting
 * strings.
 */
object StringUtils {
  // Regular expression for duration-format 'H+:MM:SS.FFF'
  // Note: this is not time-of-day. Hours can be larger than 12.
  private val regExDurationFormat = "^(\\d+):([0-5]\\d):([0-5]\\d\\.\\d+)$"

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
}
