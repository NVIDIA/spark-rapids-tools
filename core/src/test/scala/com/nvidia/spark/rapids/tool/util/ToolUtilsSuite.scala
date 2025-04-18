/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.util

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.concurrent.duration._
import scala.xml.XML

import com.nvidia.spark.rapids.tool.profiling.{ProfileOutputWriter, ProfileResult}
import org.scalatest.AppendedClues.convertToClueful
import org.scalatest.FunSuite
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal, not}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.TrampolineUtil
import org.apache.spark.sql.rapids.tool.util.{FSUtils, InPlaceMedianArrView, RapidsToolsConfUtil, StringUtils, WebCrawlerUtil}

class ToolUtilsSuite extends FunSuite with Logging {
  test("get page links of a url") {
    // Tests that getPageLinks return all the [href] in a page.
    // This is done manually by checking against a URL that won't likely change
    // (aka. an old release page should be stable).
    val baseURL = "https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12"
    val version = "23.04.0"
    val filePrefix = s"rapids-4-spark_2.12-$version"
    val mvnURL = s"$baseURL/$version"

    val webURL = mvnURL
    val allLinks = WebCrawlerUtil.getPageLinks(webURL, None)
    val expected = Set[String](
      s"$baseURL/",
      s"$mvnURL/$filePrefix-cuda11.jar.asc.sha1",
      s"$mvnURL/$filePrefix-javadoc.jar.asc.md5",
      s"$mvnURL/$filePrefix-sources.jar.md5",
      s"$mvnURL/$filePrefix-cuda11.jar.sha1",
      s"$mvnURL/$filePrefix-javadoc.jar.asc.sha1",
      s"$mvnURL/$filePrefix-javadoc.jar.sha1",
      s"$mvnURL/$filePrefix-sources.jar",
      s"$mvnURL/$filePrefix.jar.asc",
      s"$mvnURL/$filePrefix-sources.jar.sha1",
      s"$mvnURL/$filePrefix.jar.sha1",
      s"$mvnURL/$filePrefix.pom.asc.md5",
      s"$mvnURL/$filePrefix-sources.jar.asc.sha1",
      s"$mvnURL/$filePrefix-javadoc.jar",
      s"$mvnURL/$filePrefix.jar",
      s"$mvnURL/$filePrefix.jar.md5",
      s"$mvnURL/$filePrefix.jar.asc.sha1",
      s"$mvnURL/$filePrefix.jar.asc.md5",
      s"$mvnURL/$filePrefix.pom.asc.sha1",
      s"$mvnURL/$filePrefix.pom.md5",
      s"$mvnURL/$filePrefix.pom.sha1",
      s"$mvnURL/$filePrefix-javadoc.jar.asc",
      s"$mvnURL/$filePrefix-cuda11.jar.asc",
      s"$mvnURL/$filePrefix-cuda11.jar.asc.md5",
      s"$mvnURL/$filePrefix-cuda11.jar",
      s"$mvnURL/$filePrefix-javadoc.jar.md5",
      s"$mvnURL/$filePrefix-cuda11.jar.md5",
      s"$mvnURL/$filePrefix-sources.jar.asc",
      s"$mvnURL/$filePrefix-sources.jar.asc.md5",
      s"$mvnURL/$filePrefix.pom.asc",
      s"$mvnURL/$filePrefix.pom")
    // all links should be matching
    allLinks shouldBe expected
  }

  // checks that regex is used correctly to filter the href pulled from a given url
  test("get page links of a url with regex") {
    // see the list of available regex in https://jsoup.org/cookbook/extracting-data/selector-syntax
    val baseURL = "https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12"
    val version = "23.04.0"
    val filePrefix = s"rapids-4-spark_2.12-$version"
    val mvnURL = s"$baseURL/$version"

    val webURL = mvnURL
    val jarFileRegEx = ".*\\.jar$"
    val allLinks = WebCrawlerUtil.getPageLinks(webURL, Some(jarFileRegEx))
    val expected = Set[String](
      s"$mvnURL/$filePrefix-cuda11.jar",
      s"$mvnURL/$filePrefix.jar",
      s"$mvnURL/$filePrefix-javadoc.jar",
      s"$mvnURL/$filePrefix-sources.jar"
    )
    // all links should end with jar files
    allLinks shouldBe expected
  }

  //
  test("list available mvn releases") {
    // use mvn repo url got testing
    val artifactID = "rapids-4-spark_2.12"
    val baseURL = "https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12"
    val nvReleases = WebCrawlerUtil.getMvnReleasesForNVPackage(artifactID)
    // get all the links on the page
    val allLinks = WebCrawlerUtil.getPageLinks(baseURL, None).mkString("\n")
    val versionPattern = "(\\d{2}\\.\\d{2}\\.\\d+)/".r
    val actualVersions = versionPattern.findAllMatchIn(allLinks).map(_.group(1)).toSeq
    nvReleases should contain theSameElementsAs actualVersions
  }

  test("get latest release") {
    // use mvn repo url got testing
    val artifactID = "rapids-4-spark_2.12"
    val baseURL = "https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12"
    val latestRelease = WebCrawlerUtil.getLatestMvnReleaseForNVPackage(artifactID) match {
      case Some(v) => v
      case None => fail("Could not find pull the latest release successfully")
    }
    // get all the links on the page
    val mavenMetaXml = XML.load(s"$baseURL/maven-metadata.xml")
    val allVersions = (mavenMetaXml \\ "metadata" \ "versioning" \ "versions" \ "version").toList
    // get the latest release from the mvn url
    val actualRelease = allVersions.last.text
    actualRelease.matches("\\d{2}\\.\\d{2}\\.\\d+") shouldBe true
    latestRelease shouldBe actualRelease
  }

  test("Hadoop Configuration should load system properties") {
    // Tests that Hadoop configurations can load the system property passed to the
    // command line. i.e., "-Drapids.tools.hadoop.property.key=value"
    TrampolineUtil.cleanupAnyExistingSession()
    // sets a hadoop property through Rapids-Tools prefix
    System.setProperty("rapids.tools.hadoop.property.key1", "value1")
    lazy val hadoopConf = RapidsToolsConfUtil.newHadoopConf()
    hadoopConf.get("property.key1") shouldBe "value1"
    System.clearProperty("rapids.tools.hadoop.property.key1")
  }

  test("parse timeFormat 'HH:MM:SS.FFF' to Long") {
    val currCalendar = Calendar.getInstance()
    val acceptedTimeFormat = new SimpleDateFormat("HH:mm:ss.SSS")

    val hour = currCalendar.get(Calendar.HOUR_OF_DAY)
    val minute = currCalendar.get(Calendar.MINUTE)
    val seconds = currCalendar.get(Calendar.SECOND)
    val millis = currCalendar.get(Calendar.MILLISECOND)
    val currTimeAsStr = acceptedTimeFormat.format(currCalendar.getTime)
    val duration =
      hour.hours.toMillis + minute.minutes.toMillis + seconds.seconds.toMillis + millis
    // test successful parsing
    StringUtils.parseFromDurationToLongOption(currTimeAsStr).get shouldBe duration
    // test non-successful with milliseconds is not 3 digits
    val timeNoMillisAsStr = f"$hour:$minute%02d:$seconds%02d.1"
    StringUtils.parseFromDurationToLongOption(timeNoMillisAsStr).get shouldBe {
      duration - millis + 100
    }
    // test successful with high number of hours
    StringUtils.parseFromDurationToLongOption("50:30:30.000").get shouldBe {
      50.hours.toMillis + 30.minutes.toMillis + 30.seconds.toMillis
    }
    // test incorrect format with overflowing minutes
    val timeBrokenMinutesAsString = f"$hour:${minute + 60}%02d:$seconds%02d.$millis%03d"
    StringUtils.parseFromDurationToLongOption(timeBrokenMinutesAsString) should not be 'defined
    // test random string won't cause any exceptions
    StringUtils.parseFromDurationToLongOption("Hello Worlds") should not be 'defined
  }

  test("output non-english characters") {
    val nonEnglishString = "你好"
    TrampolineUtil.withTempDir { tempDir =>
      val filePrefix = "non-english"
      val tableHeader = "Non-English"
      val textFilePath = s"${tempDir.getAbsolutePath}/$filePrefix.log"
      val csvFilePath = s"${tempDir.getAbsolutePath}/$filePrefix.csv"
      val profOutputWriter =
        new ProfileOutputWriter(tempDir.getAbsolutePath, filePrefix, 1000, outputCSV = true)
      val profResults = Seq(
        MockProfileResults("appID-0", nonEnglishString, Seq(1, 2, 3).mkString(","))
      )
      try {
        profOutputWriter.write(tableHeader, profResults)
      } finally {
        profOutputWriter.close()
      }
      val csvFile = new File(csvFilePath)
      val textFile = new File(textFilePath)
      assert(csvFile.exists())
      assert(textFile.exists())
      val expectedCSVFileContent =
        s"""appID,nonEnglishField,parentIDs
           |appID-0,"你好","1,2,3"""".stripMargin
      val expectedTXTContent =
        s"""
           |Non-English:
           |+-------+---------------+---------+
           ||appID  |nonEnglishField|parentIDs|
           |+-------+---------------+---------+
           ||appID-0|你好           |1,2,3    |
           |+-------+---------------+---------+""".stripMargin
      val actualCSVContent = FSUtils.readFileContentAsUTF8(csvFilePath)
      val actualTXTContent = FSUtils.readFileContentAsUTF8(textFilePath)
      actualCSVContent should equal (expectedCSVFileContent)
      actualTXTContent should equal (expectedTXTContent)
    }
  }

  test("Finding median of arrays") {
    val testSet: Map[String, (Array[Long], Long)] = Map(
      "All same values" -> (Array[Long](5, 5, 5, 5) -> 5L),
      "Odd number of values [9, 7, 5, 3, 1]" -> (Array[Long](9, 7, 5, 3, 1) -> 5L),
      "Even number of values [11, 9, 7, 5, 3, 1]" -> (Array[Long](11, 9, 7, 5, 3, 1) -> 6),
      "Even number of values(2) [15, 13, 11, 9, 7, 5, 3, 1]" ->
        (Array[Long](15, 13, 11, 9, 7, 5, 3, 1) -> 8),
      "Even number of values(3) [3, 13, 11, 9, 7, 5, 15, 1]" ->
        (Array[Long](3, 13, 11, 9, 7, 5, 15, 1) -> 8),
      "Single element" -> (Array[Long](1) -> 1),
      "Two elements" -> (Array[Long](1, 2).reverse -> 1)
    )
    for ((desc, (arr, expectedMedian)) <- testSet) {
      val actualMedian =
        InPlaceMedianArrView.findMedianInPlace(arr)(InPlaceMedianArrView.chooseMidpointPivotInPlace)
      actualMedian shouldBe expectedMedian withClue s"Failed for $desc. " +
        s"Expected: $expectedMedian, " +
        s"Actual: $actualMedian"
    }
  }

  case class MockProfileResults(appID: String, nonEnglishField: String,
      parentIDs: String) extends ProfileResult {
    override val outputHeaders: Array[String] = Array("appID", "nonEnglishField",
      "parentIDs")

    override def convertToSeq(): Array[String] = {
      Array(appID, nonEnglishField, parentIDs)
    }

    override def convertToCSVSeq(): Array[String] = {
      Array(appID, StringUtils.reformatCSVString(nonEnglishField),
        StringUtils.reformatCSVString(parentIDs))
    }
  }
}
