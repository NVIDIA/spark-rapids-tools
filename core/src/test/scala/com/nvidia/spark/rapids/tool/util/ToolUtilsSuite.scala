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

package com.nvidia.spark.rapids.tool.util

import org.scalatest.FunSuite
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.TrampolineUtil
import org.apache.spark.sql.rapids.tool.util.{RapidsToolsConfUtil, WebCrawlerUtil}


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
    val allLinks = WebCrawlerUtil.getPageLinks(baseURL, None).mkString("\n")
    val versionPattern = "(\\d{2}\\.\\d{2}\\.\\d+)/".r
    // get the latest release from the mvn url
    val actualRelease = versionPattern.findAllMatchIn(allLinks).map(_.group(1)).toSeq.sorted.last
    latestRelease shouldBe actualRelease
  }

  test("Hadoop Configuration should load system properties") {
    // Tests that Hadoop configurations can load the system property passed to the
    // command line. i.e., "-Dspark.hadoop.property.key=value"
    TrampolineUtil.cleanupAnyExistingSession()
    // sets a hadoop property through Spark Prefix
    System.setProperty("spark.hadoop.property.key1", "value1")
    lazy val hadoopConf = RapidsToolsConfUtil.newHadoopConf()
    hadoopConf.get("property.key1") shouldBe "value1"
  }
}
