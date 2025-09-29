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

package org.apache.spark.sql.rapids.tool.util

import java.io.IOException
import java.net.URL

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.xml.XML

import org.jsoup.Jsoup

import org.apache.spark.internal.Logging


/**
 * Utility to extract information about RAPIDS packages from mvn URLs.
 */
object WebCrawlerUtil extends Logging {
  private val MAX_CRAWLER_DEPTH = 1
  private val NV_MVN_BASE_URL = "https://edge.urm.nvidia.com/artifactory/sw-spark-maven/com/nvidia"
  // defines the artifacts of the RAPIDS libraries
  private val NV_ARTIFACTS_LOOKUP = Map(
    "rapids.plugin" -> "rapids-4-spark_2.12",
    "rapids.tools" -> "rapids-4-spark-tools_2.12"
  )
  private val MAVEN_META_FILE = "maven-metadata.xml"
  // regular expression used to extract the version number from
  // the mvn repository url
  private val ARTIFACT_VERSION_REGEX = "\\d{2}\\.\\d{2}\\.\\d+/"
  // given an artifactID returns the full mvn url that lists all the
  // releases
  def getMVNArtifactURL(artifactID: String) : String = {
    val artifactUrlPart = NV_ARTIFACTS_LOOKUP.getOrElse(artifactID, artifactID)
    s"$NV_MVN_BASE_URL/$artifactUrlPart"
  }

  def getMVNMetaURL(artifactID: String) : String = {
    val artifactUrlPart = getMVNArtifactURL(artifactID)
    s"$artifactUrlPart/$MAVEN_META_FILE"
  }

  /**
   * Given a valid URL, this method recursively picks all the hrefs defined in the HTML doc.
   * @param webURL a valid URL to the html page to be crawled
   * @param regEx a valid regular expression as defined by the jsoup library.
   *              For more details, check https://jsoup.org/cookbook/extracting-data/selector-syntax
   * @param maxDepth the level of recursion to get the links.
   * @return a set of all links found recursively in the given url
   */
  def getPageLinks(
      webURL: String,
      regEx: Option[String],
      maxDepth: Int = MAX_CRAWLER_DEPTH): mutable.Set[String] = {
    def removeDefaultPorts(rawLink: String): String = {
      val jURL: URL = new URL(rawLink)
      jURL.getProtocol match {
        case "http" if jURL.getPort == 80 =>
          rawLink.replace(":80", "")
        case "https" if jURL.getPort == 443 =>
          rawLink.replace(":443", "")
        case _ => rawLink
      }
    }
    def processPageLinksInternal(currURL: String,
        cssQuery: String,
        currDepth: Int,
        allLinks: mutable.Set[String]): Unit = {
      if (currDepth < maxDepth && !allLinks.contains(currURL)) {
        try {
          val doc = Jsoup.connect(currURL).get
          val pageURLs = doc.select(cssQuery).asScala.toList
          val newDepth = currDepth + 1
          for (page <- pageURLs) {
            val newLink = page.attr("abs:href")
            // remove the default port if any
            allLinks.add(removeDefaultPorts(newLink))
            processPageLinksInternal(newLink, cssQuery, newDepth, allLinks)
          }
        } catch {
          case x: IOException =>
            logWarning(s"Exception while visiting webURL $webURL: ${x.toString}")
        }
      }
    }

    val selectorQuery = regEx match {
      case Some(value) => s"a[href~=$value]"
      case None => "a[href]"
    }
    val links = mutable.Set[String]()
    processPageLinksInternal(webURL, selectorQuery, 0, links)
    links
  }

  // given an artifactID, returns a list of strings containing all
  // available releases.
  def getMvnReleasesForNVPackage(artifactID: String): Seq[String] = {
    val mvnURL = getMVNArtifactURL(artifactID)
    val definedLinks = getPageLinks(mvnURL, Some(ARTIFACT_VERSION_REGEX)).toSeq.sorted
    definedLinks.map(_.split("/").last)
  }

  // given an artifactID, will return the latest version if any
  def getLatestMvnReleaseForNVPackage(artifactID: String): Option[String] = {
    // Reads maven-metadata.xml file to extract the latest version
    val mvnMetaFile = getMVNMetaURL(artifactID)
    try {
      val xml = XML.load(mvnMetaFile)
      Some((xml \\ "metadata" \ "versioning" \ "latest").text)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Exception loading maven-metadata.xml: ${mvnMetaFile}", e)
      None
    }
  }

  // given artifactID and release, returns the full mvn url to download the jar
  def getMvnDownloadLink(artifactID: String, release: String): String = {
    s"${getMVNArtifactURL(artifactID)}/$release/$artifactID-$release.jar"
  }

  def getPluginMvnDownloadLink(release: String): String = {
    getMvnDownloadLink(NV_ARTIFACTS_LOOKUP("rapids.plugin"), release)
  }

  // get the latest version available for rapids plugin
  def getLatestPluginRelease: Option[String] = {
    getLatestMvnReleaseForNVPackage(NV_ARTIFACTS_LOOKUP("rapids.plugin"))
  }

  // get the latest version available for rapids tools
  def getLatestToolsRelease: Option[String] = {
    getLatestMvnReleaseForNVPackage(NV_ARTIFACTS_LOOKUP("rapids.tools"))
  }
}
