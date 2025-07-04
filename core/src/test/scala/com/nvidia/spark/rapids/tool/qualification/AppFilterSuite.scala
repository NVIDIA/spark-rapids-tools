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

package com.nvidia.spark.rapids.tool.qualification

import java.util.Calendar

import com.nvidia.spark.rapids.BaseNoSparkSuite
import com.nvidia.spark.rapids.tool.{EventlogContentGenMeta, EventlogProviderImpl, StatusReportCounts}
import com.nvidia.spark.rapids.tool.qualification.checkers.{QToolResultCoreChecker, QToolStatusChecker, QToolTestCtxtBuilder}

import org.apache.spark.sql.rapids.tool.AppFilterImpl

class AppFilterSuite extends BaseNoSparkSuite {

  test("illegal args") {
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("0"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("1hd"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("1yr"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("-1d"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("0m"))
  }

  test("time period minute parsing") {
    testTimePeriod(msMinAgo(6), "10min")
  }

  test("time period hour parsing") {
    testTimePeriod(msHoursAgo(10), "14h")
  }

  test("time period day parsing") {
    testTimePeriod(msDaysAgo(40), "41d")
  }

  test("time period day parsing default") {
    testTimePeriod(msDaysAgo(5), "6")
  }

  test("time period week parsing") {
    testTimePeriod(msWeeksAgo(2), "3w")
  }

  test("time period month parsing") {
    testTimePeriod(msMonthsAgo(8), "10m")
  }

  test("time period minute parsing fail") {
    testTimePeriod(msMinAgo(16), "10min", failFilter = true)
  }

  test("time period hour parsing fail") {
    testTimePeriod(msHoursAgo(10), "8h", failFilter = true)
  }

  test("time period day parsing fail") {
    testTimePeriod(msDaysAgo(40), "38d", failFilter = true)
  }

  test("time period week parsing fail") {
    testTimePeriod(msWeeksAgo(2), "1w", failFilter = true)
  }

  test("time period month parsing fail") {
    testTimePeriod(msMonthsAgo(8), "7m", failFilter = true)
  }

  private def testTimePeriod(eventLogTime: Long, startTimePeriod: String,
      failFilter: Boolean = false): Unit = {
    val (resSize, statusCount) = if (failFilter) {
      // Status counts: 0 SUCCESS, 0 FAILURE, 0 SKIPPED, 0 UNKNOWN
      // The status should be empty
      (0, StatusReportCounts(0, 0, 0, 0))
    } else {
      // Status counts: 1 SUCCESS, 0 FAILURE, 0 SKIPPED, 0 UNKNOWN
      (1, StatusReportCounts(1, 0, 0, 0))
    }
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with text generator")
          .withContentGenerator { _ =>
            // scalastyle:off line.size.limit
            val applicationName = "Spark shell"
            val applicationID = "local-1626104300434"
            val fileContent =
              s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
                 |{"Event":"SparkListenerApplicationStart","App Name":"$applicationName","App ID":"$applicationID","Timestamp":$eventLogTime,"User":"user1"}""".stripMargin
            // scalastyle:on line.size.limit
            EventlogContentGenMeta(applicationName = applicationName,
              applicationId = applicationID, evLogCont = fileContent)
          })
      .withToolArgs(Array("--start-app-time", startTimePeriod))
      .withChecker(
        QToolResultCoreChecker("check app count and potential problems")
          .withExpectedSize(resSize)
          .withSuccessCode())
      .withChecker(
        QToolStatusChecker("check the status is as expected")
          .withExpectedCounts(statusCount))
      .build()
  }

  private def msMonthsAgo(months: Int): Long = {
    val c = Calendar.getInstance
    c.add(Calendar.MONTH, -months)
    c.getTimeInMillis
  }

  private def msWeeksAgo(weeks: Int): Long = {
    val c = Calendar.getInstance
    c.add(Calendar.WEEK_OF_YEAR, -weeks)
    c.getTimeInMillis
  }

  private def msDaysAgo(days: Int): Long = {
    val c = Calendar.getInstance
    c.add(Calendar.DATE, -days)
    c.getTimeInMillis
  }

  private def msMinAgo(mins: Int): Long = {
    val c = Calendar.getInstance
    c.add(Calendar.MINUTE, -mins)
    c.getTimeInMillis
  }

  private def msHoursAgo(hours: Int): Long = {
    val c = Calendar.getInstance
    c.add(Calendar.HOUR, -hours)
    c.getTimeInMillis
  }

  val appsToTest: Array[TestEventLogInfo] =
    Array(TestEventLogInfo("ndshours18", msHoursAgo(18), 1),
      TestEventLogInfo("ndsweeks2", msWeeksAgo(2), 2),
      TestEventLogInfo("ndsmonths4", msMonthsAgo(5), 3),
      TestEventLogInfo("ndsdays3", msDaysAgo(3), 4),
      TestEventLogInfo("ndsmins34", msMinAgo(34), 5),
      TestEventLogInfo("nds86", msDaysAgo(4), 6),
      TestEventLogInfo("nds86", msWeeksAgo(2), 7),
      TestEventLogInfo("otherapp", msWeeksAgo(2), 8))

  test("app name and start time 20m") {
    testTimePeriodAndStart(appsToTest, "20m", "nds", appsToTest.length - 1)
  }

  test("app name no matche and start time 20m") {
    testTimePeriodAndStart(appsToTest, "20m", "nomatch", 0)
  }

  test("app name and start time 2d") {
    testTimePeriodAndStart(appsToTest, "2d", "nds", 2)
  }

  test("app name and start time small") {
    testTimePeriodAndStart(appsToTest, "1min", "nds", 0)
  }

  test("app name exact and start time 2d") {
    testTimePeriodAndStart(appsToTest, "2d", "ndsmins34", 1)
  }

  test("app name and start time 20h") {
    testTimePeriodAndStart(appsToTest, "20h", "nds", 2)
  }

  test("app name exact and start time 6d") {
    testTimePeriodAndStart(appsToTest, "6d", "nds86", 1)
  }

  case class TestEventLogInfo(appName: String, eventLogTime: Long, uniqueId: Int)

  private def testTimePeriodAndStart(apps: Array[TestEventLogInfo],
      startTimePeriod: String, filterAppName: String, expectedFilterSize: Int): Unit = {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with text generator")
          .withContentGenerators(
            apps.map { app =>
              (_: EventlogProviderImpl) => {
                val applicationName = app.appName
                val applicationId = "local-16261043-" + app.uniqueId
                // scalastyle:off line.size.limit
                val fileContent =
                  s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
                     |{"Event":"SparkListenerApplicationStart","App Name":"$applicationName","App ID":"$applicationId","Timestamp":${app.eventLogTime},"User":"user1"}""".stripMargin
                // scalastyle:on line.size.limit
                EventlogContentGenMeta(applicationName, applicationId, fileContent)
              }}))
      .withToolArgs(
        Array("--start-app-time", startTimePeriod,
          "--application-name", filterAppName))
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(expectedFilterSize)
          .withSuccessCode())
      .build()
  }

  case class TestEventLogFSAndAppNameInfo(appName: String, fsTime: Long, uniqueId: Int)

  private val appsWithFsToTest = Array(
    TestEventLogFSAndAppNameInfo("ndshours18", msHoursAgo(18), 1),
    TestEventLogFSAndAppNameInfo("ndsweeks2", msWeeksAgo(2), 2),
    TestEventLogFSAndAppNameInfo("nds86", msDaysAgo(4), 3),
    TestEventLogFSAndAppNameInfo("nds86", msWeeksAgo(2), 4))

  test("app name exact and fs 10-newest-filesystem") {
    testFileSystemTimeAndStart(appsWithFsToTest, "10-newest-filesystem", "nds86", 2)
  }

  test("app name exact and 2-oldest-filesystem") {
    testFileSystemTimeAndStart(appsWithFsToTest, "2-oldest-filesystem", "ndsweeks2", 1)
  }

  private val appsWithFsNewOlderToTest = Array(
    TestEventLogFSAndAppNameInfo("feb01-00",
      AppFilterImpl.parseDateTimePeriod("2024-02-01 00:00:00").get, 1),
    TestEventLogFSAndAppNameInfo("feb02-07",
      AppFilterImpl.parseDateTimePeriod("2024-02-02 07:12:11").get, 2),
    TestEventLogFSAndAppNameInfo("feb02-15",
      AppFilterImpl.parseDateTimePeriod("2024-02-02 15:00:00").get, 3),
    TestEventLogFSAndAppNameInfo("may15-13",
      AppFilterImpl.parseDateTimePeriod("2024-05-15 13:00:00").get, 4))

  test("start and end filesystem both on all") {
    testFileSystemNewerAndOlderTimes(appsWithFsNewOlderToTest, "2024-02-01 00:00:00",
      "2024-05-15 13:00:00", 4)
  }

  test("start and end filesystem both on some") {
    testFileSystemNewerAndOlderTimes(appsWithFsNewOlderToTest, "2024-02-00 10:00:00",
      "2024-02-02 13:00:00", 2)
  }

  test("start filesystem some exact") {
    testFileSystemNewerAndOlderTimes(appsWithFsNewOlderToTest, "2024-02-02 07:12:11",
      "", 3)
  }

  test("start filesystem none") {
    testFileSystemNewerAndOlderTimes(appsWithFsNewOlderToTest, "2024-05-15 15:00:00",
      "", 0)
  }
  test("end filesystem some exact") {
    testFileSystemNewerAndOlderTimes(appsWithFsNewOlderToTest, "2024-02-02 15:00:00",
      "", 2)
  }

  test("end filesystem none") {
    testFileSystemNewerAndOlderTimes(appsWithFsNewOlderToTest, "",
      "2024-01-01 00:00:00", 0)
  }

  private def testFileSystemTimeAndStart(apps: Array[TestEventLogFSAndAppNameInfo],
      filterCriteria: String, filterAppName: String, expectedFilterSize: Int): Unit = {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with text generator")
          .withContentGenerators(
            apps.map { app =>
              (_: EventlogProviderImpl) => {
                val applicationName = app.appName
                val applicationId = "local-16261043-" + app.uniqueId
                // scalastyle:off line.size.limit
                val fileContent =
                  s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
                     |{"Event":"SparkListenerApplicationStart","App Name":"$applicationName","App ID":"$applicationId","Timestamp":1626104299853,"User":"user1"}""".stripMargin
                // scalastyle:on line.size.limit
                EventlogContentGenMeta(
                  applicationName, applicationId, fileContent, Some(app.fsTime))
              }}))
      .withToolArgs(
        Array("--filter-criteria", filterCriteria,
          "--application-name", filterAppName))
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(expectedFilterSize)
          .withSuccessCode())
      .build()
  }

  private def testFileSystemNewerAndOlderTimes(
      apps: Array[TestEventLogFSAndAppNameInfo],
      fsStartTime: String,
      fsEndTime: String,
      expectedFilterSize: Int): Unit = {
    val startTimeArgs = if (fsStartTime.nonEmpty) {
      Array("--fs-start-time", fsStartTime)
    } else {
      Array.empty[String]
    }
    val endTimeArgs = if (fsEndTime.nonEmpty) {
      Array("--fs-end-time", fsEndTime)
    } else {
      Array.empty[String]
    }
    val filterArgs = Array() ++ startTimeArgs ++ endTimeArgs
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with text generator")
          .withContentGenerators(
            apps.map { app =>
              (_: EventlogProviderImpl) => {
                val applicationName = app.appName
                val applicationId = "local-16261043-" + app.uniqueId
                // scalastyle:off line.size.limit
                val fileContent =
                  s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
                     |{"Event":"SparkListenerApplicationStart","App Name":"$applicationName","App ID":"$applicationId","Timestamp":1626104299853,"User":"user1"}""".stripMargin
                // scalastyle:on line.size.limit
                EventlogContentGenMeta(
                  applicationName, applicationId, fileContent, Some(app.fsTime))
              }}))
      .withToolArgs(filterArgs)
      .withChecker(
        QToolResultCoreChecker("check app count and potential problems")
          .withExpectedSize(expectedFilterSize)
          .withSuccessCode())
      .build()
  }

  case class TestEventLogFSAndAppInfo(fileName: String, fsTime: Long, appName: String,
    appTime: Long, uniqueId: Int)

  private val appsFullWithFsToTest = Array(
    TestEventLogFSAndAppInfo("app-ndshours18", msHoursAgo(16), "ndshours18", msHoursAgo(18), 1),
    TestEventLogFSAndAppInfo("app-ndsweeks2", msWeeksAgo(2), "ndsweeks2", msWeeksAgo(2), 2),
    TestEventLogFSAndAppInfo("app-nds86-1", msDaysAgo(3), "nds86", msDaysAgo(4), 3),
    TestEventLogFSAndAppInfo("app-nds86-2", msDaysAgo(13), "nds86", msWeeksAgo(2), 4))

  test("full app name exact and fs 10-newest-filesystem 6 days") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "10-newest-filesystem",
      "nds86", "nds86", "6d", 1)
  }

  test("full app name exact and 2-oldest-filesystem no match from app start") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "2-oldest-filesystem",
      "ndsweeks2", "nds", "6d", 0)
  }

  test("full app name exact and 2-oldest-filesystem") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "2-oldest-filesystem",
      "ndsweeks2", "nds", "3w", 1)
  }

  test("full app name exact and 2-oldest-filesystem no match from filename") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "2-oldest-filesystem",
      "nds", "nomatch", "3w", 0)
  }

  test("full 2-oldest-filesystem no match from app name") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "2-oldest-filesystem",
      "nomatch", "nds", "3w", 0)
  }

  test("full app name exact and 10-oldest-filesystem and 3w") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "10-oldest-filesystem",
      "nds86", "app-nds86", "3w", 2)
  }

  test("app name and 2-oldest by app time no match from filename") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "2-oldest",
      "nds", "nomatch", "3w", 0)
  }

  test("app name and 2-oldest by app time") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "2-oldest",
      "nds", "nds", "3w", 2)
  }

  test("app name and 2-newest by app time") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "2-newest",
      "ndsweeks2", "nds", "3w", 1)
  }

  test("app name and 1-newest-per-app-name") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "1-newest-per-app-name",
      "nds", "nds", "3w", 3)
  }

  test("app name and 10-newest-per-app-name") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "10-oldest-per-app-name",
      "nds", "nds", "3w", 4)
  }

  test("app name and 1-newest-per-app-name no match from filename") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "1-newest-per-app-name", "nds",
      "nomatch", "3w", 0)
  }

  private def testFileSystemTimeAndStartAndAppFull(apps: Array[TestEventLogFSAndAppInfo],
      filterCriteria: String, filterAppName: String, matchFileName: String,
      startTimePeriod: String, expectedFilterSize: Int): Unit = {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with text generator")
          .withContentGenerators(
            apps.map { app =>
              (_: EventlogProviderImpl) => {
                val applicationName = app.appName
                val applicationId = s"local-16261043003${app.uniqueId}"
                // scalastyle:off line.size.limit
                val fileContent =
                  s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
                     |{"Event":"SparkListenerApplicationStart","App Name":"${app.appName}","App ID":"$applicationId","Timestamp":${app.appTime},"User":"user1"}""".stripMargin
                // scalastyle:on line.size.limit
                // for this test, we want the eventlog to be named after the filename.
                EventlogContentGenMeta(applicationName, app.fileName, fileContent,
                  Some(app.fsTime))
              }}))
      .withToolArgs(
        Array("--filter-criteria", filterCriteria,
          "--application-name", filterAppName,
          "--start-app-time", startTimePeriod,
          "--match-event-logs", matchFileName))
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(expectedFilterSize)
          .withSuccessCode())
      .build()
  }

  private val appsWithAppNameCriteriaToTest = Array(
    TestEventLogFSAndAppInfo("app-ndshours18", msHoursAgo(16), "ndshours18", msHoursAgo(18), 1),
    TestEventLogFSAndAppInfo("app-ndsweeks-1", msWeeksAgo(1), "ndsweeks", msWeeksAgo(1), 2),
    TestEventLogFSAndAppInfo("app-ndsweeks-2", msWeeksAgo(2), "ndsweeks", msWeeksAgo(2), 3),
    TestEventLogFSAndAppInfo("app-nds86-1", msDaysAgo(3), "nds86", msDaysAgo(4), 4),
    TestEventLogFSAndAppInfo("app-nds86-2", msDaysAgo(13), "nds86", msWeeksAgo(2), 5),
    TestEventLogFSAndAppInfo("app-nds86-3", msDaysAgo(18), "nds86", msWeeksAgo(3), 6))

  test("standalone 1-oldest-per-app-name") {
    val expected = Array(
      ("ndshours18", "local-162610430031"),
      ("ndsweeks", "local-162610430033"),
      ("nds86", "local-162610430036"))
    testAppFilterCriteriaAndPerAppName(appsWithAppNameCriteriaToTest, "1-oldest-per-app-name",
      3, expected)
  }

  test("standalone 2-newest-per-app-name") {
    val expected = Array(
      ("ndshours18", "local-162610430031"),
      ("ndsweeks", "local-162610430032"),
      ("ndsweeks", "local-162610430033"),
      ("nds86", "local-162610430034"),
      ("nds86", "local-162610430035"))
    testAppFilterCriteriaAndPerAppName(appsWithAppNameCriteriaToTest, "2-newest-per-app-name",
      5, expected)
  }

  test("standalone 2-newest based on app time") {
    val expected = Array(
      ("ndshours18", "local-162610430031"),
      ("nds86", "local-162610430034"))
    testAppFilterCriteriaAndPerAppName(appsWithAppNameCriteriaToTest,
      "2-newest", 2, expected)
  }

  test("standalone 10-oldest based on app time") {
    val expected = Array(
      ("ndshours18", "local-162610430031"),
      ("ndsweeks", "local-162610430032"),
      ("ndsweeks", "local-162610430033"),
      ("nds86", "local-162610430034"),
      ("nds86", "local-162610430035"),
      ("nds86", "local-162610430036"))
    testAppFilterCriteriaAndPerAppName(appsWithAppNameCriteriaToTest, "10-oldest", 6, expected)
  }

  private def testAppFilterCriteriaAndPerAppName(
      apps: Array[TestEventLogFSAndAppInfo],
      filterCriteria: String, expectedFilterSize: Int,
      expectedAppName: Array[(String, String)]): Unit = {

    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with text generator")
          .withContentGenerators(
            apps.map { app =>
              (_: EventlogProviderImpl) => {
                val applicationName = app.appName
                val applicationId = s"local-16261043003${app.uniqueId}"
                // scalastyle:off line.size.limit
                val fileContent =
                  s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
                     |{"Event":"SparkListenerApplicationStart","App Name":"$applicationName","App ID":"$applicationId","Timestamp":${app.appTime},"User":"user1"}""".stripMargin
                // scalastyle:on line.size.limit
                EventlogContentGenMeta(applicationName, applicationId, fileContent)
              }}))
      .withToolArgs(
        Array("--filter-criteria", filterCriteria))
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(expectedFilterSize)
          .withSuccessCode()
          .withCheckBlock(
            "check the appName, IDS are correct",
            qRes => {
              val reasultAppNames = qRes.appSummaries.map(x => (x.appName, x.appId)).toArray
              reasultAppNames sameElements expectedAppName
            }))
      .build()
  }

  case class TestRegexAppNameAndUserName(fileName: String, fsTime: Long, appName: String,
      appTime: Long, uniqueId: Int, userName: String)

  private val appsWithAppNameRegexAndUserNameToTest = Array(
    TestRegexAppNameAndUserName("app-ndshours18", msHoursAgo(16), "ndshours18",
      msHoursAgo(18), 1, "user1"),
    TestRegexAppNameAndUserName("app-ndsweeks-1", msWeeksAgo(1), "ndsweeks",
      msWeeksAgo(1), 2, "user1"),
    TestRegexAppNameAndUserName("app-ndsweeks-2", msWeeksAgo(2), "ndsweeks",
      msWeeksAgo(2), 3, "user2"),
    TestRegexAppNameAndUserName("app-ndsweeks-3", msWeeksAgo(3), "Ndsweeks",
      msWeeksAgo(3), 4, "user3"),
    TestRegexAppNameAndUserName("app-nds86-1", msDaysAgo(3), "nds86", msDaysAgo(4), 5, "user1"),
    TestRegexAppNameAndUserName("app-nds86-2", msDaysAgo(13), "Nds86", msWeeksAgo(2), 6, "user2"),
    TestRegexAppNameAndUserName("app-nds86-3", msDaysAgo(18), "nds86", msWeeksAgo(3), 7, "user3"))

  test("App Name Regex match with all user name") {
    testAppNameRegexAndUserName(appsWithAppNameRegexAndUserNameToTest,
      "10-newest", "[Nn].*", "user", "all", 7)
  }

  test("App Name Regex match with user name match") {
    testAppNameRegexAndUserName(appsWithAppNameRegexAndUserNameToTest,
      "10-newest", "[Nn].*", "user3", "all", 2)
  }

  test("App Name Regex exclude with user name match") {
    testAppNameRegexAndUserName(appsWithAppNameRegexAndUserNameToTest,
      "10-newest", "[^Nn].*", "user3", "all", 0)
  }

  test("App Name partial with username match") {
    testAppNameRegexAndUserName(appsWithAppNameRegexAndUserNameToTest,
      "5-newest", "nds", "user1", "all", 3)
  }

  test("Filter only on username match") {
    testAppNameRegexAndUserName(appsWithAppNameRegexAndUserNameToTest,
      "nomatch", "nomatch", "user3", "username", 2)

  }

  private def testAppNameRegexAndUserName(
      apps: Array[TestRegexAppNameAndUserName],
      filterCriteria: String, filterAppName: String, userName: String,
      filterArgs: String, expectedFilterSize: Int): Unit = {
    val allArgs = if (filterArgs.endsWith("all")) {
      Array(
        "--filter-criteria",
        filterCriteria,
        "--application-name",
        filterAppName,
        "--user-name",
        userName
      )
    } else {
      Array("--user-name", userName)
    }
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with text generator")
          .withContentGenerators(
            apps.map { app =>
              (_: EventlogProviderImpl) => {
                val applicationName = app.appName
                val applicationId = s"local-16261043003${app.uniqueId}"
                // scalastyle:off line.size.limit
                val fileContent =
                  s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
                     |{"Event":"SparkListenerApplicationStart","App Name":"$applicationName","App ID":"$applicationId","Timestamp":${app.appTime},"User":"${app.userName}"}""".stripMargin
                // scalastyle:on line.size.limit
                EventlogContentGenMeta(applicationName, applicationId, fileContent)
              }}))
      .withToolArgs(allArgs)
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(expectedFilterSize)
          .withSuccessCode())
      .build()
  }

  case class TestConjunctionAndDisjunction(
      fileName: String, fsTime: Long, appName: String,
      appTime: Long, uniqueId: Int, userName: String)

  private val appsNameConjunctionAndDisjunctionToTest = Array(
    TestConjunctionAndDisjunction("app-ndshours18", msHoursAgo(16), "Ndshours18",
      msHoursAgo(18), 1, "user1"),
    TestConjunctionAndDisjunction("app-Ndsweeks-1", msWeeksAgo(1), "ndsweeks",
      msWeeksAgo(1), 2, "user1"),
    TestConjunctionAndDisjunction("app-ndsweeks-2", msWeeksAgo(2), "Ndsweeks",
      msWeeksAgo(2), 3, "user2"),
    TestConjunctionAndDisjunction("app-ndsweeks-3", msWeeksAgo(3), "ndsweeks",
      msWeeksAgo(3), 4, "user3"),
    TestConjunctionAndDisjunction("app-Nds86-1", msDaysAgo(3), "nds86",
      msDaysAgo(4), 5, "user1"),
    TestConjunctionAndDisjunction("app-nds86-2", msDaysAgo(6), "nds86",
      msWeeksAgo(1), 6, "user2"),
    TestConjunctionAndDisjunction("app-nds86-3", msDaysAgo(18), "nds86",
      msWeeksAgo(3), 7, "user3"))

  test("Test disjunction all filters") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterCriteria("10-newest") ++ filterAppName("nds") ++
          startTimePeriod("3w") ++ userName("user1"), expectedFilterSize = 7, "any")
  }

  test("Test disjunction no appName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterCriteria("10-newest") ++
          startTimePeriod("2w") ++ userName("user3"), 6, "any")
  }

  test("Test disjunction no startTime") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterCriteria("10-newest") ++ filterAppName("nds") ++ userName("user1"),
      6, "any")
  }

  test("Test disjunction no userName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterCriteria("10-newest") ++ filterAppName("nds") ++
          startTimePeriod("2w"), 6, "any")
  }

  test("Test disjunction only userName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterCriteria("10-newest") ++ userName("user1"), 3, "any")
  }

  test("Test disjunction only appName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterAppName("nds"), 5, "any")
  }

  test("Test disjunction match fileName or appName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      matchFileName("app-nds") ++ filterAppName("Nds"),
      5, "any")
  }

  test("Test disjunction match filename, 10-newest-filesystem and appName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      matchFileName("app-nds") ++ filterCriteria("10-newest-filesystem") ++ filterAppName("nds"),
      7, "any")
  }

  test("Test disjunction only startTime no match") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      startTimePeriod("10min"), 0, "any")
  }

  test("Test conjunction all filters") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterCriteria("10-newest") ++ filterAppName("nds") ++
          startTimePeriod("3w") ++ userName("user1"), 2)
  }

  test("Test conjunction no appName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterCriteria("10-newest") ++
          startTimePeriod("2w") ++ userName("user3"), 0)
  }

  test("Test conjunction no startTime") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterCriteria("10-newest") ++ filterAppName("nds") ++ userName("user1"), 2)
  }

  test("Test conjunction no userName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterCriteria("10-newest") ++ filterAppName("nds") ++
          startTimePeriod("2w"), 3)
  }

  test("Test conjunction only userName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterCriteria("10-newest") ++ userName("user1"), 3)
  }

  test("Test conjunction only appName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterAppName("nds"), 5)
  }

  test("Test conjunction match fileName and appName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      matchFileName("app-nds") ++ filterAppName("Nds"), 2)
  }

  test("Test conjunction match filename, 10-newest-filesystem and appName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      matchFileName("app-nds") ++ filterCriteria("10-newest-filesystem") ++ filterAppName("nds"),
      3)
  }

  test("Test conjunction match appName and config") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterAppName("nds") ++ filterSparkProperty("spark.driver.port:43492"), 1)
  }

  test("Test conjunction match filename and config") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      matchFileName("app-nds") ++ filterSparkProperty("spark.app.name:Ndsweeks"), 1)
  }

  test("Test conjunction match filename and spark hive metastore config") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      matchFileName("app-nds") ++ filterSparkProperty("spark.sql.hive.metastore.sharedPrefixes:" +
          "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver"), 5)
  }

  test("Test conjunction match fileName and appName with configs") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      matchFileName("app-nds") ++ filterSparkProperty("spark.driver.port:43492")
          ++ filterAppName("Nds"), 1)
  }

  test("Test conjunction match redaction regex config and appName") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterSparkProperty("spark.redaction.regex") ++ filterAppName("Nds"), 2)
  }

  test("Test conjunction spark shuffle configs") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterSparkProperty("spark.shuffle.io.maxRetries:2") ++
          filterSparkProperty("spark.shuffle.registration.maxAttempts:3"), 2)
  }

  test("Test disjunction match multiple configs") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterSparkProperty("spark.driver.host:10.10.19.13")
        ++ filterSparkProperty("spark.driver.port:43492"), 4, "any")
  }

  test("Test disjunction match multiple special case configs") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterSparkProperty("spark.eventLog.dir:file:///tmp/spark-events-1")
        ++ filterSparkProperty("spark.master:spark://5.6.7.8:7076"), 5, "any")
  }

  test("Test disjunction match config containing url") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterSparkProperty("spark.sql.maven.additionalRemoteRepositories:" +
          "https://maven-central.storage-download.googleapis.com/maven2/")
      ++ filterSparkProperty("spark.eventLog.dir:file:///tmp/spark-events-1"), 7, "any")
  }

  test("Test disjunction with appName and config") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterAppName("Nds") ++ filterSparkProperty("spark.driver.port:43491"), 4, "any")
  }

  test("Test disjunction match appName and config") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterAppName("nds") ++ filterSparkProperty("spark.driver.port:43492"), 6, "any")
  }

  test("Test disjunction match appName and mix match configs") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      filterAppName("nds") ++ filterSparkProperty("spark.driver.port:43492") ++
          filterSparkProperty("spark.driver.host"), 7, "any")
  }

  test("Test conjunction match fileName and appName with non existent configs") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      matchFileName("app-nds") ++ filterSparkProperty("spark.driver.hosta")
          ++ filterSparkProperty("spark.driver.porta") ++ filterAppName("Nds"), 0)
  }

  test("Test disjunction match fileName and appName with non existent configs") {
    testConjunctionAndDisjunction(appsNameConjunctionAndDisjunctionToTest,
      matchFileName("app-nds") ++ filterSparkProperty("spark.driver.hosta")
          ++ filterSparkProperty("spark.driver.porta") ++ filterAppName("Nds"), 5, "any")
  }

  def filterCriteria(filterCrit: String): Array[String] = {
    Array("--filter-criteria", filterCrit)
  }

  def filterAppName(appName: String): Array[String] = {
    Array("--application-name", appName)
  }

  def matchFileName(appName: String): Array[String] = {
    Array("--match-event-logs", appName)
  }

  def startTimePeriod(startPeriod: String): Array[String] = {
    Array("--start-app-time", startPeriod)
  }

  def userName(name: String): Array[String] = {
    Array("--user-name", name)
  }

  def filterSparkProperty(configNames: String): Array[String] = {
    Array("--spark-property", configNames)
  }

  private def testConjunctionAndDisjunction(
      apps: Array[TestConjunctionAndDisjunction],
      filtersToApply: Array[String],
      expectedFilterSize: Int,
      logicFilter: String = "all"): Unit = {
    val allArgs = filtersToApply ++ Array(s"--$logicFilter")
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with text generator")
          .withContentGenerators(
            apps.map { app =>
              (_: EventlogProviderImpl) => {
                val userPattern = "user(\\d+)".r
                val userId = userPattern.findFirstMatchIn(app.userName).get.group(1).toInt
                val applicationId = s"local-16261043003${app.uniqueId}"
                // scalastyle:off line.size.limit
                val fileContent =
                  s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
                     |{"Event":"SparkListenerApplicationStart","App Name":"${app.appName}", "App ID":"$applicationId","Timestamp":${app.appTime}, "User":"${app.userName}"}
                     |{"Event":"SparkListenerEnvironmentUpdate","JVM Information":{"Java Home":"/usr/lib/jvm/java-8-openjdk-amd64/jre"},"Spark Properties":{"spark.driver.host":"10.10.19.1$userId","spark.app.name":"${app.appName}","spark.driver.port":"4349$userId","spark.eventLog.enabled":"true","spark.master":"spark://5.6.7.8:707${userId + 4}","spark.redaction.regex":"*********(redacted)","spark.eventLog.dir":"file:///tmp/spark-events-$userId","spark.sql.maven.additionalRemoteRepositories":"https://maven-central.storage-download.googleapis.com/maven2/","spark.sql.hive.metastore.sharedPrefixes":"com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver","spark.shuffle.io.maxRetries":"$userId","spark.shuffle.registration.maxAttempts":"${userId + 1}"},"Hadoop Properties":{"hadoop.service.shutdown.timeout":"30s"},"System Properties":{"java.io.tmpdir":"/tmp"},"Classpath Entries":{"/home/user1/runspace/spark311/spark-3.1.1-bin-hadoop3.2/jars/hive-exec-2.3.7-core.jar":"System Classpath"}}""".stripMargin
                // scalastyle:on line.size.limit
                // set the file name from the qpp object.
                EventlogContentGenMeta(app.appName, app.fileName, fileContent)
              }}))
      .withToolArgs(allArgs)
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(expectedFilterSize)
          .withSuccessCode())
      .build()
  }

  test("Test filtering eventlog with missing start event") {
    QToolTestCtxtBuilder()
      .withEvLogProvider(
        EventlogProviderImpl("create an app with text generator")
          .withContentGenerators(
            appsFullWithFsToTest.map { app =>
              (_: EventlogProviderImpl) => {
                val applicationName = app.appName
                val applicationId = "local-16261043-" + app.uniqueId
                // scalastyle:off line.size.limit
                val fileContent =
                  s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}""".stripMargin
                // scalastyle:on line.size.limit
                EventlogContentGenMeta(applicationName, applicationId, fileContent,
                  Some(app.fsTime))
              }}))
      .withToolArgs(
        Array("--filter-criteria", "1-newest"))
      .withChecker(
        QToolResultCoreChecker("check app count")
          .withExpectedSize(0)
          .withSuccessCode())
      .build()
  }
}
