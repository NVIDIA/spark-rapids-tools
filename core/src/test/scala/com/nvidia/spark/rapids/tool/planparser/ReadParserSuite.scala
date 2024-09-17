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


package com.nvidia.spark.rapids.tool.planparser

import com.nvidia.spark.rapids.BaseTestSuite
import org.scalatest.Matchers.{be, convertToAnyShouldWrapper}

// Tests the implementation of the ReadParser independently of end-2-end tests
class ReadParserSuite extends BaseTestSuite {
  // Wrapper to hold the related test cases
  case class MetaFieldsTestCase(testDescription: String,
      nodeDescr: String, expectedMetafields: Map[String, String]) {
  }

  test("Read Metadata fields from a graphNode (pushedFilters, dataFilters, partitionFilters)") {
    val nodeDescrPrologue =
      """FileScan parquet db.table[field_00#952L,label#953,field_02#954,field_03#958L,
        |field_04#960,field_05#961L,field_06#962L,field_07#963L,field_08#964L,field_09#965L,
        |field_10#966L,field_11#967L,field_12#968L,field_13#969L,field_14#970L,field_15#971L,
        |field_16#972L,field_17#973L,field_18#974L,field_19#975L,field_20#976L,field_21#977L,
        |field_22#978L,field_22#979L,... 36 more fields]""".stripMargin.replaceAll("\n", "")
    // All metrafields exist and terminated by closing brackets
    val allTestScenarios = Seq(
      MetaFieldsTestCase(
        "All the 3 MetaFields are present -- terminated by closing bracket",
        nodeDescrPrologue + """ Batched: true,
          | DataFilters: [isnotnull(flag_00#1013L), (flag_00#1013L = 1)],
          | Format: Parquet,
          | Location: InMemoryFileIndex(20 paths)[hdfs://directory/subdirectory/file_1....,
          | PartitionFilters: [isnotnull(date_00#1014), (date_00#1014 = 20240621)],
          | PushedFilters: [IsNotNull(flag_00), EqualTo(flag_00,1)],
          | ReadSchema: struct<field_00:bigint,label:string,field_02:string,field_03:bigint,
          |field_04:string,high_price...
          |""".stripMargin.stripMargin.replaceAll("\n", ""),
        Map(
          ReadParser.METAFIELD_TAG_PUSHED_FILTERS ->
            "IsNotNull(flag_00), EqualTo(flag_00,1)",
          ReadParser.METAFIELD_TAG_DATA_FILTERS ->
            "isnotnull(flag_00#1013L), (flag_00#1013L = 1)",
          ReadParser.METAFIELD_TAG_PARTITION_FILTERS ->
            "isnotnull(date_00#1014), (date_00#1014 = 20240621)")),
      MetaFieldsTestCase(
        "All the 3 MetaFields are present -- terminated by closing bracket -- Order is different",
        nodeDescrPrologue + """ Batched: true,
          | PushedFilters: [IsNotNull(flag_00), EqualTo(flag_00,1)],
          | DataFilters: [isnotnull(flag_00#1013L), (flag_00#1013L = 1)],
          | Format: Parquet,
          | Location: InMemoryFileIndex(20 paths)[hdfs://directory/subdirectory/file_1....,
          | PartitionFilters: [isnotnull(date_00#1014), (date_00#1014 = 20240621)],
          | ReadSchema: struct<field_00:bigint,label:string,field_02:string,field_03:bigint,
          |field_04:string,high_price...
          |""".stripMargin.stripMargin.replaceAll("\n", ""),
        Map(
          ReadParser.METAFIELD_TAG_PUSHED_FILTERS ->
            "IsNotNull(flag_00), EqualTo(flag_00,1)",
          ReadParser.METAFIELD_TAG_DATA_FILTERS ->
            "isnotnull(flag_00#1013L), (flag_00#1013L = 1)",
          ReadParser.METAFIELD_TAG_PARTITION_FILTERS ->
            "isnotnull(date_00#1014), (date_00#1014 = 20240621)")),
      MetaFieldsTestCase(
        "Only 1 MetaField is present -- terminated by closing bracket",
        nodeDescrPrologue + """ Batched: true,
          | PushedFilters: [IsNotNull(flag_00), EqualTo(flag_00,1)],
          | Format: Parquet,
          | Location: InMemoryFileIndex(20 paths)[hdfs://directory/subdirectory/file_1....,
          | ReadSchema: struct<field_00:bigint,label:string,field_02:string,field_03:bigint,
          |field_04:string,high_price...
          |""".stripMargin.stripMargin.replaceAll("\n", ""),
        Map(
          ReadParser.METAFIELD_TAG_PUSHED_FILTERS ->
            "IsNotNull(flag_00), EqualTo(flag_00,1)",
          ReadParser.METAFIELD_TAG_DATA_FILTERS -> ReadParser.UNKNOWN_METAFIELD,
          ReadParser.METAFIELD_TAG_PARTITION_FILTERS -> ReadParser.UNKNOWN_METAFIELD)),
      MetaFieldsTestCase(
        "Metafields might be truncated (not terminated by closing bracket)",
        nodeDescrPrologue + """ Batched: true,
          | PushedFilters: [IsNotNull(flag_00), EqualTo(flag_00,1),...,
          | DataFilters: [isnotnull(flag_00#1013L), (flag_00#1013L = 1)...,
          | Format: Parquet,
          | Location: InMemoryFileIndex(20 paths)[hdfs://directory/subdirectory/file_1....,
          | ReadSchema: struct<field_00:bigint,label:string,field_02:string,field_03:bigint,
          |field_04:string,high_price...
          | PartitionFilters: [isnotnull(date_00#1014), (date_00#1014 = 20240621)...
          |""".stripMargin.stripMargin.replaceAll("\n", ""),
        Map(
          ReadParser.METAFIELD_TAG_PUSHED_FILTERS ->
            "IsNotNull(flag_00), EqualTo(flag_00,1),...",
          ReadParser.METAFIELD_TAG_DATA_FILTERS ->
            "isnotnull(flag_00#1013L), (flag_00#1013L = 1)...",
          ReadParser.METAFIELD_TAG_PARTITION_FILTERS ->
            "isnotnull(date_00#1014), (date_00#1014 = 20240621)...")),
      MetaFieldsTestCase(
        "Metafields might be empty",
        nodeDescrPrologue + """ Batched: true,
          | PushedFilters: [IsNotNull(flag_00), EqualTo(flag_00,1),...,
          | DataFilters: [],
          | Format: Parquet,
          | Location: InMemoryFileIndex(20 paths)[hdfs://directory/subdirectory/file_1....,
          | ReadSchema: struct<field_00:bigint,label:string,field_02:string,field_03:bigint,
          |field_04:string,high_price...
          | PartitionFilters: [isnotnull(date_00#1014), (date_00#1014 = 20240621)...
          |""".stripMargin.stripMargin.replaceAll("\n", ""),
        Map(
          ReadParser.METAFIELD_TAG_PUSHED_FILTERS ->
            "IsNotNull(flag_00), EqualTo(flag_00,1),...",
          ReadParser.METAFIELD_TAG_DATA_FILTERS -> "",
          ReadParser.METAFIELD_TAG_PARTITION_FILTERS ->
            "isnotnull(date_00#1014), (date_00#1014 = 20240621)..."))
    )
    for (scenario <- allTestScenarios) {
      try {
        ReadParser.extractReadTags(scenario.nodeDescr) should be (scenario.expectedMetafields)
      } catch {
        case e: Exception =>
          fail(s"Failed for scenario: ${scenario.testDescription}", e)
      }
    }
  }
}
