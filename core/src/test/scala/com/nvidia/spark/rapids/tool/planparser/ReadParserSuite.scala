/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.BaseTestSuite
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper}

import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}
import org.apache.spark.sql.rapids.tool.plangraph.{SparkPlanGraphNode, SQLPlanMetric, ToolsPlanGraph}

// Tests the implementation of the ReadParser independently of end-2-end tests
class ReadParserSuite extends BaseTestSuite {
  // Wrapper to hold the related test cases
  case class MetaFieldsTestCase(testDescription: String,
      nodeDescr: String, expectedMetafields: Map[String, String]) {
  }

  case class BatchScanTestCase(
      testDescription: String,
      nodeName: String,
      nodeDescr: String,
      expectedReadMetaData: ReadMetaData,
      expectedPushedFilters: Option[String] = None) {
    lazy val graphNode: SparkPlanGraphNode = ToolsPlanGraph.constructGraphNode(
      1,
      nodeName,
      nodeDescr, Seq[SQLPlanMetric]())
    lazy val actualReadMetaData: ReadMetaData = BatchScanExecParser.extractReadMetaData(graphNode)
    def eval(): Unit = {
      withClue(s"Test failed for: $testDescription\n") {
        actualReadMetaData should be (expectedReadMetaData)
        expectedPushedFilters.foreach { pf =>
          actualReadMetaData.pushedFilters should be (pf)
        }
      }
    }
  }

  test("Read Metadata fields from a graphNode (pushedFilters, dataFilters, partitionFilters)") {
    val nodeDescrPrologue =
      """FileScan parquet db.table[field_00#952L,label#953,field_02#954,field_03#958L,
        |field_04#960,field_05#961L,field_06#962L,field_07#963L,field_08#964L,field_09#965L,
        |field_10#966L,field_11#967L,field_12#968L,field_13#969L,field_14#970L,field_15#971L,
        |field_16#972L,field_17#973L,field_18#974L,field_19#975L,field_20#976L,field_21#977L,
        |field_22#978L,field_22#979L,... 36 more fields]""".stripMargin.replaceAll("\n", "")
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
            "isnotnull(date_00#1014), (date_00#1014 = 20240621)",
          ReadParser.METAFIELD_TAG_RUNTIME_FILTERS -> ReadParser.UNKNOWN_METAFIELD)),
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
            "isnotnull(date_00#1014), (date_00#1014 = 20240621)",
          ReadParser.METAFIELD_TAG_RUNTIME_FILTERS -> ReadParser.UNKNOWN_METAFIELD)),
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
          ReadParser.METAFIELD_TAG_PARTITION_FILTERS -> ReadParser.UNKNOWN_METAFIELD,
          ReadParser.METAFIELD_TAG_RUNTIME_FILTERS -> ReadParser.UNKNOWN_METAFIELD)),
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
            "isnotnull(date_00#1014), (date_00#1014 = 20240621)...",
          ReadParser.METAFIELD_TAG_RUNTIME_FILTERS -> ReadParser.UNKNOWN_METAFIELD)),
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
            "isnotnull(date_00#1014), (date_00#1014 = 20240621)...",
          ReadParser.METAFIELD_TAG_RUNTIME_FILTERS -> ReadParser.UNKNOWN_METAFIELD))
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

  test("BatchScan Parsing") {
    // Tests the extraction of BatchScan read metadata from a graphNode.
    val batchScanCases = Seq(
      BatchScanTestCase(
        // test that RuntimeFilters is not appended to the schema fields.
        "BatchScan from file has correct parsing of readSchema",
        "BatchScan csv file:/path/to/file.txt",
        """BatchScan csv
          | file:/path/to/file.txt
          |[f_00#16038L, f_01#16039, f_02#16040, f_03#16041, f_04#16042,
          | f_05#16043, f_06#16044, f_07#16045, f_08#16046, f_09#16047, f_10#16048, f_11#16049,
          | f_12#16050, first_home_buyer#16051, f_13#16052, f_14#16053, f_15#16054, f_16#16055,
          | f_17#16056, f_18#16057, f_19#16058, f_20#16059, f_21#16060, f_22#16061, f_23#16062]
          | CSVScan
          | DataFilters: [],
          | Format: csv,
          | Location: InMemoryFileIndex(1 paths)[file:/path/to/f...,
          | PartitionFilters: [],
          | PushedFilters: [],
          | ReadSchema: struct<f_00:bigint,f_01:string,f_02:string,f_03:double,f_04:i...
          | RuntimeFilters: []""".stripMargin.replaceAll("\n", ""),
        expectedReadMetaData = ReadMetaData(
          schema = "f_00:bigint,f_01:string,f_02:string,f_03:double,f_04:i...",
          location = "file:/path/to/file.txt",
          format = "csv",
          tags = Map(
            ReadParser.METAFIELD_TAG_DATA_FILTERS -> "",
            ReadParser.METAFIELD_TAG_PUSHED_FILTERS -> "",
            ReadParser.METAFIELD_TAG_PARTITION_FILTERS -> "",
            ReadParser.METAFIELD_TAG_RUNTIME_FILTERS -> ""
          )
        ),
        expectedPushedFilters = Some("filters=[],runtimeFilters=[]"
      ))
    )
    batchScanCases.foreach { batchCase =>
      batchCase.eval()
    }
  }

  test("test different types in ReadSchema") {
    // this tests parseReadSchema by passing different schemas as strings. Schemas
    // with complex types, complex nested types, decimals and simple types
    val testSchemas: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer(
      ArrayBuffer(""),
      ArrayBuffer("firstName:string,lastName:string", "", "address:string"),
      ArrayBuffer("properties:map<string,string>"),
      ArrayBuffer("name:array<string>"),
      ArrayBuffer("name:string,booksInterested:array<struct<name:string,price:decimal(8,2)," +
        "author:string,pages:int>>,authbook:array<map<name:string,author:string>>, " +
        "pages:array<array<struct<name:string,pages:int>>>,name:string,subject:string"),
      ArrayBuffer("name:struct<fn:string,mn:array<string>,ln:string>," +
        "add:struct<cur:struct<st:string,city:string>," +
        "previous:struct<st:map<string,string>,city:string>>," +
        "next:struct<fn:string,ln:string>"),
      ArrayBuffer("name:map<id:int,map<fn:string,ln:string>>, " +
        "address:map<id:int,struct<st:string,city:string>>," +
        "orders:map<id:int,order:array<map<oname:string,oid:int>>>," +
        "status:map<name:string,active:string>")
    )

    var index = 0
    val expectedResult = List(
      ("", ""),
      ("", ""),
      ("map<string,string>", ""),
      ("array<string>", ""),
      ("array<struct<name:string,price:decimal(8,2),author:string,pages:int>>;" +
        "array<map<name:string,author:string>>;array<array<struct<name:string,pages:int>>>",
        "array<struct<name:string,price:decimal(8,2),author:string,pages:int>>;" +
          "array<map<name:string,author:string>>;array<array<struct<name:string,pages:int>>>"),
      ("struct<fn:string,mn:array<string>,ln:string>;" +
        "struct<cur:struct<st:string,city:string>,previous:struct<st:map<string,string>," +
        "city:string>>;struct<fn:string,ln:string>",
        "struct<fn:string,mn:array<string>,ln:string>;" +
          "struct<cur:struct<st:string,city:string>,previous:struct<st:map<string,string>," +
          "city:string>>"),
      ("map<id:int,map<fn:string,ln:string>>;map<id:int,struct<st:string,city:string>>;" +
        "map<id:int,order:array<map<oname:string,oid:int>>>;map<name:string,active:string>",
        "map<id:int,map<fn:string,ln:string>>;map<id:int,struct<st:string,city:string>>;" +
          "map<id:int,order:array<map<oname:string,oid:int>>>"))

    val result = testSchemas.map(x => AppBase.parseReadSchemaForNestedTypes(x))
    result.foreach { actualResult =>
      assert(ToolUtils.formatComplexTypes(actualResult._1).equals(expectedResult(index)._1))
      assert(ToolUtils.formatComplexTypes(actualResult._2).equals(expectedResult(index)._2))
      index += 1
    }
  }
}
